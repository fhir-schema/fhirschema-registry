(ns sandbox
  (:require [system]
            [gcs]
            [http]
            [fhir-pkg]
            [pg]
            [pg.repo]
            [cheshire.core]
            [clj-yaml.core]
            [clojure.string :as str]
            [utils.ndjson :as ndjson]
            [fhirschema.terminology]
            [fhir.schema.transpiler :refer [translate]]))

(defn dump [x]
  (spit "/tmp/dump.yaml" (clj-yaml.core/generate-string x)))

(def canonicals
  ["activitydefinition"
   "actordefinition"
   "capabilitystatement"
   "chargeitemdefinition"
   "citation"
   "codesystem"
   "compartmentdefinition"
   "conceptmap"
   "conditiondefinition"
   "eventdefinition"
   "evidence"
   "evidencereport"
   "evidencevariable"
   "examplescenario"
   "graphdefinition"
   "implementationguide"
   "library"
   "measure"
   "messagedefinition"
   "namingsystem"
   "observationdefinition"
   "operationdefinition"
   "plandefinition"
   "questionnaire"
   "requirements"
   "searchparameter"
   "specimendefinition"
   "structuredefinition"
   "structuremap"
   "subscriptiontopic"
   "terminologycapabilities"
   "testplan"
   "testscript"
   "valueset"])

(def canonical-columns
  {:package_name {:type "text" :required true}
   :package_version {:type "text" :required true}
   :url {:type "text" :required true}
   :version {:type "text" :required true}
   :resource {:type "jsonb"}})

(defn migrate [context]

  (pg.repo/register-repo
   context {:table "package"
            :primary-key [:name]
            :columns {:name {:type "text" :required true}
                      :resource {:type "jsonb"}}})

  (pg.repo/register-repo
   context {:table "package_version"
            :primary-key [:name :version :resource]
            :columns {:name {:type "text" :required true}
                      :version {:type "text" :required true}
                      :resource {:type "jsonb"}}})

  (pg.repo/register-repo
   context {:table "package_dependency"
            :primary-key [:name :version :dep_name :dep_version]
            :columns {:name {:type "text" :required true}
                      :version {:type "text" :required true}
                      :dep_name {:type "text"}
                      :dep_version {:type "text"}
                      :resource {:type "jsonb"}}})

  (pg.repo/register-repo
   context {:table "structuredefinition_element"
            :primary-key [:package_name :package_version :definition :version :id]
            :columns {:package_name    {:type "text" :required true}
                      :package_version {:type "text" :required true}
                      :definition      {:type "text" :required true}
                      :version         {:type "text" :required true}
                      :id              {:type "text" :required true}
                      :path            {:type "text"}
                      :resource {:type "jsonb"}}})

  (doseq [tbl canonicals]
    (pg.repo/register-repo
     context {:table tbl
              :primary-key [:url :version :package_name :package_version]
              :columns canonical-columns})))


(defn loadable? [nm]
  (let [lnm (str/lower-case nm)]
    (and (str/ends-with? nm ".json")
         (or (contains? #{"package.json" ".index.json"} nm)
             (some (fn [x] (str/starts-with? lnm x)) canonicals)))))

(defn prepare-resource [nm pkgi res]
  (-> (assoc-in (dissoc res :text :snapshot :expansion) [:meta :file] nm)
      (assoc :package_name (:name pkgi)
             :package_version (:version pkgi)
             :version (or (:version res) (:version pkgi)))))

(defn read-package
  "load package into hash-map with :package :index and <rt>"
  [context package-info]
  (let [pkgi package-info]
    (->
     (fhir-pkg/reduce-package
      context pkgi
      (fn [acc nm read-file]
        (if (loadable? nm)
          (let [res (read-file true)]
            (cond
              (= nm "package.json")
              (assoc acc
                     :package_version res
                     :package_dependency (:dependencies res))
              (= nm ".index.json")
              (assoc acc :index res)
              (and (:resourceType res) (:url res))
              (let [res (prepare-resource nm pkgi res)
                    path (->> [(str/lower-case (or (:resourceType res) nm)) (:url res)] (filter identity))]
                (assoc-in acc path res))
              :else
              (do #_(println :skip nm) acc)))
          (do
            #_(println :skip nm)
            acc))))
     (assoc :package pkgi))))

(defn- load-canonicals [context pkg]
  (doseq [cn canonicals]
    (when-let [rs (vals (get pkg cn))]
      (system/info context ::load (str cn " - " (count rs)))
      (pg.repo/load
       context {:table cn}
       (fn [insert]
         (doseq [r rs]
           (insert r)))))))

(declare load-package)

(defn- load-deps [context pkg pkgi opts]
  (->> (:package_dependency pkg)
       (mapv (fn [[k v]]
               (load-package context (name k) v (update opts [:path] (fn [x] (conj (or x []) (:name pkgi))))))))
  (doseq [[dep-name dep-version] (:package_dependency pkg)]
    (pg.repo/insert context {:table "package_dependency"
                             :resource {:name (:name pkgi)
                                        :version (:version pkgi)
                                        :dep_name (name dep-name)
                                        :dep_version dep-version}})))

(defn load-elements [context pkg-bundle]
  (pg.repo/load
   context {:table "structuredefinition_element"}
   (fn [insert]
     (doseq [[url sd] (get pkg-bundle "structuredefinition")]
       (doseq [el (get-in sd [:differential :element])]
         (insert
          (assoc el
                 :definition   url
                 :version      (or (:version sd) (:package_version sd))
                 :package_name (:package_name sd)
                 :package_version (:package_version sd))))))))

(defn load-package
  "load package recursively"
  [context package-name package-version & [opts]]
  (when-not (pg.repo/read context {:table "package_version" :match {:name package-name :version package-version}})
    (system/info context ::load-package (str package-name "@" package-version))
    (let [pkgi (fhir-pkg/pkg-info context (str package-name "@" package-version))
          pkg-bundle (read-package context pkgi)]
      (pg.repo/upsert context {:table "package" :resource pkgi})
      (pg.repo/insert context {:table "package_version" :resource (:package pkg-bundle)})
      (load-deps context pkg-bundle pkgi opts)
      (load-canonicals context pkg-bundle)
      (load-elements context pkg-bundle))))

(defn truncate [context]
  (doseq [cn canonicals]
    (pg.repo/truncate context {:table cn}))
  (pg.repo/truncate context {:table "package"})
  (pg.repo/truncate context {:table "package_version"})
  (pg.repo/truncate context {:table "package_dependency"})
  (pg.repo/truncate context {:table "structuredefinition_element"}))


(comment

  (def context (system/start-system {:services ["pg" "pg.repo" "gcs" "fhir-pkg"]
                                     :http {:port 7777}
                                     :pg (cheshire.core/parse-string (slurp "connection.json") keyword)}))

  (system/stop-system context)


  (pg/execute! context {:sql ["drop table if exists package;"]})
  (pg/execute! context {:sql ["drop table if exists package_version;"]})

  (migrate context)
  (truncate context)

  (def pkgi (fhir-pkg/pkg-info context "hl7.fhir.us.core"))
  (def pkg-bundle (read-package context pkgi))


  (time (load-package context (:name pkgi) (:version pkgi)))

  (count (vals (get pkg "StructureDefinition")))


  (get-in pkg-bundle ["structuredefinition"   "http://hl7.org/fhir/us/core/StructureDefinition/us-core-bmi" :differential])

  (pg.repo/select context {:table "structuredefinition" :limit 10})
  (pg.repo/select context {:table "structuredefinition_element" :limit 10})

  (pg.repo/select context {:table "valueset" :limit 10})
  (pg.repo/select context {:table "codesystem" :limit 10})

  (pg.repo/select context {:table "package" :limit 10})

  (pg/execute! context {:sql "select name, version from package_version"})
  (pg/execute! context {:sql "select * from package_dependency"})

  (pg/execute! context {:sql "select * from structuredefinition_element where resource->'binding' is not null limit 10"})

  (pg.repo/read context {:table "package" :match {:name "hl7.fhir.us.core" :version "6.1.0"}})

  (pg/execute! context {:sql "select count(*) from structuredefinition"})
  (pg/execute! context {:sql "select count(*) from codesystem"})
  (pg/execute! context {:sql "select count(*) from valueset"})

  (pg/execute! context {:sql "select url, version, package_name, package_version from structuredefinition limit 10"})

  (pg/execute!
   ctx ["
CREATE MATERIALIZED VIEW structuredefinitions AS
SELECT
resource#>>'{meta,package,url}' as package_name,
resource#>>'{meta,package,version}' as package_version,
resource#>>'{url}' as url,
resource#>>'{version}' as version,
resource as resource
from _resources
order by 1,2,3,4

"])

  (pg/execute! ctx ["create index sd_package_name_trgrm on structuredefinitions USING gin (package_name gin_trgm_ops)"])

  (pg/execute! ["REFRESH MATERIALIZED VIEW structuredefinitions;"])

  (pg/execute! ctx ["
select distinct package_version
from structuredefinitions
where
 package_name ilike 'hl7.fhir.us.core'
limit 10"])

  (pg/execute! ctx ["select * from structuredefinitions where package_name ilike 'hl7.fhir.us.core' and package_version = '5.0.1'limit 10"])

  (->> (pg/execute! ctx ["select * from structuredefinitions where package_name ilike 'hl7.fhir.us.core' and package_version = '6.1.0'limit 10"])
       (mapv (fn [x]
               (fhirschema.transpiler/translate (:resource x)))))


  (def sds 
    (with-open [s (ndjson/url-stream "http://fs.get-ig.org/p/hl7.fhir.us.core/7.0.0/structuredefinition.ndjson.gz")]
      (ndjson/read-stream s (fn [acc res _] (conj acc res)) [])))

  (def canonicals
    (time
     (with-open [s (ndjson/url-stream "http://fs.get-ig.org/p/hl7.fhir.us.core/7.0.0/structuredefinition.ndjson.gz")]
       (ndjson/read-stream s (fn [acc res _] (assoc acc (:url res) (translate res))) {}))))

  (count (keys canonicals))


  (count sds)

  (->> sds
       (mapv (fn [r]
               (spit (str "tmp/schemas/" (last (str/split (:url r) #"/"))  ".yaml")
                     (clj-yaml.core/generate-string [(translate r) r])))))


  (def storage (gcs/mk-storage))

  (def pkgs
    (time
     (->> (gcs/objects)
          (filterv (fn [x] (str/ends-with? (.getName x) "package.json")))
          (pmap (fn [x] (gcs/blob-content x {:json true})))
          (into []))))

  (count pkgs)
  (mapv (fn [x] [(:name x) (:version x)]) pkgs)

  (def blb (gcs/get-blob storage gcs/get-ig-bucket "p/hl7.fhir.r4.core/4.0.1/structuredefinition.ndjson.gz"))
  (def sds (gcs/read-ndjson-blob blb))

  (def bindings
    (->> sds
         (mapcat
          (fn [sd]
            (when true #_(and (= "specialization" (:derivation sd)) (= "resource" (:kind sd)))
              (->> (get-in sd [:differential :element])
                   (filter (fn [x]
                             (and (:binding x)
                                  (not (= "example" (get-in x [:binding :strength]))))))
                   (mapv (fn [e]
                           (-> (select-keys (:binding e) [:strength :valueSet])
                               (update :valueSet (fn [s] (first (str/split s #"\|"))))
                               (assoc :path (:path e) :sd (:url sd)))))))))))

  (count bindings)
  (take 100 bindings)

  (def required-bindings
    (->> bindings
         (mapv :valueSet)
         (into #{})
         #_(count)))


  (defn url-idx [xs] (->> xs (reduce (fn [acc x] (assoc acc (:url x) x)) {})))

  (def cs-blb (gcs/get-blob storage gcs/get-ig-bucket "p/hl7.fhir.r4.core/4.0.1/codesystem.ndjson.gz"))
  (def codesystems (gcs/read-ndjson-blob cs-blb))
  (def cs-idx (url-idx codesystems))

  (def vs-blb (gcs/get-blob storage gcs/get-ig-bucket "p/hl7.fhir.r4.core/4.0.1/valueset.ndjson.gz"))
  (def valuesets (gcs/read-ndjson-blob vs-blb))
  (def vs-idx (url-idx valuesets))

  (count (keys vs-idx))
  (clojure.set/difference required-bindings (into #{} (keys vs-idx)) )
  #{"http://loinc.org/vs/LL379-9"
    "http://dicom.nema.org/medical/dicom/current/output/chtml/part16/sect_CID_29.html"
    "http://www.rsna.org/RadLex_Playbook.aspx"
    "http://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_B.5.html#table_B.5-1"}

  (count required-bindings)

  (def resource-bindings
    (->> sds
         (mapcat
          (fn [sd]
            (when true (and (= "specialization" (:derivation sd)) (= "resource" (:kind sd)))
                  (->> (get-in sd [:differential :element])
                       (filter (fn [x]
                                 (and (:binding x)
                                      (not (= "example" (get-in x [:binding :strength]))))))
                       (mapv (fn [e]
                               (-> (select-keys (:binding e) [:strength :valueSet])
                                   (update :valueSet (fn [s] (first (str/split s #"\|"))))
                                   (assoc :path (:path e) :sd (:url sd)))))))))))

  (def resource-binding-set (into #{} (mapv :valueSet resource-bindings)))

  (count resource-binding-set)


  (def cs-blb (gcs/get-blob storage gcs/get-ig-bucket "p/hl7.fhir.r4.core/4.0.1/codesystem.ndjson.gz"))
  (def codesystems (gcs/read-ndjson-blob cs-blb))

  (def cs-b (gcs/get-blob storage gcs/get-ig-bucket "p/hl7.fhir.r4.core/4.0.1/codesystem.ndjson.gz"))

  (time (gcs/blob-stream cs-b {:gzip true} (fn [in] (pg/copy-ndjson-stream ctx "cs" in))))

  (pg/execute! ctx ["create table cs (resource jsonb)"])

  (pg/execute! ctx ["select * from cs limit 1"])

  (pg/execute! ctx ["create table tcs (resource jsonb)"])
  (pg/execute! ctx ["create table tcs_concept (resource jsonb)"])


  (do
    (pg/execute! ctx ["truncate tcs; truncate tcs_concept"])
    (time
     (let [pkg (gcs/blob-content  (gcs/package-file "hl7.fhir.r4.core" "4.0.1" "package.json") {:json true})
           pkg-fields {:package (:name pkg) :package_version (:version pkg)}]
       (pg/copy-ndjson ctx "tcs_concept"
                       (fn [csc-write]
                         (pg/copy-ndjson ctx "tcs"
                                         (fn [cs-write]
                                           (gcs/process-ndjson-blob
                                            (gcs/package-file "hl7.fhir.r4.core" "4.0.1" "codesystem.ndjson.gz")
                                            (fn [res _ln]
                                              (let [tcs (fhirschema.terminology/tiny-codesystem res)]
                                                (cs-write (merge (dissoc tcs :concept) pkg-fields))
                                                (when (= "complete" (:content tcs))
                                                  (doseq [c (:concept tcs)]
                                                    (csc-write (merge (assoc c :codesystem (:url tcs) :version (:version tcs))
                                                                      pkg-fields))))))))))))))

  (time
   (gcs/process-ndjson-blob (gcs/package-file "hl7.fhir.r4.core" "4.0.1" "structuredefinition.ndjson.gz")
    (fn [res ln] (println ln (:url res) (:kind res) (:derivation res)))))

  (pg/execute! ctx ["select * from tcs where resource->>'content' <> 'complete' limit 10"])

  (pg/execute! ctx ["select * from tcs where resource->>'content' <> 'complete' limit 10"])
  (pg/execute! ctx ["select resource->>'content', count(*) from tcs group by 1 order by 2 desc"])

  {:select {:c [:. :content] :cnt [:count :*]} :from :tcs :group-by 1 :order-by [:desc 2]}

  (pg/execute! ctx ["select count(*) from tcs"])

  (pg/execute! ctx ["select * from tcs_concept limit 100"])


  (def pkgs
    (time
     (->> (gcs/objects)
          (filterv (fn [x] (str/ends-with? (.getName x) "package.json")))
          (pmap (fn [x] (gcs/blob-content x {:json true})))
          (into []))))

  (pg/copy-ndjson
   ctx "packages"
   (fn [write]
     (->> (gcs/objects)
          (filterv (fn [x] (str/ends-with? (.getName x) "package.json")))
          (pmap (fn [x]
                  (let [pkg (gcs/blob-content x {:json true})]
                    (try
                      (write pkg)
                      (catch Exception e
                        (println :error pkg (.getMessage e)))))))
          (into []))))

  (pg/execute! ctx ["create table packages (resource jsonb)"])
  (pg/execute! ctx ["truncate packages"])
  (pg/execute! ctx ["select distinct resource->>'name' from packages"])
  (pg/execute! ctx ["select * from packages limit 10"])
  (pg/execute! ctx ["
drop materialized view if exists package_names;
create materialized view package_names AS
select
  resource->>'name' as name,
  array_agg(resource->>'version') as versions
from packages
group by 1
order by 1
"])

  (pg/execute! ctx ["select * from package_names"])

  (pg/execute! ctx ["select * from packages limit 10"])

  (pg/execute! ctx ["
drop materialized view if exists packages_view;
create materialized view packages_view AS
select
  resource->>'name' as name,
  resource->>'version' as version,
  resource->>'canonical' as canonical,
  resource->>'fhirVersion' as fhirversion,
  resource->'dependencies' as deps
from packages
order by 1,2
"])

  (pg/execute! ctx ["select * from packages_view limit 100"])


  (pg/execute! ctx ["
drop materialized view if exists packages_deps;
create materialized view packages_deps AS
select
  resource#>>'{name}' as name,
  resource#>>'{version}' as version,
  substring(kv.key::text,2) as dep_name,
  kv.value::text as dep_version
from packages,
jsonb_each_text( resource->'dependencies' ) as kv
order by name, version, dep_name, dep_version
"])


  (pg/execute! ctx ["
select
dep_name, dep_version,count(*)
from packages_deps
group by 1,2
order by 3 desc
limit 20"])

  ;; sync package

;; packages management
;; register package
;; sync package with s3 bucket
;; * load structuredefinitions
;;   * elements view
;;   * bindings view
;;   * fhirschemas
;; * load codesystems
;;   * built-in cs
;;   * others
;;   * concepts view
;; * load valuesets

;; bindings -> valuesets [built-in and static]
;; enrich fhirschemas with valuesets and elements like array/scalar, descriminators etc [put limit on codes < 100]
;; serve fhirschemas and may be codesystems + valuesets


  )



