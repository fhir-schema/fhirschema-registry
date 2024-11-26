(ns sandbox
  (:require [system]
            [svs.gcp :as gcp]
            [svs.http :as http]
            [svs.pg :as pg]
            [cheshire.core]
            [clj-yaml.core]
            [clojure.string :as str]
            [utils.ndjson :as ndjson]
            [fhirschema.terminology]
            [fhirschema.transpiler :refer [translate]]))

(defn dump [x]
  (spit "/tmp/dump.yaml" (clj-yaml.core/generate-string x)))

(comment
  (def sys (system/new-system))

  (system/start sys
   ['svs.http 'svs.pg 'svs.gcp]
   {:svs.http {:port 8081}
    :svs.pluggins {:list ["plugins.auth-jwt"]}})

  (svs.settings/set ctx :svs.http/port 8087)

  ;; --svs.logger/level INFO
  ;; --svs.http/port 8081
  ;; --svs.plugins/list plugins.auth-jwt,
  ;; --svs.pg/database fhirschema
  ;; --auth.auth-jwt/url http://some-url/well-known

  (def system (system/new-system {}))


  (http/start system {:port 3334})
  (gcp/start system)
  (pg/start system)

  (gcp/stop system)
  (http/stop system)

  (def ctx (system/new-context system))

  (defmethod rpc/op :get-sandbox
    [ctx req]
    (println ::remote-addr (svs.http/ctx-remote-addr ctx))
    (let [pkg-blob (gcp/get-blob ctx (gcp/package-file-name "hl7.fhir.r4.core" "4.0.1" "package.json"))]
      {:status 200
       :body (http/response-body ctx (pg/execute! ctx ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"]))}))

  (http/request ctx {:path "/sandbox"})

  (pg/execute! ctx ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"])

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


  (def storage (gcp/mk-storage))

  (def pkgs
    (time
     (->> (gcp/objects)
          (filterv (fn [x] (str/ends-with? (.getName x) "package.json")))
          (pmap (fn [x] (gcp/blob-content x {:json true})))
          (into []))))

  (count pkgs)
  (mapv (fn [x] [(:name x) (:version x)]) pkgs)

  (def blb (gcp/get-blob storage gcp/get-ig-bucket "p/hl7.fhir.r4.core/4.0.1/structuredefinition.ndjson.gz"))
  (def sds (gcp/read-ndjson-blob blb))

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

  (def cs-blb (gcp/get-blob storage gcp/get-ig-bucket "p/hl7.fhir.r4.core/4.0.1/codesystem.ndjson.gz"))
  (def codesystems (gcp/read-ndjson-blob cs-blb))
  (def cs-idx (url-idx codesystems))

  (def vs-blb (gcp/get-blob storage gcp/get-ig-bucket "p/hl7.fhir.r4.core/4.0.1/valueset.ndjson.gz"))
  (def valuesets (gcp/read-ndjson-blob vs-blb))
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


  (def cs-blb (gcp/get-blob storage gcp/get-ig-bucket "p/hl7.fhir.r4.core/4.0.1/codesystem.ndjson.gz"))
  (def codesystems (gcp/read-ndjson-blob cs-blb))

  (def cs-b (gcp/get-blob storage gcp/get-ig-bucket "p/hl7.fhir.r4.core/4.0.1/codesystem.ndjson.gz"))

  (time (gcp/blob-stream cs-b {:gzip true} (fn [in] (pg/copy-ndjson-stream ctx "cs" in))))

  (pg/execute! ctx ["create table cs (resource jsonb)"])

  (pg/execute! ctx ["select * from cs limit 1"])

  (pg/execute! ctx ["create table tcs (resource jsonb)"])
  (pg/execute! ctx ["create table tcs_concept (resource jsonb)"])


  (do
    (pg/execute! ctx ["truncate tcs; truncate tcs_concept"])
    (time
     (let [pkg (gcp/blob-content  (gcp/package-file "hl7.fhir.r4.core" "4.0.1" "package.json") {:json true})
           pkg-fields {:package (:name pkg) :package_version (:version pkg)}]
       (pg/copy-ndjson ctx "tcs_concept"
                       (fn [csc-write]
                         (pg/copy-ndjson ctx "tcs"
                                         (fn [cs-write]
                                           (gcp/process-ndjson-blob
                                            (gcp/package-file "hl7.fhir.r4.core" "4.0.1" "codesystem.ndjson.gz")
                                            (fn [res _ln]
                                              (let [tcs (fhirschema.terminology/tiny-codesystem res)]
                                                (cs-write (merge (dissoc tcs :concept) pkg-fields))
                                                (when (= "complete" (:content tcs))
                                                  (doseq [c (:concept tcs)]
                                                    (csc-write (merge (assoc c :codesystem (:url tcs) :version (:version tcs))
                                                                      pkg-fields))))))))))))))

  (time
   (gcp/process-ndjson-blob (gcp/package-file "hl7.fhir.r4.core" "4.0.1" "structuredefinition.ndjson.gz")
    (fn [res ln] (println ln (:url res) (:kind res) (:derivation res)))))

  (pg/execute! ctx ["select * from tcs where resource->>'content' <> 'complete' limit 10"])

  (pg/execute! ctx ["select * from tcs where resource->>'content' <> 'complete' limit 10"])
  (pg/execute! ctx ["select resource->>'content', count(*) from tcs group by 1 order by 2 desc"])

  {:select {:c [:. :content] :cnt [:count :*]} :from :tcs :group-by 1 :order-by [:desc 2]}

  (pg/execute! ctx ["select count(*) from tcs"])

  (pg/execute! ctx ["select * from tcs_concept limit 100"])


  (def pkgs
    (time
     (->> (gcp/objects)
          (filterv (fn [x] (str/ends-with? (.getName x) "package.json")))
          (pmap (fn [x] (gcp/blob-content x {:json true})))
          (into []))))

  (pg/copy-ndjson
   ctx "packages"
   (fn [write]
     (->> (gcp/objects)
          (filterv (fn [x] (str/ends-with? (.getName x) "package.json")))
          (pmap (fn [x]
                  (let [pkg (gcp/blob-content x {:json true})]
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



