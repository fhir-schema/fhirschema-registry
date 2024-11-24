(ns sandbox
  (:require [pg.core :as pg]
            [cheshire.core]
            [gcp]
            [clj-yaml.core]
            [clojure.string :as str]
            [ndjson]
            [fhirschema.terminology]
            [fhirschema.transpiler :refer [translate]]))

(defn dump [x]
  (spit "/tmp/dump.yaml" (clj-yaml.core/generate-string x)))

(comment

  (def ztx (atom {}))

  (def conn (cheshire.core/parse-string (slurp "connection.json") keyword))

  (pg/start ztx conn)

  (pg/execute! ztx ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"])

  (pg/execute!
   ztx ["
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

  (pg/execute! ztx ["create index sd_package_name_trgrm on structuredefinitions USING gin (package_name gin_trgm_ops)"])

  (pg/execute! ["REFRESH MATERIALIZED VIEW structuredefinitions;"])

  (pg/execute! ztx ["
select distinct package_version
from structuredefinitions
where
 package_name ilike 'hl7.fhir.us.core'
limit 10"])

  (pg/execute! ztx ["select * from structuredefinitions where package_name ilike 'hl7.fhir.us.core' and package_version = '5.0.1'limit 10"])

  (->> (pg/execute! ztx ["select * from structuredefinitions where package_name ilike 'hl7.fhir.us.core' and package_version = '6.1.0'limit 10"])
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
     (->> (gcp/objects storage "fs.get-ig.org")
          (filterv (fn [x] (str/ends-with? (.getName x) "package.json")))
          (pmap (fn [x] (gcp/blob-content x {:json true})))
          (into []))))

  (count pkgs)

  (def pkgs
    (time
     (->> (gcp/objects storage "fs.get-ig.org")
          (filterv (fn [x] (str/starts-with? (.getName x) "/p/hl7.fhir.r4.core/4.0.1/codesystem.ndjson")))
          (pmap (fn [x] (gcp/read-ndjson-blob x {:json true})))
          (into []))))

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

  (time (gcp/blob-stream cs-b {:gzip true} (fn [in] (pg/copy-ndjson-stream ztx "cs" in))))

  (pg/execute! ztx ["create table cs (resource jsonb)"])

  (pg/execute! ztx ["select * from cs limit 1"])

  (pg/execute! ztx ["create table tcs (resource jsonb)"])
  (pg/execute! ztx ["create table tcs_concept (resource jsonb)"])


  (do
    (pg/execute! ztx ["truncate tcs; truncate tcs_concept"])
    (time
     (let [pkg (gcp/blob-content  (gcp/package-file "hl7.fhir.r4.core" "4.0.1" "package.json") {:json true})
           pkg-fields {:package (:name pkg) :package_version (:version pkg)}]
       (pg/copy-ndjson ztx "tcs_concept"
                       (fn [csc-write]
                         (pg/copy-ndjson ztx "tcs"
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

  (pg/execute! ztx ["select * from tcs where resource->>'content' <> 'complete' limit 10"])

  (pg/execute! ztx ["select * from tcs where resource->>'content' <> 'complete' limit 10"])
  (pg/execute! ztx ["select resource->>'content', count(*) from tcs group by 1 order by 2 desc"])

  {:select {:c [:. :content] :cnt [:count :*]} :from :tcs :group-by 1 :order-by [:desc 2]}

  (pg/execute! ztx ["select count(*) from tcs"])

  (pg/execute! ztx ["select * from tcs_concept limit 100"])

  )



