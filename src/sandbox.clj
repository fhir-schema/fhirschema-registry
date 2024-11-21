(ns sandbox
  (:require [pg.core :as pg]
            [cheshire.core]
            [clj-yaml.core]
            [clojure.string :as str]
            [ndjson]
            [fhirschema.transpiler :refer [translate]]))

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

  (count sds)

  (mapv (fn [r]
          (spit (str "tmp/schemas/" (last (str/split (:url r) #"/"))  ".yaml")
                (clj-yaml.core/generate-string (translate r)))) sds)
  


  )


