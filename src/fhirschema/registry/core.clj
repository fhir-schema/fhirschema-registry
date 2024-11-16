(ns fhirschema.registry.core
  (:require [http.core :as http]
            [pg.core :as pg]
            [clojure.string :as str]
            [cheshire.core :as json]
            [rpc]))

(defn from-json [x]
  (json/parse-string x keyword))

(defn to-json [x]
  (json/generate-string x))

(defn start [ztx config]
  (pg/start ztx (:pg config))
  (http/start ztx (:http config)))

(defn stop [ztx]
  (pg/stop ztx)
  (http/stop ztx))


(defmethod rpc/op :get-package
  [ztx req]
  (println @ztx)
  {:status 200
   :body (pg/execute! ztx ["select * from packages limit 10"])})

(defmethod rpc/op :get-package-lookup
  [ztx {params :query-params}]
  (println params)
  (let [results (pg/execute! ztx ["select name, versions from package_names where name ilike ? order by name limit 100"
                                  (str "%" (-> (str/replace (str/trim (or (:name params) "")) #"\s+" "%")
                                               (str/replace #"\." "\\.")))])]
    {:status 200
     :body results}))


(comment
  (def ztx (atom {}))

  (start ztx {:pg (json/parse-string (slurp "connection.json") keyword)
              :http {:port 7777}})

  ztx

  (stop ztx)


  (require '[org.httpkit.client :as cl])

  (from-json (slurp (:body @(cl/get "http://localhost:7777/Package?name=r4"))))

  (time
   (from-json (slurp (:body @(cl/get "http://localhost:7777/Package/$lookup?name=fhir.r4")))))

  (pg/execute! ztx ["select 1"])
  (pg/execute! ztx ["select name from packages where name ilike ? order by name limit 100" "%r4%"])

  (pg/execute! ztx ["select name, versions from package_names where name ilike ? order by name limit 100" "%fhir\\.r4%"])

  (pg/execute! ztx ["

drop table packages;
create table packages as (
select
resource->>'url' as name,
case when jsonb_typeof(resource->'fhirVersions') = 'array' then
(select array_agg(x)::text[] from jsonb_array_elements_text(resource->'fhirVersions') x)
else
ARRAY[resource->>'fhirVersions']
end as fhirVersions,
resource->>'description' as description,
resource->>'author' as author,
resource->>'version' as version,
resource->>'dependencies' as dependencies
from _resources
where resource->>'resourceType' = 'Package'
)
"])

  (pg/execute! ztx ["
drop table if exists package_names;

create table package_names as (
select name, array_agg(version) as versions
 from (select name, version from packages order by name, version) _
 group by name
 order by name
)

"])

  (pg/execute! ztx ["CREATE EXTENSION IF NOT EXISTS pg_trgm; "])

  (pg/execute! ztx ["create index package_names_name_trgrm on package_names USING gin (name gin_trgm_ops)"])

  )
