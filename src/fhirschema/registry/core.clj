(ns fhirschema.registry.core
  (:require [http.core :as http]
            [pg.core :as pg]
            [clojure.string :as str]
            [cheshire.core :as json]
            [rpc])
  (:gen-class))

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

(str/replace "fhir oups" #"\." "\\\\.")

(defn mk-query [s]
  (str "%" (-> (str/replace (str/trim (or s "")) #"\s+" "%")
               (str/replace #"\." "\\\\."))
       "%"))

(mk-query "a b")
(mk-query "FHIR b")

(defmethod rpc/op :get-package-lookup
  [ztx {params :query-params}]
  (println params)
  (let [q (mk-query (:name params))
        _ (println :q q )
        results (pg/execute! ztx ["select name, versions from package_names where name ilike ? order by name limit 100" q])]
    {:status 200
     :body results}))

(defmethod rpc/op :get-stream
  [ztx {params :query-params :as req}]
  (let [q (mk-query (:name params))]
    (println :packages q params)
    (http/stream
     req
     (fn [wr]
       (pg/fetch ztx  ["select jsonb_build_object('name',name,'versions', versions) as res from package_names where name ilike ?" q]
                 100 "res" (fn [res _i] (wr res)))))))

;; (str/split "a:b"  #":")

(defmethod rpc/op :get-fhirschema-old
  [ztx {params :query-params}]
  (let [[pkg v] (str/split (or (:package params) "") #":")
        _ (println :schemas pkg v params)
        results (pg/execute! ztx ["select resource from fhirschemas where package_name = ? and package_version = ?" pkg v])]
    {:status 200
     :body (mapv :resource results)}))

(defmethod rpc/op :get-fhirschema
  [ztx {params :query-params :as req}]
  (let [[pkg v] (str/split (or (:package params) "") #":")]
    (println :schemas pkg v)
    (if (and (not pkg) (not v))
      {:status 422 :body {:message "Parameter name=<package>:<version> is required"}}
      (http/stream
       req
       (fn [wr]
         (pg/fetch ztx  ["select resource as res  from fhirschemas where package_name = ? and package_version = ?" pkg v] 100 "res" (fn [res _i] (wr res))))))))

(defn -main [& args]
  (println :args args)
  (def ztx (atom {}))
  (start ztx {:pg (json/parse-string (slurp "connection.json") keyword)
              :http {:port 80}}))

(defn init [ztx]
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

  (pg/execute! ztx ["
drop table if exists fhirschemas;
drop index if exists fhirschemas_pkg;
drop index if exists fhirschemas_url;
create table fhirschemas as (
select
resource#>>'{meta,package,url}'     as package_name,
resource#>>'{meta,package,version}' as package_version,
resource->>'url' as url,
resource->>'version' as version,
resource as resource
from _resources
where resource->>'resourceType' = 'FHIRSchema'
order by 1,2,3,4
);
create index fhirschemas_pkg on fhirschemas (package_name, package_version);
create index fhirschemas_url on fhirschemas (url, version);
"])

  (pg/execute! ztx ["CREATE EXTENSION IF NOT EXISTS pg_trgm; "])

  (pg/execute! ztx ["create index package_names_name_trgrm on package_names USING gin (name gin_trgm_ops)"])

  )


(comment
  (def ztx (atom {}))

  (start ztx {:pg (json/parse-string (slurp "connection.json") keyword)
              :http {:port 7777}})

  ztx

  (stop ztx)


  (require '[org.httpkit.client :as cl])

  (from-json (slurp (:body @(cl/get "http://localhost:7777/Package?name=r4"))))

  (time
   (from-json (slurp (:body @(cl/get "http://localhost:7777/Package/$lookup?name=hl7%20fhir%20core")))))

  (pg/execute! ztx ["select 1"])
  (pg/execute! ztx ["select name from packages where name ilike ? order by name limit 100" "%r4%"])

  (pg/execute! ztx ["select name, versions from package_names where name ilike ? order by name limit 100" "%fhir\\.r4%"])

  


  @(cl/get "http://localhost:7777/Package/$lookup?name=hl7%20core")

  (time
   (def s (from-json (slurp (:body @(cl/get "http://localhost:7777/FHIRSchema?package=hl7.fhir.r4.core:4.0.1"))))))

  @(cl/get "http://localhost:7777/FHIRSchema?package=hl7.fhir.r4.core:4.0.1")


  @(cl/get "http://localhost:7777/stream?name=hl7")


  )
