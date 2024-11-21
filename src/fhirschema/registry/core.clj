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

#_(defmethod rpc/op :get-package-lookup
  [ztx {params :query-params}]
  (println params)
  (let [q (mk-query (:name params))
        _ (println :q q )
        results (pg/execute! ztx ["select name, versions from package_names where name ilike ? order by name limit 100" q])]
    {:status 200
     :body results}))

#_(defmethod rpc/op :get-package-lookup
  [ztx {params :query-params :as req}]
  (let [q (mk-query (:name params))]
    (println :packages q params)
    (http/stream
     req
     (fn [wr]
       (pg/fetch ztx  ["select jsonb_build_object('name',name,'versions', versions) as res from package_names where name ilike ?" q]
                 100 "res" (fn [res _i] (wr res)))))))

(defmethod rpc/op :get-package-lookup
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


(def canonicals-summary-sql
  "select row_to_json(c.*) as res from canonicals c where package_name = ? and package_version = ?  order by resourceType, url")

(defmethod rpc/op :get-canonicalresource-summary
  [ztx {params :query-params :as req}]
  (let [[pkg v] (str/split (or (:package params) "") #":")]
    (println :schemas pkg v)
    (if (and (not pkg) (not v))
      {:status 422 :body {:message "Parameter name=<package>:<version> is required"}}
      (http/stream
       req
       (fn [wr]
         (pg/fetch ztx  [canonicals-summary-sql pkg v] 100 "res" (fn [res _i] (wr res))))))))


(def deps-sql
  "
-- Recursive query to find all package dependencies with versions
WITH RECURSIVE dep_tree AS (
    -- Base case: direct dependencies
    SELECT
        name,
        version,
        dep_name,
        dep_version,
        1 as depth,
        ARRAY[name || '@' || version] as parents,
        ARRAY[name || '@' || version, dep_name || '@' || dep_version] as dep_path
    FROM package_deps
    WHERE name = ?  -- Starting package
    AND version = ?  -- Starting version
    UNION ALL
    -- Recursive case: dependencies of dependencies
    SELECT
        pd.name,
        pd.version,
        pd.dep_name,
        pd.dep_version,
        dt.depth + 1,
        dt.parents || (pd.name || '@' || pd.version),
        dt.dep_path || (pd.dep_name || '@' || pd.dep_version)
    FROM dep_tree dt
    JOIN package_deps pd 
        ON pd.name = dt.dep_name 
        AND pd.version = dt.dep_version
    WHERE NOT (pd.dep_name || '@' || pd.dep_version) = ANY(dt.dep_path)  -- Prevent cycles
)
SELECT row_to_json(c.*) as res from (
SELECT
    dep_name as package,
    dep_version as version,
    array_agg(
      distinct array_to_string(dep_path, ' -> ')
    ) as dependency_paths
FROM dep_tree
group by 1,2
ORDER BY dep_name
) c

")

(defmethod rpc/op :get-package-deps
  [ztx {params :query-params :as req}]
  (let [[pkg v] (str/split (or (:package params) "") #":")]
    (println :schemas pkg v)
    (if (and (not pkg) (not v))
      {:status 422 :body {:message "Parameter name=<package>:<version> is required"}}
      (http/stream
       req
       (fn [wr] (pg/fetch ztx  [deps-sql pkg v] 100 "res" (fn [res _i] (wr res))))))))


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

  @(cl/get "http://localhost:7777/Package/$deps?package=hl7.fhir.r4.core:4.0.1")


  @(cl/get "http://localhost:7777/stream?name=hl7")

  @(cl/get "http://localhost:7777/CanonicalResource/$summary?package=hl7.fhir.r4.core:4.0.1")

  (pg/execute! ztx ["
select
resource#>'{meta,package}' as pkg,
resource->>'resourceType' as rt,
count(*)
from _resources
group by 1, 2
order by 1
limit 10
"])

  (pg/execute! ztx ["
drop table canonicals;
create table canonicals AS (
select
resource#>>'{meta,package,url}' as package_name,
resource#>>'{meta,package,version}' as package_version,
resource->>'resourceType' as resourceType,
resource->>'url' as url,
resource->>'version' as version,
resource->>'description' as description,
jsonb_strip_nulls(jsonb_build_object(
  'kind', resource->>'kind',
  'type', resource->>'type',
  'derivation', resource->>'derivation',
  'base', resource->>'base'
)) as extra
from _resources
order by package_name, package_version, url, resourceType, version
)
"])

  (pg/execute! ztx ["create index canoninicals_package_idx on canonicals (package_name, package_version)"])


  (pg/execute! ztx ["select * from canonicals where package_name = 'hl7.fhir.r4.core'  order by resourceType, url limit 100"])

  ;; elements
  (do

    (pg/execute! ztx ["
drop table if exists elements;
create table elements as (
select
resource#>>'{meta,package,url}' as package_name,
resource#>>'{meta,package,version}' as package_version,
resource#>>'{url}' as url,
el#>>'{binding,valueSet}' as vs_url,
el#>>'{binding,strength}' as vs_strength,
el->'type' as type,
el->>'id' as id,
el->>'path' as path,
el->>'sliceName' as sliceName,
el->'slicing' as slicing,
el - '{id,path,binding,type, sliceName, slicing}'::text[] as rest
from _resources,
jsonb_array_elements(resource#>'{differential,element}') el
where resource->>'resourceType' = 'StructureDefinition'
)
"])

    (pg/execute! ztx ["create index elements_id_idx on elements USING gin (id gin_trgm_ops)"])
    (pg/execute! ztx ["create index elements_path_idx on elements USING gin (path gin_trgm_ops)"])
    (pg/execute! ztx ["create index elements_dep_idx on elements USING gin (package_name gin_trgm_ops)"])
    )

  (pg/execute! ztx ["select * from elements limit 10"])

  ;; todo do valueset analytics - calculate which could be enumerated


  (pg/execute! ztx ["
drop table if exists package_deps;
create table package_deps AS (
select
resource#>>'{meta, package,url}' as name,
resource#>>'{meta, package,version}' as version,
substring(kv.key::text,2) as dep_name,
kv.value::text as dep_version
from _resources,
jsonb_each_text(resource->'dependencies') as kv
where resource->>'resourceType' = 'Package'
order by name, version, dep_name, dep_version
)
"])

  (pg/execute! ztx ["select name, version, count(*) from package_deps group by 1,2 order by 3 desc limit 10"])

  (pg/execute! ztx ["select dep_name, dep_version, count(*) from package_deps group by 1,2  order by 3 desc limit 20"])

  (->> (pg/execute! ztx ["select package->>'url' as pkg, id, path, slicename from elements where id ilike '%:%:%:%' and slicename is not null limit 100"])
       (mapv (fn [x] [ (:pkg x) (:id x) (:path x) (:slicename x)])))

  (->> (pg/execute! ztx ["select package->>'url' as pkg, id, path, slicename from elements where path ilike '%[x]%' and slicename is not null limit 100"])
       (mapv (fn [x] [ (:pkg x) (:id x) (:path x) (:slicename x)])))

  (->> (pg/execute! ztx ["
select count(*) as cnt, package_name, slicing->'discriminator' as d
from elements
where slicing is not null
and package_name ilike 'hl7.fhir.us.core'
group by 2,3
order by 1 desc
 limit 1000"])
       (mapv (fn [x] [(:cnt x) (:package_name x) (:d x)])))

[[203 "hl7.fhir.us.core" [{:path "$this", :type "pattern"}]]
 [31 "hl7.fhir.us.core"  [{:path "code", :type "pattern"}]]
 [25 "hl7.fhir.us.core"  [{:path "type", :type "pattern"}]]
 [14 "hl7.fhir.us.core"  [{:path "url", :type "value"}]]
 [12 "hl7.fhir.us.core"  [{:path "coding.code", :type "value"} {:path "coding.system", :type "value"}]]
 [7 "hl7.fhir.us.core"   [{:path "code", :type "value"} {:path "system", :type "value"}]]
 [2 "hl7.fhir.us.core"   [{:path "system", :type "value"}]]
 [1 "hl7.fhir.us.core"   [{:path "$this", :type "type"}]]
 [1 "hl7.fhir.us.core"   [{:path "code.coding", :type "pattern"}]]]


  (pg/execute! ztx [
                    "
-- Recursive query to find all package dependencies with versions
WITH RECURSIVE dep_tree AS (
    -- Base case: direct dependencies
    SELECT
        name,
        version,
        dep_name,
        dep_version,
        1 as depth,
        ARRAY[name || '@' || version] as parents,
        ARRAY[name || '@' || version, dep_name || '@' || dep_version] as dep_path
    FROM package_deps
    WHERE name = 'ch.fhir.ig.ch-etoc'  -- Starting package
    AND version = '2.0.0'  -- Starting version
    UNION ALL
    -- Recursive case: dependencies of dependencies
    SELECT
        pd.name,
        pd.version,
        pd.dep_name,
        pd.dep_version,
        dt.depth + 1,
        dt.parents || (pd.name || '@' || pd.version),
        dt.dep_path || (pd.dep_name || '@' || pd.dep_version)
    FROM dep_tree dt
    JOIN package_deps pd
        ON pd.name = dt.dep_name
        AND pd.version = dt.dep_version
    WHERE NOT (pd.dep_name || '@' || pd.dep_version) = ANY(dt.dep_path)  -- Prevent cycles
)
SELECT
    dep_name as package,
    dep_version as version,
    array_agg(
      distinct array_to_string(dep_path, ' -> ')
    ) as dependency_paths
FROM dep_tree
group by 1,2
ORDER BY dep_name;

"])
  (pg/execute! ztx ["
create index _resource_package_idx on _resources (
  (resource#>>'{meta,package,url}'),
  (resource#>>'{meta,package,version}')
) "])

  (pg/execute! ztx ["
create index _resource_package_name_idx on _resources
USING gin (
  (resource#>>'{meta,package,url}') gin_trgm_ops
)"])




  (pg/execute! ztx ["
select
--resource#>'{meta,package}' as pkg,
--resource->'url' as url,
--resource
distinct resource->'resourceType'
from _resources
where
--resource->>'resourceType' = 'CodSystem' and
resource#>>'{meta,package,url}' = 'hl7.fhir.r4.core'
--and resource->>'url' = 'http://terminology.hl7.org/ValueSet/v3-AdministrativeGender'
"])

  (-> (pg/execute! ztx ["
select
resource
from _resources
where
resource->>'resourceType' = 'FHIRSchema' and
resource#>>'{meta,package,url}' = 'hl7.fhir.r4.core'
and resource->>'name' ='Questionnaire'
"])
      first
      :resource
      )

(->> (pg/execute! ztx ["
select distinct type
from elements
where slicing#>>'{discriminator,0,path}' = '$this'
and package_name ilike 'hl7.fhir.us.core'
 limit 10"])
     (mapv (fn [x] x)))

(->> (pg/execute! ztx ["
select *
from elements
where slicing#>>'{discriminator,0,path}' = '$this'
and package_name ilike 'hl7.fhir.us.core'
 limit 10"])
     (mapv (fn [x] x)))

[{:type ({:code "CodeableConcept"})} {:type ({:code "Identifier"})} {:type nil}]

(->> (pg/execute! ztx ["
select *
from elements
where slicing#>>'{discriminator,0,path}' = '$this'
and type is null
and package_name ilike 'hl7.fhir.us.core'
 limit 10"])
     (mapv (fn [x] x)))

(->> (pg/execute! ztx ["
select *
from elements
where
id ilike '%\\.extension%extension%'
and package_name ilike 'hl7.fhir.us.core'
order by path
 limit 1000"])
     (mapv (fn [x] [(:url x) (:id x) (:path x)])))


  )
