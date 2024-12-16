(ns far
  (:require
   [system]
   [gcs]
   [http]
   [pg]
   [pg.repo]
   [clojure.string :as str]
   [cheshire.core :as json]
   [far.package]
   [far.tx]
   [clojure.tools.cli :refer [parse-opts]]))

(system/defmanifest
  {:description "FHIR Canonicals Repository"
   :deps ["pg" "http"]})

(defn from-json [x]
  (json/parse-string x keyword))

(defn to-json [x]
  (json/generate-string x))

(defn get-package [ctx _req]
  {:status 200
   :body (pg/execute! ctx ["select * from packages limit 10"])})

(str/replace "fhir oups" #"\." "\\\\.")

(defn mk-query [s]
  (str "%" (-> (str/replace (str/trim (or s "")) #"\s+" "%")
               (str/replace #"\." "\\\\."))
       "%"))

(defn get-package-lookup
  [ctx {params :query-params :as req}]
  (let [q (mk-query (:name params))]
    (system/info ctx ::packages q params)
    (http/stream
     req
     (fn [wr]
       (pg/fetch ctx  ["select jsonb_build_object('name',name,'versions', versions) as res from package_names where name ilike ?" q]
                 100 "res" (fn [res _i] (wr res)))))))


(defn get-fhirschema
  [ctx {params :query-params :as req}]
  (let [[pkg v] (str/split (or (:package params) "") #":")]
    (println :schemas pkg v)
    (if (and (not pkg) (not v))
      {:status 422 :body {:message "Parameter name=<package>:<version> is required"}}
      (http/stream
       req
       (fn [wr]
         (pg/fetch
          ctx  ["select resource as res  from fhirschemas where package_name = ? and package_version = ?" pkg v] 100 "res"
          (fn [res _i] (wr res))))))))


(def canonicals-summary-sql
  "select row_to_json(c.*) as res from canonicals c where package_name = ? and package_version = ?  order by resourceType, url")

(defn get-canonicalresource-summary
  [ctx {params :query-params :as req}]
  (let [[pkg v] (str/split (or (:package params) "") #":")]
    (println :schemas pkg v)
    (if (and (not pkg) (not v))
      {:status 422 :body {:message "Parameter name=<package>:<version> is required"}}
      (http/stream
       req
       (fn [wr]
         (pg/fetch ctx  [canonicals-summary-sql pkg v] 100 "res" (fn [res _i] (wr res))))))))


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

(defn get-package-deps
  [ctx {params :query-params :as req}]
  (let [[pkg v] (str/split (or (:package params) "") #":")]
    (println :schemas pkg v)
    (if (and (not pkg) (not v))
      {:status 422 :body {:message "Parameter name=<package>:<version> is required"}}
      (http/stream
       req
       (fn [wr] (pg/fetch ctx  [deps-sql pkg v] 100 "res" (fn [res _i] (wr res))))))))



(system/defstart [context _config]
  (http/register-endpoint context {:method :get :path "/Package" :fn #'get-package})
  (http/register-endpoint context {:method :get :path "/Package/$deps" :fn #'get-package-deps})
  (http/register-endpoint context {:method :get :path "/Package/$lookup" :fn #'get-package-lookup})
  (http/register-endpoint context {:method :get :path "/FHIRSchema" :fn #'get-fhirschema})
  (system/info context ::started "started")
  {})

(system/defstop [context state])

(defn main [config]
  (system/start-system
   (assoc config
          :services ["pg" "http" "http.openapi" "gcs" "far"]
          :pg (cheshire.core/parse-string (slurp "connection.json") keyword))))


(comment

  (def context (system/start-system {:services ["http" "http.openapi" "pg" "gcs" "far" "far.ui"]
                                     :http {:port 7777}
                                     :pg (json/parse-string (slurp "connection.json") keyword)}))

  (system/stop-system context)

  (http/request context {:path "/api"})

  (http/request context {:path "/Package" :query-params {:name "r4"}})

  (http/request context {:path "/Package/$lookup" :query-params {:name "hl7.fhir.core"}})

  (def vs (pg.repo/read context {:table "canonical" :match {:id "04479aa4-c6e0-5dd2-827c-f2036a35fd66"}}))

  (def vs (pg.repo/read context {:table "valueset" :match {:id "04479aa4-c6e0-5dd2-827c-f2036a35fd66"}}))
  (def dep (pg.repo/select context {:table "canonical_deps" :match {:definition_id "04479aa4-c6e0-5dd2-827c-f2036a35fd66"}}))

  (:package_id vs)

  dep

  (far.package.loader/resolve-dep context dep)

  (far.package/canonical-deps context vs)

  (def vs (pg.repo/read context {:table "valueset" :match {:id "2029daa1-25b6-58d5-a42b-f58441b58095"}}))
  (def dep (pg.repo/read context {:table "canonical_deps" :match {:definition_id (:id vs)}}))



  (far.package.loader/get-deps-idx context (:package_id dep))

  (far.package.loader/resolve-dep context dep)



  (let [pid (:package_id vs)
        vid (:id vs)]
    (time
     (do
       (pg.repo/delete context {:table "canonical_deps" :match {:package_id pid :definition_id vid}})
       (pg.repo/load
        context {:table "canonical_deps"}
        (fn [insert]
          (pg.repo/fetch
           context {:table "valueset" :match {:package_id pid :id vid}}
           (fn [vs]
             (doseq [d  (far.package.canonical-deps/extract-deps vs)]
               (insert (far.package.loader/resolve-dep context d))))))))))




  )

