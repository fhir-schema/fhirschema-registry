(ns fhirschema.registry
  (:require
   [system]
   [svs.gcp :as gcp]
   [svs.http :as http]
   [svs.pg :as pg]
   [svs.logger :as log]
   [clojure.string :as str]
   [cheshire.core :as json]
   [clojure.tools.cli :refer [parse-opts]]
   [rpc])
  (:gen-class))

(defn from-json [x]
  (json/parse-string x keyword))

(defn to-json [x]
  (json/generate-string x))


(defn stop [sys]
  (system/stop-system sys))

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
    (log/info ctx ::packages q params)
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

(def cli-options
  ;; An option with a required argument
  [["-p" "--port PORT" "Port number"
    :default 80
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]

   ["-v" nil "Verbosity level"
    :id :verbosity
    :default 0
    :update-fn inc]

   ["-h" "--help"]])

(defn start [sys _config]
  (http/register-endpoint sys :get "/Package" #'get-package)
  (http/register-endpoint sys :get "/Package/$deps" #'get-package-deps)
  (http/register-endpoint sys :get "/Package/$lookup" #'get-package-lookup)
  (http/register-endpoint sys :get "/FHIRSchema" #'get-fhirschema)
  (log/info sys ::started "started"))

(defn main [config]
  (system/start-system
   (assoc config :services ["svs.pg" "svs.http" "svs.gcp" "fhirschema.registry"])))

(defn -main [& args]
  (let [opts (parse-opts args cli-options)]
    (main {:svs.http {:port (get-in opts [:options :port])}})))

(comment

  (def pg-conn (json/parse-string (slurp "gcp-connection.json") keyword))
  (def system (main {:svs.pg pg-conn :svs.http {:port 7777}}))

  (system/stop-system system)

  (stop system)

  (http/request system {:path "/Package?name=r4"})

  (http/request system {:path "/Package/$lookup?name=hl7%20fhir%20core"})

  )
