(ns fhirschema.registry.core
  (:require
   [system]
   [gcp :as gcp]
   [http :as http]
   [pg :as pg]
   [clojure.string :as str]
   [cheshire.core :as json]
   [clojure.tools.cli :refer [parse-opts]]
   [rpc])
  (:gen-class))

(defn from-json [x]
  (json/parse-string x keyword))

(defn to-json [x]
  (json/generate-string x))

(defn start [sys config]
  (pg/start sys (:pg config))
  (http/start sys (:http config))
  (gcp/start sys (:gcp config)))

(defn stop [sys]
  (system/stop-system sys))

(defmethod rpc/op :get-package
  [ctx _req]
  {:status 200
   :body (pg/execute! ctx ["select * from packages limit 10"])})

(str/replace "fhir oups" #"\." "\\\\.")

(defn mk-query [s]
  (str "%" (-> (str/replace (str/trim (or s "")) #"\s+" "%")
               (str/replace #"\." "\\\\."))
       "%"))

(defmethod rpc/op :get-package-lookup
  [ctx {params :query-params :as req}]
  (let [q (mk-query (:name params))]
    (println :packages q params)
    (http/stream
     req
     (fn [wr]
       (pg/fetch ctx  ["select jsonb_build_object('name',name,'versions', versions) as res from package_names where name ilike ?" q]
                 100 "res" (fn [res _i] (wr res)))))))

(defmethod rpc/op :get-fhirschema-old
  [ctx {params :query-params}]
  (let [[pkg v] (str/split (or (:package params) "") #":")
        _ (println :schemas pkg v params)
        results (pg/execute! ctx ["select resource from fhirschemas where package_name = ? and package_version = ?" pkg v])]
    {:status 200
     :body (mapv :resource results)}))

(defmethod rpc/op :get-fhirschema
  [ctx {params :query-params :as req}]
  (let [[pkg v] (str/split (or (:package params) "") #":")]
    (println :schemas pkg v)
    (if (and (not pkg) (not v))
      {:status 422 :body {:message "Parameter name=<package>:<version> is required"}}
      (http/stream
       req
       (fn [wr]
         (pg/fetch ctx  ["select resource as res  from fhirschemas where package_name = ? and package_version = ?" pkg v] 100 "res" (fn [res _i] (wr res))))))))


(def canonicals-summary-sql
  "select row_to_json(c.*) as res from canonicals c where package_name = ? and package_version = ?  order by resourceType, url")

(defmethod rpc/op :get-canonicalresource-summary
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

(defmethod rpc/op :get-package-deps
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

(defn -main [& args]
  (let [sys (system/new-system)
        opts (parse-opts args cli-options)]
    (start sys {:http {:port (get-in opts [:options :port])}})))

(comment

  (def system (system/new-system {}))

  (def pg-conn (json/parse-string (slurp "gcp-connection.json") keyword))

  (start system {:pg pg-conn :http {:port 7777}})

  (system/stop-system system)

  (stop system)

  (def ctx (system/new-context system))

  (http/request ctx {:path "/Package?name=r4"})
  (http/request ctx {:path "/Package/$lookup?name=hl7%20fhir%20core"})

  (parse-opts ["action" "-p" "8080"]  cli-options)


  ;;file, args, settings



  )
