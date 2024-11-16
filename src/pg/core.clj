(ns pg.core
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [cheshire.core])
  (:import (java.sql Connection DriverManager)
           (java.io BufferedInputStream BufferedReader FileInputStream FileNotFoundException InputStream InputStreamReader)
           (java.util.zip GZIPInputStream)
           (org.postgresql Driver PGConnection PGProperty)
           [java.sql PreparedStatement ResultSet]
           (org.postgresql.copy CopyManager)
           [org.postgresql.jdbc PgArray]
           [org.postgresql.util PGobject]))

(defn coerce [r]
  (->> r
       (reduce (fn [acc [k v]]
                 (assoc acc (keyword (name k))
                        (cond (instance? PGobject v) (cheshire.core/parse-string (.getValue ^PGobject v) keyword)
                              (instance? java.math.BigDecimal v) (double v)
                              (instance? PgArray v) (vec (.getArray v))
                              :else v))
                 ) {})))

(defn connection [ztx]
  (get @ztx :pg))

(defn readonly-connection [ztx]
  (get-in @ztx [:ctx :pg-readonly]))

(defn execute* [conn q]
  (->> (jdbc/execute! conn q {:builder-fn rs/as-unqualified-maps})
       (mapv coerce)))


(defn execute! [ztx q]
  (->> (jdbc/execute! (connection ztx) q)
       (mapv coerce)))

(defn array-of [ztx type array]
  (.createArrayOf (connection ztx) type (into-array String array)))

(defn truncate! [ztx t]
  (->> (jdbc/execute! (connection ztx) [(format "truncate \"%s\"" t)])
       (mapv coerce)))

;; TODO: fix for safe execute
(defn safe-execute! [ztx q]
  (->> (jdbc/execute! (connection ztx) q)
       (mapv coerce)))

(defn get-connection [opts]
  (let [conn-string (str "jdbc:postgresql://"
                         (get opts :host "localhost" )
                         ":"(get opts :port 5432)
                         "/" (get opts :database )
                         "?stringtype=unspecified")]
    (DriverManager/getConnection conn-string (:user opts) (:password opts))))


(defn fetch [ztx sql-vector fetch-size field on-row]
  (with-open [^PreparedStatement ps (jdbc/prepare (connection ztx) sql-vector)]
    (.setFetchSize ps fetch-size)
    (let [^ResultSet rs  (.executeQuery ps)]
      (loop [i 0]
        (if (.next rs)
          (do
            (on-row (.getString rs field) i)
            (recur (inc i)))
          i)))))


(defn start-2 [ztx config]
  (if-let [c config]
    (let [tmp-conn (get-connection c)
          res (jdbc/execute! tmp-conn ["SELECT * FROM pg_database WHERE datname = ?" (:database config)])]
      (when-not (seq res)
        (println :ensure-db (jdbc/execute! tmp-conn [(str "create database " (:database config))])))
      (get-connection (assoc c :database (:database config))))
    (get-connection config)))

(defn copy-ndjson [ztx file-path]
  (with-open [gzip-stream (-> file-path io/input-stream GZIPInputStream. InputStreamReader. BufferedReader.)]
    (let [copy-manager (CopyManager. (.unwrap ^Connection (connection ztx) PGConnection))
          copy-sql "COPY _resources (resource) FROM STDIN csv quote e'\\x01' delimiter e'\\t'"]
      (.copyIn copy-manager copy-sql gzip-stream))))

(defn stop [ztx _config]
  (when-let [state (get @ztx :pg)]
    (.close state)))

(defn start [ztx opts]
  (let [db (get-connection opts)]
    (println :db db (jdbc/execute! db ["select 1"]))
    (swap! ztx assoc :pg db)
    :done))

(defn stop [ztx]
  (when-let [^Connection conn (get-in @ztx [:pg])]
    (.close conn)))

(comment
  (def ztx (atom {}))

  (def conn (cheshire.core/parse-string (slurp "connection.json") keyword))

  conn

  (start ztx conn)

  (execute! ztx ["create table _resources (resource jsonb)"])
  (execute! ztx ["select count(*) from _resources"])
  (execute! ztx ["vacuum analyze _resources"])


  (with-open [^PreparedStatement ps (jdbc/prepare (connection ztx) ["select name from packages where name ilike ?" "%"])]
    (.setFetchSize ps 100)
    (let [^ResultSet rs  (.executeQuery ps)]
      (loop [i 0]
        (if (.next rs)
          (do
            (println (.getString rs "name"))
            (recur (inc i)))
          i))))

  (fetch ztx  ["select name from package_names where name ilike ?" "%"]
         100 "name" (fn [x i] (println i x)))


  ;; (copy-ndjson ztx "resources.ndjson.gz")
  (connection ztx)


  (execute! ztx ["select count(*) from (select resource->>'name' from _resources where resource->>'resourceType' = 'Package' group by resource->>'name') _"])

  (def pkgs
    (execute! ztx ["select * from _resources where resource->>'resourceType' = 'Package' order by resource->>'name'"]))

  (count pkgs)

  (fhirschema.registry.core/write-ndjson-gz "packages.ndjson.gz"
                                            (fn [w]
                                              (doseq [x pkgs]
                                                (.write w (cheshire.core/generate-string (:resource x)))
                                                (.write w "\n"))))

  (take 100 pkgs)

  (execute! ztx ["select count(*) from _resources where resource->>'resourceType' = 'FHIRSchema'"])
  (execute! ztx ["select count(*) from _resources where resource->>'resourceType' = 'ValueSet'"])

  (execute! ztx ["create index resources_rt_idx on _resources ((resource->>'resourceType'))"])

  (stop ztx)

  (execute! ztx ["select * from _resources where resource->>'resourceType' = 'Package' and resource->>'name' ilike '%core%' order by resource->>'name'"])


  "https://storage.googleapis.com/fhir-packages/v1/Berkay.Sandbox/v0.0.1.ndjson.gz"

  (execute! ztx ["

create table packages as (
select
resource->'url' as name,
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

  )
