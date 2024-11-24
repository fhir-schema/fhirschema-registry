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
           (org.postgresql.copy CopyManager CopyIn)
           (com.zaxxer.hikari HikariConfig HikariDataSource)
           [org.postgresql.jdbc PgArray]
           [org.postgresql.util PGobject]))



(set! *warn-on-reflection* true)

(defn coerce [r]
  (->> r
       (reduce (fn [acc [k v]]
                 (assoc acc (keyword (name k))
                        (cond (instance? PGobject v) (cheshire.core/parse-string (.getValue ^PGobject v) keyword)
                              (instance? java.math.BigDecimal v) (double v)
                              (instance? PgArray v) (vec (.getArray ^PgArray v))
                              :else v))
                 ) {})))


(defn readonly-connection [ztx]
  (get-in @ztx [:ctx :pg-readonly]))

(defn execute* [conn q]
  (->> (jdbc/execute! conn q {:builder-fn rs/as-unqualified-maps})
       (mapv coerce)))

(defn datasource [ztx]
  (:pg @ztx))

(defn connection [ztx]
  (let [^HikariDataSource datasource (:pg @ztx)]
    (.getConnection datasource)))

(defn with-connection [ztx f]
  (with-open [^Connection conn (datasource ztx)]
    (f conn)))

(defn execute! [ztx q]
  (->> (jdbc/execute! (datasource ztx) q)
       (mapv coerce)))

(defn array-of [ztx type array]
  (with-connection ztx (fn [c] (.createArrayOf ^Connection c type (into-array String array)))))

(defn truncate! [ztx t]
  (->> (jdbc/execute! (datasource ztx) [(format "truncate \"%s\"" t)])
       (mapv coerce)))

;; TODO: fix for safe execute
(defn safe-execute! [ztx q]
  (->> (jdbc/execute! (datasource ztx) q)
       (mapv coerce)))

;; (defn get-connection [opts]
;;   (let [conn-string (str "jdbc:postgresql://"
;;                          (get opts :host "localhost" )
;;                          ":"(get opts :port 5432)
;;                          "/" (get opts :database )
;;                          "?stringtype=unspecified")]
;;     (DriverManager/getConnection conn-string (:user opts) (:password opts))))


(defn get-pool [conn]
  (let [^HikariConfig config (HikariConfig.)]
    (doto config
      (.setJdbcUrl (str "jdbc:postgresql://" (:host conn) ":" (:port conn) "/" (:dbname conn)))
      (.setUsername (:user conn))
      (.setPassword (:password conn))
      (.setMaximumPoolSize 10)
      (.setMinimumIdle 5)
      (.setIdleTimeout 300000)
      (.setConnectionTimeout 20000)
      (.addDataSourceProperty "cachePrepStmts" "true")
      (.addDataSourceProperty "prepStmtCacheSize" "250")
      (.addDataSourceProperty "prepStmtCacheSqlLimit" "2048"))
    (HikariDataSource. config)))


(defn fetch [ztx sql-vector fetch-size field on-row]
  (with-open [^Connection c (connection ztx)
              ^PreparedStatement ps (jdbc/prepare c sql-vector)]
    (.setFetchSize ps fetch-size)
    (let [^ResultSet rs  (.executeQuery ps)]
      (loop [i 0]
        (if (.next rs)
          (do
            (on-row (.getString rs ^String field) i)
            (recur (inc i)))
          i)))))

(defn copy-ndjson-stream [ztx table ^InputStream stream & [jsonb-column]]
  (with-open [^Connection c (connection ztx)]
    (let [copy-manager (CopyManager. (.unwrap ^Connection c PGConnection))
          copy-sql (str "COPY " table " (" (or jsonb-column "resource") " ) FROM STDIN csv quote e'\\x01' delimiter e'\\t'")]
      (.copyIn copy-manager copy-sql stream))))

(defn copy-ndjson-file [ztx file-path]
  (with-open [gzip-stream (-> file-path io/input-stream GZIPInputStream. InputStreamReader. BufferedReader.)]
    (copy-ndjson-stream ztx "_resource" gzip-stream)))

(def ^bytes NEW_LINE (.getBytes "\n"))

(defn copy [ztx sql cb]
  (with-open [^Connection c (connection ztx)]
    (let [^CopyManager cm (CopyManager. (.unwrap ^Connection c PGConnection))
          ^CopyIn ci (.copyIn cm sql)
          write (fn wr [^String s]
                  (let [^bytes bt (.getBytes s)]
                    (.writeToCopy ci bt 0 (count bt))
                    (.writeToCopy ci NEW_LINE 0 1)))]
      (try
        (cb write)
        (finally
          (.endCopy  ci))))))

(defn copy-ndjson [ztx table cb]
  (with-open [^Connection c (connection ztx)]
    (let [^CopyManager cm (CopyManager. (.unwrap ^Connection c PGConnection))
          copy-sql (str "COPY " table " (" (or "resource") " ) FROM STDIN csv quote e'\\x01' delimiter e'\\t'")
          ^CopyIn ci (.copyIn cm copy-sql)
          write (fn wr [res]
                  (let [^bytes bt (.getBytes (cheshire.core/generate-string res))]
                    (.writeToCopy ci bt 0 (count bt))
                    (.writeToCopy ci NEW_LINE 0 1)))]
      (try (cb write)
           (finally (.endCopy  ci))))))

#_(defmacro load-data [ztx writers & rest]
  (println :? writers)
  (let [cns (->> writers
                 (partition 2)
                 (mapcat (fn [[b _]] [(symbol (str b "-connection")) (list 'connection '_ztx)]))
                 (into []))
        wrts (->> writers
                  (partition 2)
                  (mapcat (fn [[b opts]]
                            (let [in-nm (symbol (str b "-in"))]
                              [in-nm (list 'make-copy-in (symbol (str b "-connection")) opts)
                               b (list 'make-writer in-nm opts)])))
                  (into []))]
    `(let [_ztx ~ztx]
         (with-open ~cns
           (let ~wrts
             ~@rest)))))


;; (defn start-2 [ztx config]
;;   (if-let [c config]
;;     (let [tmp-conn (get-connection c)
;;           res (jdbc/execute! tmp-conn ["SELECT * FROM pg_database WHERE datname = ?" (:database config)])]
;;       (when-not (seq res)
;;         (println :ensure-db (jdbc/execute! tmp-conn [(str "create database " (:database config))])))
;;       (get-connection (assoc c :database (:database config))))
;;     (get-connection config)))

(defn start [ztx opts]
  (let [db (get-pool opts)]
    (println :db db (jdbc/execute! db ["select 1"]))
    (swap! ztx assoc :pg db)
    :done))

(defn stop [ztx]
  (when-let [^HikariDataSource conn (get-in @ztx [:pg])]
    (.close conn)))

(comment
  (def ztx (atom {}))

  (def conn (cheshire.core/parse-string (slurp "connection.json") keyword))

  (start ztx conn)
  (stop ztx)

  (execute! ztx ["select 1"])
  (execute! ztx ["create table _resources (resource jsonb)"])
  (execute! ztx ["select count(*) from _resources"])
  (execute! ztx ["vacuum analyze _resources"])


  (execute! ztx ["select count(*) from _resources where resource->>'resourceType' = 'FHIRSchema'"])
  (execute! ztx ["select count(*) from _resources where resource->>'resourceType' = 'ValueSet'"])
  (execute! ztx ["create index resources_rt_idx on _resources ((resource->>'resourceType'))"])
  (execute! ztx ["select * from _resources where resource->>'resourceType' = 'Package' and resource->>'name' ilike '%core%' order by resource->>'name'"])

  (execute! ztx ["create table test (resoruce jsonb)"])

  (dotimes [i 20]
    (copy ztx "copy test (resource) FROM STDIN csv quote e'\\x01' delimiter e'\\t'"
          (fn [w]
            (doseq [i (range 100)]
              (w (cheshire.core/generate-string {:a i}))))))

  (execute! ztx ["select count(*) from test"])
  (execute! ztx ["truncate test"])

  (dotimes [i 100]
    (fetch ztx ["select resource from test"] 100 "resource" (fn [x i] (print "."))))



  )
