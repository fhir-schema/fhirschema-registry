(ns svs.pg
  (:require
   [svs.logger :as log]
   [system]
   [clojure.string :as str]
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


(defn readonly-connection [ctx]
  (get-in @ctx [:ctx :pg-readonly]))

(defn execute* [conn q]
  (->> (jdbc/execute! conn q {:builder-fn rs/as-unqualified-maps})
       (mapv coerce)))

(defn datasource [ctx]
  (system/get-system-state ctx [:datasource]))

(defn connection [ctx]
  (let [^HikariDataSource datasource (datasource ctx)]
    (.getConnection datasource)))

(defn with-connection [ctx f]
  (with-open [^Connection conn (datasource ctx)]
    (f conn)))

(defn execute! [ctx q]
  (log/info ctx ::execute q)
  (->> (jdbc/execute! (datasource ctx) q)
       (mapv coerce)))

(defn array-of [ctx type array]
  (with-connection ctx (fn [c] (.createArrayOf ^Connection c type (into-array String array)))))

(defn truncate! [ctx t]
  (->> (jdbc/execute! (datasource ctx) [(format "truncate \"%s\"" t)])
       (mapv coerce)))

;; TODO: fix for safe execute
(defn safe-execute! [ctx q]
  (->> (jdbc/execute! (datasource ctx) q)
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


(defn fetch [ctx sql-vector fetch-size field on-row]
  (with-open [^Connection c (connection ctx)
              ^PreparedStatement ps (jdbc/prepare c sql-vector)]
    (.setFetchSize ps fetch-size)
    (let [^ResultSet rs  (.executeQuery ps)]
      (loop [i 0]
        (if (.next rs)
          (do
            (on-row (.getString rs ^String field) i)
            (recur (inc i)))
          i)))))

(defn copy-ndjson-stream [ctx table ^InputStream stream & [jsonb-column]]
  (with-open [^Connection c (connection ctx)]
    (let [copy-manager (CopyManager. (.unwrap ^Connection c PGConnection))
          copy-sql (str "COPY " table " (" (or jsonb-column "resource") " ) FROM STDIN csv quote e'\\x01' delimiter e'\\t'")]
      (.copyIn copy-manager copy-sql stream))))

(defn copy-ndjson-file [ctx file-path]
  (with-open [gzip-stream (-> file-path io/input-stream GZIPInputStream. InputStreamReader. BufferedReader.)]
    (copy-ndjson-stream ctx "_resource" gzip-stream)))

(def ^bytes NEW_LINE (.getBytes "\n"))

(defn copy [ctx sql cb]
  (with-open [^Connection c (connection ctx)]
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

(defn copy-ndjson [ctx table cb]
  (with-open [^Connection c (connection ctx)]
    (let [^CopyManager cm (CopyManager. (.unwrap ^Connection c PGConnection))
          copy-sql (str "COPY " table " (" (or "resource") " ) FROM STDIN csv quote e'\\x01' delimiter e'\\t'")
          ^CopyIn ci (.copyIn cm copy-sql)
          write (fn wr [res]
                  (let [^bytes bt (.getBytes (cheshire.core/generate-string res))]
                    (.writeToCopy ci bt 0 (count bt))
                    (.writeToCopy ci NEW_LINE 0 1)))]
      (try (cb write)
           (finally (.endCopy  ci))))))

#_(defmacro load-data [ctx writers & rest]
    (println :? writers)
    (let [cns (->> writers
                   (partition 2)
                   (mapcat (fn [[b _]] [(symbol (str b "-connection")) (list 'connection '_ctx)]))
                   (into []))
          wrts (->> writers
                    (partition 2)
                    (mapcat (fn [[b opts]]
                              (let [in-nm (symbol (str b "-in"))]
                                [in-nm (list 'make-copy-in (symbol (str b "-connection")) opts)
                                 b (list 'make-writer in-nm opts)])))
                    (into []))]
      `(let [_ctx ~ctx]
         (with-open ~cns
           (let ~wrts
             ~@rest)))))


;; (defn start-2 [ctx config]
;;   (if-let [c config]
;;     (let [tmp-conn (get-connection c)
;;           res (jdbc/execute! tmp-conn ["SELECT * FROM pg_database WHERE datname = ?" (:database config)])]
;;       (when-not (seq res)
;;         (println :ensure-db (jdbc/execute! tmp-conn [(str "create database " (:database config))])))
;;       (get-connection (assoc c :database (:database config))))
;;     (get-connection config)))

(defn default-connection []
  (cheshire.core/parse-string (slurp "connection.json") keyword))

(defn start [system & [opts]]
  (system/start-service
   system
   (let [connection (or opts (default-connection))
         _ (log/info system ::connecting (:database connection) (dissoc connection :password))
         db (get-pool connection)]
     (jdbc/execute! db ["select 1"])
     (log/info system ::connected (:database connection) (dissoc connection :password))
     {:datasource db :connection/info connection})))

(defn stop [system]
  (system/stop-service
   system
   (when-let [^HikariDataSource conn (system/get-system-state system [:datasoruce])]
     (.close conn))))

(meta #'log/manifest)

(system/defmanifest
  {:description "postgresql service"
   :deps [#'log/manifest]
   :config
   {:port      {:type "integer" :default 5432 :validator pos-int?}
    :host      {:type "string"  :required true}
    :database  {:type "string"  :required true}
    :password  {:type "string"  :sensitive true :required true}
    :pool-size {:type "integer" :default 5 :validator pos-int?}}})

(comment
  (def context (system/new-system {}))

  (def conn (cheshire.core/parse-string (slurp "connection.json") keyword))
  (start context conn)

  context

  (system/stop-system context)
  (stop context)

  (execute! context ["select 1"])
  (execute! context ["create table if not exists test  (resoruce jsonb)"])

  (dotimes [i 20]
    (copy context "copy test (resource) FROM STDIN csv quote e'\\x01' delimiter e'\\t'"
          (fn [w]
            (doseq [i (range 100)]
              (w (cheshire.core/generate-string {:a i}))))))

  (execute! context ["select count(*) from test"])
  (svs.logger/set-system-level context :error)
  (svs.logger/set-system-level context :debug)

  (svs.logger/get-system-level context)
  (svs.logger/error context ::test "msg")
  (svs.logger/info context ::test "msg")
  (svs.logger/debug context ::test "msg")

  context

  (execute! context ["truncate test"])

  (dotimes [i 100]
    (fetch context ["select resource from test"] 100 "resource" (fn [x i] (print "."))))


  (execute! context ["select count(*) from test"])
  (execute! context ["truncate test"])
  (copy-ndjson context "test" (fn [w] (doseq [i (range 100)] (w {:i i}))))

  )
