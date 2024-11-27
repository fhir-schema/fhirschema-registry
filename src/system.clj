(ns system
  (:require [system.config]
            [clojure.spec.alpha :as s]))
;; TODO: rewrite start with context

(s/def ::config :system.config/config-spec)
(s/def ::descripton string?)
(s/def ::manifest (s/keys :opt-un [::config ::description]))

(defmacro defmanifest [manifest]
  (when-not (s/valid? ::manifest manifest)
    (throw (ex-info "Invalid manifest" (s/explain-data ::manifest manifest))))
  (list 'def 'manifest manifest))

(defn new-system [ & [config]]
  {:system (atom {:system/config (or config {})})})

(defn new-context [ctx & [params]]
  (merge (or params {}) {:system (:system ctx)}))

(defn -set-state [system key value]
  (swap! system assoc key value))

(defmacro set-state [system value]
  `(-set-state ~system ~(keyword (.getName *ns*)) ~value))

(defn -clear-state [system key]
  (swap! system dissoc key))

(defmacro clear-state [system]
  `(-clear-state ~system ~(keyword (.getName *ns*))))

(defn -get-state [system key]
  (get @system key))

(defmacro get-state [system]
  `(-get-state ~system ~(keyword (.getName *ns*))))

(defn -set-system-state [system key path value]
  (swap! system assoc-in (into [key] path) value))

(defmacro set-system-state [ctx path value]
  `(-set-system-state (:system ~ctx) ~(keyword (.getName *ns*)) ~path ~value))

(defn -clear-system-state [system key & [path]]
  (if (or (nil? path) (empty? path))
    (swap! system dissoc key)
    (swap! system (fn [x] (update-in x (into [key] (butlast path)) dissoc (last path))))))

(defmacro clear-system-state [ctx path]
  `(-clear-system-state (:system ~ctx) ~(keyword (.getName *ns*)) ~path))

(defn -update-system-state [system key path f]
  (swap! system update-in (into [key] path) f))

(defmacro update-system-state [ctx path f]
  `(-update-system-state (:system ~ctx) ~(keyword (.getName *ns*)) ~path ~f))

(defn -get-system-state [system key path default]
  (get-in @system (into [key] path) default))

(defmacro get-system-state [ctx path & [default]]
  `(-get-system-state (:system ~ctx) ~(keyword (.getName *ns*)) ~path ~default))

(defmacro start-service [ctx & body]
  (let [key (.getName *ns*)]
    `(when-not (contains? (:services @(:system ~ctx)) '~key)
       (swap! (:system ~ctx) update :services (fn [x#] (conj (or x# #{}) '~key)))
       (set-system-state ~ctx [] (do ~@body)))))

(defmacro stop-service [ctx & body]
  (let [key (.getName *ns*)]
    `(when (contains? (:services @(:system ~ctx)) '~key)
       ~@body
       (swap! (:system ~ctx) update :services (fn [x#] (when x# (disj x# '~key))))
       (clear-system-state ~ctx []))))

(defn ctx-get [ctx & path]
  (get-in ctx path))


(defn manifest-hook [ctx hook-name opts]
  (update-system-state ctx [:manifested-hooks hook-name] opts))

(defn register-hook [ctx hook-name hook-handler & [opts]]
  ;; TODO: check hooks availability
  (update-system-state ctx [:hooks hook-name] (fn [x] (assoc (or x {}) hook-handler (or opts {})))))

(defn get-hooks [ctx hook-name]
  (->> (get-system-state ctx [:hooks hook-name])
       (mapv (fn [[h opts]] (assoc opts :fn h)))))

(defn -register-config [ctx service-name config]
  (update-system-state ctx [:config service-name] config))

(defmacro register-config [ctx config]
  (let [key (keyword (.getName *ns*))]
    `(-register-config ~ctx ~key ~config)))


(defn start-system [{services :services :as config}]
  (let [system (new-system {})]
    (doseq [svs services]
      (require (symbol svs))
      (when-let [manifest (resolve (symbol (name svs) "manifest"))]
        (println :MANIFEST svs (var-get manifest)))
      (when-let [start-fn (resolve (symbol (name svs) "start"))]
        (start-fn system (get config (keyword svs) {}))))
    system))

(defn stop-system [ctx]
  (doseq [sv (:services @(:system ctx))]
    (require [sv])
    (when-let [stop-fn (resolve (symbol (name sv) "stop"))]
      (println :stop stop-fn)
      (println :> (stop-fn ctx)))))

;; TODO: get rid of start-service and stop-service
;; TODO: make register module using manifest
;; on module registration it register all config params
;; this params are used to validate before start

(comment
  (require ['svs.pg])
  (require ['svs.ups])

  (def system (new-system {}))
  system

  (set-system-state system [:connection] :ok)
  (get-system-state system [])

  (def ctx (new-context system))

  (set-system-state ctx [] {:a 1})

  (set-system-state ctx [:uri] "uri")
  (get-system-state ctx [:uri])
  (get-system-state ctx [:a])

  (clear-system-state ctx [:uri])
  (clear-system-state ctx [])

  ctx

  (start-service
   system
   (println :ok))

  (stop-system system)

  (def pg-system
    (start-system
     {:services ["svs.pg"]
      :svs.pg (cheshire.core/parse-string (slurp "connection.json") keyword)}))

  (stop-system pg-system)

  (svs.pg/execute! pg-system ["select 1"])

  )
