(ns system)
;; TODO: rewrite start with context

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

(defmacro get-state-from-ctx [ctx]
  `(-get-state (get ~ctx :system) ~(keyword (.getName *ns*))))

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

(defn stop-services [ctx]
  (doseq [sv (:services @(:system ctx))]
    (require [sv])
    (when-let [stop-fn (resolve (symbol (name sv) "stop"))]
      (println :stop stop-fn)
      (println :> (stop-fn ctx)))))

(defn ctx-get [ctx & path]
  (get-in ctx path))

(comment
  (def system (new-system {}))
  system

  (set-system-state system [:connection] :ok)
  (get-system-state system [])

  (def ctx (new-context system))

  (get-state-from-ctx ctx)

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

  (stop-services system)



  )
