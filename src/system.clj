(ns system)

(defn new-system [ & [config]]
  (atom {:system/config (or config {})}))

(defn new-context [system & [params]]
  (merge (or params {}) {:system system}))

(defn -set-state [system key value]
  (swap! system assoc key value))

(defmacro set-state [system value]
  `(-set-state ~system ~(keyword (.getName *ns*)) ~value))

(defn -get-state [system key]
  (println :get-state key)
  (get @system key))

(defmacro get-state [system]
  `(-get-state ~system ~(keyword (.getName *ns*))))

(defmacro get-state-from-ctx [ctx]
  `(-get-state (get ~ctx :system) ~(keyword (.getName *ns*))))


(defmacro start-service [system & body]
  (let [key (.getName *ns*)]
    `(when-not (contains? (:services @~system) '~key)
       (swap! ~system update :services (fn [x#] (conj (or x# #{}) '~key)))
       (set-state ~system (do ~@body)))))

(defn stop-services [system]
  (doseq [sv (:services @system)]
    (require [sv])
    (when-let [stop-fn (resolve (symbol (name sv) "stop"))]
      (println :stop stop-fn)
      (println :> (stop-fn system)))))

(comment
  (def system (new-system {}))
  system

  (set-state system {:connection :ok})
  (get-state system)

  (def ctx (new-context system))

  (get-state-from-ctx ctx)

  (start-service
   system
   (println :ok))

  (stop-services system)



  )
