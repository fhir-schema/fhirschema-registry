(ns svs.logger
  (:require [system]))

(def default-config {:level :info})

(def levels
  {:debug 0 :info 1 :error 2})

(defn start [system config]
  (system/start-service system (update (merge default-config config) :level #(get levels %))))

(defn stop [system]
  )

(defn set-system-level [ctx lvl]
  (system/set-system-state ctx [:level] (get levels lvl)))

(defn get-system-level [ctx]
  (get levels (system/get-system-state ctx [:level])))

(defn error [ctx ev msg & [params]]
  (when (<= (system/get-system-state ctx [:level] 1) 2)
    (println :error (merge params {:event.name ev :message msg :level "ERROR" :timestamp (java.util.Date.)}))))

(defn info [ctx ev msg & [params]]
  (when (<= (system/get-system-state ctx [:level] 1) 1)
    (println :info (merge params {:event.name ev :message msg :level "INFO" :timestamp (java.util.Date.)}))))

(defn debug [ctx ev msg & [params]]
  (when (<= (system/get-system-state ctx [:level] 1) 0)
    (println :debug (merge params {:event.name ev :message msg :level "DEBUG" :timestamp (java.util.Date.)}))))


