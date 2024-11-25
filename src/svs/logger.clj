(ns svs.logger
  (:require [system]))

(defn start [system config]
  (system/start-service system config))

(defn stop [system])

(def levels
  {:debug 0 :info 1 :error 2})

(defn set-level [ctx lvl])

(defn get-level [ctx])

(defn error [ctx msg & [params]]
  )
(defn info [ctx msg & [params]]
  )
(defn debug [ctx msg & [params]]
  )

