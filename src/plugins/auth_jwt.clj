(ns plugins.auth-jwt
  (:require
   [system]
   [svs.http :as http]
   [svs.logger :as log]))

(defn authorize [ctx req]
  (log/info ctx ::auth (pr-str (keys req)))
  ctx)

(defn start [ctx & [config]]
  (system/start-service ctx
   (http/register-middleware ctx #'authorize)))

(defn stop [ctx & [config]])


