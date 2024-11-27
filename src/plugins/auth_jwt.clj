(ns plugins.auth-jwt
  (:require
   [system]
   [http :as http]
   [logger :as log]))

(defn authorize [ctx req]
  (log/info ctx ::auth (pr-str (keys req)))
  ctx)

(system/defmanifest
  {:description "provide jwt interceptor"})

(system/defstart [context config]
  (http/register-middleware context #'authorize)
  {})

(system/defstop [context state])


