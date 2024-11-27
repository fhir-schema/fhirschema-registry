(ns svs.settings
  (:require [system]
            [svs.pg]))

(system/defmanifest
  {:description "manage dynamic settings"})

(system/defstart [context config])

(system/defstop [context state])
