(ns system-test
  (:require [clojure.test :refer [deftest testing is]]
            [matcho.core :as matcho]
            [system]))

(deftest basic-test
  (def sys (system/new-system {}))

  sys

  (system/set-system-state sys [:var] 1)

  (matcho/match (system/get-system-state sys [:var]) 1)

  (system/clear-system-state sys [:var])

  (is (nil? (system/get-system-state sys [:var])))


  )
