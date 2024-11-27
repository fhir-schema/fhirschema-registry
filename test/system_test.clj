(ns system-test
  (:require [clojure.test :refer [deftest testing is]]
            [matcho.core :as matcho]
            [system]))

(system/defmanifest
  {:config {:param {:required true :type "string"}}}
  )

(deftest basic-test

  (def sys (system/new-system {}))

  sys

  (system/set-system-state sys [:var] 1)

  (matcho/match (system/get-system-state sys [:var]) 1)

  (system/clear-system-state sys [:var])

  (is (nil? (system/get-system-state sys [:var])))


  )


(comment

  (s/explain-str ::manifest {:config {:param {:type "string"}}})

  (s/explain-data ::manifest )

  (system/defmanifest
    {:config {:param {:required true :type "string"}}}
    )

  (system/defmanifest
    {:config {:ups {:required 1 :type "ups"}}}
    )

  )
