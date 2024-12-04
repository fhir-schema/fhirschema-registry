(ns far.package-test
  (:require [system]
            [far.package]
            [pg]
            [pg.repo]
            [clojure.test :as t]
            [cheshire.core]
            [clojure.string :as str]
            [matcho.core :as matcho]))

(defonce context-atom (atom nil))

(def cfg (cheshire.core/parse-string (slurp "connection.json") keyword))

(defn ensure-context []
  (when-not @context-atom
    (println :connect)
    (def context (system/start-system {:services ["pg" "pg.repo" "far.package"] :pg cfg}))
    (reset! context-atom context)))

(t/deftest far-package-test
  (ensure-context)

  (far.package/truncate context)

  (def pkgi (far.package/pkg-info context "hl7.fhir.us.mcode"))

  pkgi

  (def pkg-bundle (far.package/read-package pkgi))

  (:package_version pkg-bundle)

  (:package_dependency pkg-bundle)

  (def new-packages (far.package/load-package context (:name pkgi) (:version pkgi)))


  (matcho/match new-packages #(not (empty? %)))


  (t/is (seq (pg/execute! context {:sql "
select name, version,
  resource->'dependencies' as d,
 resource->'all_dependencies' as ad
from package_version
  where resource->'dependencies' <> resource->'all_dependencies'"})))

  (def pkv (pg.repo/read context {:table "package_version" :match {:name "hl7.fhir.us.core"}}))

  pkv

  (far.package/resolve-all-deps context pkv)

  (far.package/print-deps-tree
   (far.package/deps-tree context pkv))

  (def mcode (pg.repo/read context {:table "package_version" :match {:name "hl7.fhir.us.mcode"}}))

  (far.package/deps-tree context pkv)

  )

(comment

  (system/stop-system context)

  (pg.repo/select context {:table "structuredefinition" :limit 10})
  (pg.repo/select context {:table "structuredefinition_element" :limit 10})



  )
