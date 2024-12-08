(ns far.package-test
  (:require [system]
            [far.package]
            [pg]
            [pg.repo]
            [clojure.test :as t]
            [cheshire.core]
            [clojure.string :as str]
            [far.package.canonical-deps :refer [extract-deps]]
            [matcho.core :as matcho]))

(defonce context-atom (atom nil))

(def cfg (cheshire.core/parse-string (slurp "connection.json") keyword))

(defn ensure-context []
  (when-not @context-atom
    (println :connect)
    (def context (system/start-system {:services ["pg" "pg.repo" "far.package"] :pg cfg}))
    (reset! context-atom context)))

(comment
  (do
    (system/stop-system context)
    (reset! context-atom nil))

  (ensure-context)

  (far.package/drop-tables context)

  )

(t/deftest far-package-test

  (ensure-context)

  context

  (far.package/truncate context)

  (def pkgi (far.package/pkg-info context "hl7.fhir.r4.core"))

  (def pkg-bundle (far.package/read-package pkgi))


  (def pkgi-r5 (far.package/pkg-info context "hl7.fhir.r5.core"))

  (def pkg-r5-bundle (far.package/read-package pkgi-r5))

  (into #{} (mapv :kind (:files (:index pkg-r5-bundle))))

  (:package_version pkg-bundle)

  (:package_dependency pkg-bundle)

  (def pkgi (far.package/pkg-info context "hl7.fhir.us.mcode"))

  (def new-packages (far.package/load-package context (:name pkgi) (:version pkgi)))


  (pg.repo/select context {:table  "canonical"
                           :match { :resource_type "StructureDefinition"} 
                           :limit 10})

  (pg.repo/select context {:table  "canonical"
                           :match { :resource_type "ValueSet"} 
                           :limit 10})

  (pg.repo/select context {:table  "canonical"
                           :match { :resource_type "CodeSystem"} 
                           :limit 10})


  (pg.repo/select context {:table  "canonical"
                           :match {:package_id #uuid "d58c1dcd-a628-50ac-94de-757eee16627e"} 
                           :select [:url]
                           :order-by [:pg/asc :url]
                           :limit 1000})


  (matcho/match new-packages #(not (empty? %)))

  (t/is (seq (pg/execute! context {:sql "
select name, version,
  resource->'dependencies' as d,
 resource->'all_dependencies' as ad
from package_version
  where resource->'dependencies' <> resource->'all_dependencies'"})))

  (def pkv (pg.repo/read context {:table "package_version" :match {:name "hl7.fhir.us.core"}}))

  pkv

  (time (far.package/resolve-all-deps context pkv))

  (far.package/print-deps-tree
   (far.package/deps-tree context pkv))

  (def mcode (pg.repo/read context {:table "package_version" :match {:name "hl7.fhir.us.mcode"}}))

  (far.package/resolve-all-deps context mcode)

  (far.package/print-deps-tree
   (far.package/deps-tree context mcode))

  (pg.repo/select context {:table  "structuredefinition" :limit 10})
  (pg/execute! context {:sql "select url, id, package_id from structuredefinition limit 10"})

  ;; same url
  (pg/execute! context {:sql "
   select
     v.name,
     v.version,
     row_to_json(c1.*) as c1,
     row_to_json(c2.*) as c2
from canonical c1, canonical c2, package_version v
where
 c1.id = c2.id
 and c1.resource_type <> c2.resource_type
 and v.id  = c1.package_id
limit 10
   "})


  (pg/execute! context {:sql "select status, count(*) from canonical_deps group by 1"})
  (pg/execute! context {:sql "select type, count(*) from canonical_deps group by 1"})

  (pg.repo/select context {:table "canonical_deps" :match {:status "error"} :limit 10})


  (matcho/match
   (pg.repo/select context {:table "canonical_deps" :limit 10
                            :order-by :url
                            :match {:package_name "hl7.fhir.r4.core"}})
   [{:package_name "hl7.fhir.r4.core",
     :definition "http://hl7.org/fhir/ValueSet/cosmic",
     :url "http://cancer.sanger.ac.uk/cancergenome/projects/cosmic",
     :package_id #uuid "d58c1dcd-a628-50ac-94de-757eee16627e",
     :defnition_id #uuid "f208c752-f76d-525c-99bb-ceb7f7448d8a"}])


  )



(comment
  ;; TODO process discriminator profiles
  ;; TODO make table
  ;; TODO impl pg.repo/fetch
  ;; TODO process and load deps
  ;; TODO we may resolve url into :id
  ;; TODO impl for VS

  ;; TODO: (canonical all-deps)
  ;; TODO: visualize deps
  )







