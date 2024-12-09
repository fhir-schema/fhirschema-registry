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

(comment
  (do
    (system/stop-system context)
    (reset! context-atom nil))

  (ensure-context)

  (far.package/drop-tables context)

  )

(t/deftest package-helpers

  (def pkgi (far.package/pkg-info context "hl7.fhir.r4.core"))

  (def pkg-bundle (far.package/package-bundle pkgi))

  (def pkgi-r5 (far.package/pkg-info context "hl7.fhir.r5.core"))

  (def pkg-r5-bundle (far.package/package-bundle pkgi-r5))

  (into #{} (mapv :kind (:files (:index pkg-r5-bundle))))

  (:package_version pkg-bundle)

  (:package_dependency pkg-bundle)


  )

(t/deftest far-package-test

  (ensure-context)

  context

  (far.package/truncate context)

  (def pkgi (far.package/pkg-info context "hl7.fhir.us.mcode"))

  ;; load-package "hl7.fhir.us.mcode" "4.0.0-ballot" -> return the package
  ;; package "name" & "version"

  (def test-pkg-name "hl7.fhir.us.mcode")
  (def test-pkg-version "4.0.0-ballot")

  ;; TODO: add check that package exists
  (def new-packages (far.package/load-package context test-pkg-name test-pkg-version))

  (matcho/match new-packages #(not (empty? %)))

  (matcho/match
   (far.package/packages context {:select [:id :name :version]})
   [{:name "hl7.fhir.r4.core", :version "4.0.1"}
    {:name "hl7.fhir.r4.examples", :version "4.0.1"}
    {:name "hl7.fhir.r5.core", :version "5.0.0"}
    {:name "hl7.fhir.us.core", :version "5.0.1"}
    {:name "hl7.fhir.us.mcode", :version "4.0.0-ballot"}
    {:name "hl7.fhir.uv.bulkdata", :version "2.0.0"}
    {:name "hl7.fhir.uv.extensions", :version "5.1.0"}
    {:name "hl7.fhir.uv.genomics-reporting", :version "2.0.0"}
    {:name "hl7.fhir.uv.sdc", :version "3.0.0"}
    {:name "hl7.fhir.uv.smart-app-launch", :version "2.0.0"}
    {:name "hl7.terminology.r4", :version "3.1.0"}
    {:name "hl7.terminology.r4", :version "5.3.0"}
    {:name "hl7.terminology.r5", :version "5.5.0"}
    {:name "us.nlm.vsac", :version "0.7.0"}])

  (matcho/match
   (far.package/package context "hl7.fhir.us.mcode" "4.0.0-ballot")
   {:name "hl7.fhir.us.mcode"
    :version "4.0.0-ballot"
    :all_dependencies seq})

  (far.package/canonicals context {:match {:resource_type "StructureDefinition"} :limit 10})

  (ensure-context)

  (pg.repo/select context {:table "canonical_deps"  :limit 10})

  (def pts (pg/execute! context {:sql "select id,url,package_name from canonical where url = 'http://hl7.org/fhir/StructureDefinition/Patient' limit 10"}))


  pts

  (def pt-deps (time (far.package/canonical-deps context (first pts) 3)))


  (far.package/canonicals context {:match {:resource_type "ValueSet"} :limit 10})
  (far.package/canonicals context {:match {:resource_type "CodeSystem"} :limit 10})
  (far.package/canonicals context {:match {:package_id #uuid "d58c1dcd-a628-50ac-94de-757eee16627e"}
                                   :select [:url] :limit 10})

  (t/is (seq (pg/execute! context {:sql "
select name, version,
  resource->'dependencies' as d,
 resource->'all_dependencies' as ad
from package_version
  where resource->'dependencies' <> resource->'all_dependencies'"})))

  (def pkv (pg.repo/read context {:table "package_version" :match {:name "hl7.fhir.us.core"}}))

  (pg.repo/read context {:table "package" :match {:name "hl7.fhir.us.core"}})

  (far.package/package-deps context pkv)

  (far.package/package-deps-print context pkv)

  (def mcode (far.package/package-by-name context "hl7.fhir.us.mcode"))

  (far.package/package-deps context mcode)

  (far.package/package-deps-print context mcode)

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
     :definition_id #uuid "f208c752-f76d-525c-99bb-ceb7f7448d8a"}])


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







