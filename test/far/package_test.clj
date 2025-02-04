(ns far.package-test
  (:require [system]
            [far.package]
            [far.package.repos]
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
    (reset! context-atom context))
  (system/clear-context-cache context [])
  (swap! (:cache context) dissoc :far.package.loader)
  )

(comment
  (do
    (system/stop-system context)
    (reset! context-atom nil))

  (ensure-context)

  (far.package/drop-tables context)
  (far.package.repos/migrate context)

  )

(t/deftest package-helpers

  (def pkgi (far.package/pkg-info context "hl7.fhir.r4.core"))

  (far.package/pkg-info context "hl7.fhir.uv.ips@2.0.0-ballot")

  (far.package/pkg-info context "hl7.fhir.us.mcode@4.0.0-ballot")


  (def pkg-bundle (far.package/package-bundle context pkgi))

  (def pkgi-r5 (far.package/pkg-info context "hl7.fhir.r5.core"))

  (def pkg-r5-bundle (far.package/package-bundle context pkgi-r5))

  (keys pkg-r5-bundle)
  (:package pkg-r5-bundle)

  (matcho/match
   (:package_version pkg-r5-bundle)
   {:dependencies {}})

  (into #{} (mapv :kind (:files (:index pkg-r5-bundle))))

  (:package_version pkg-bundle)

  (:package_dependency pkg-bundle)


  )

(t/deftest far-package-test

  (ensure-context)

  context

  (far.package/truncate context)

  (def pkgi (far.package/pkg-info context "hl7.fhir.us.mcode"))

  (far.package/pkg-info context "ups")

  ;; load-package "hl7.fhir.us.mcode" "4.0.0-ballot" -> return the package
  ;; package "name" & "version"

  (def test-pkg-name "hl7.fhir.us.mcode")
  (def test-pkg-version "4.0.0-ballot")

  (far.package/load-package context "hl7.fhir.uv.ips" "2.0.0-ballot")
  (far.package/load-package context "hl7.fhir.ca.baseline" "1.1.8")
  (far.package/load-package context "hl7.fhir.au.base" "4.1.0")

  (far.package/load-package context "us.nlm.vsac" "0.17.0")

  (far.package/pkg-info context "hl7.fhir.r4.core")
  (far.package/pkg-info context "hl7.terminology.r4")

  (pg.repo/select context {:table "CodeSystem" :limit 10})


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
  (time (far.package/canonical-deps context (first pts)))

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

  ;; (far.package/package-deps context pkv)

  ;; (far.package/package-deps-print context pkv)

  (def mcode (far.package/package-by-name context "hl7.fhir.us.mcode"))

  ;; (far.package/package-deps context mcode)

  ;; (far.package/package-deps-print context mcode)

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


  (def vs (pg.repo/read context {:table "valueset" :match {:id "04479aa4-c6e0-5dd2-827c-f2036a35fd66"}}))

  (matcho/match
   (pg.repo/select context {:table "canonical_deps" :match {:definition_id (:id vs)}})
   [{:package_name "hl7.fhir.r5.core",
     :definition "http://hl7.org/fhir/ValueSet/bundle-type",
     :dep_package_id #uuid "de285a0f-b21c-5227-82be-946deea4d3d5",
     :dep_id #uuid "81ad69ed-11e6-529f-ba63-418d0917c634",
     :type "vs/include-cs",
     :package_version "5.0.0",
     :definition_version "5.0.0",
     :resource_type "CodeSystem",
     :status "resolved",
     :url "http://hl7.org/fhir/bundle-type",
     :package_id #uuid "de285a0f-b21c-5227-82be-946deea4d3d5",
     :definition_id (:id vs)}])



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

  ;; Meta core

  far.package.repos/canonicals

  (def meta-core
    (pg/execute! context {:dsql {:select {:id :id :url :url :name [:resource#>> [:name]]}
                                 :from :structuredefinition
                                 :where [:and
                                         [:in [:pg/sql "lower(resource#>>'{name}')"] [:pg/params-list far.package.repos/core-canonicals]]
                                         [:= :package_id "de285a0f-b21c-5227-82be-946deea4d3d5"]]}}))


  (def meta-core-deps
    (->> (pg/execute! context {:dsql {:select [:pg/sql "distinct url, type, dep_id, dep_package_id"]
                                      :from :canonical_deps
                                      :order-by :url
                                      :where [:and [:in :definition_id [:pg/params-list (mapv :id meta-core)]]
                                              [:<> :type "binding"]
                                              ]}})))



  (->> meta-core-deps (mapv :url))

  (def meta-core-deps-deps
    (->> (pg/execute! context {:dsql {:select [:pg/sql "distinct url, type, dep_id, dep_package_id"]
                                      :from :canonical_deps
                                      :order-by :url
                                      :where [:and
                                              [:in :definition_id [:pg/params-list (mapv :dep_id meta-core-deps)]]
                                              [:not [:in :definition_id [:pg/params-list (mapv :dep_id meta-core)]]]
                                              [:<> :type "binding"]]}})))

  (->> meta-core
       (mapv (fn [x] [(:id x) (:url x)])))

  (let [pkg-id "454f267d-ea0f-5a07-927c-09a7b56ff302"
        pkg-id "de285a0f-b21c-5227-82be-946deea4d3d5"]
    (->> (pg/execute! context {:dsql {:select [:pg/sql "distinct type, url"]
                                      :from :canonical_deps
                                      :order-by [:pg/list :type :url]
                                      :where [:and
                                              [:or [:not [:= :dep_package_id pkg-id]]
                                               [:not [:= :local true]]
                                               [:is :dep_package_id nil]]
                                              [:= :package_id pkg-id]]}})
         (mapv (fn [x] (str/join " " [(:type x) (:url x)])))))


  (def root-cn
    ["http://hl7.org/fhir/StructureDefinition/StructureDefinition"
     "http://hl7.org/fhir/StructureDefinition/ValueSet"
     "http://hl7.org/fhir/StructureDefinition/ElementDefinition"
     "http://hl7.org/fhir/StructureDefinition/CodeSystem"
     ])

  (def first-layer
    (->> (pg.repo/select context {:table "canonical_deps"
                                  :where [:and
                                          [:in :definition [:pg/params-list root-cn]]
                                          [:<> :type "reference"]
                                          [:or [:is :binding_strength nil] [:<> :binding_strength "example"]]
                                          [:= :package_id "d58c1dcd-a628-50ac-94de-757eee16627e"]]})
         (mapv :url)
         (into #{})))

  (sort (concat root-cn first-layer))

  (def second-layer
    (->> (pg.repo/select context {:table "canonical_deps"
                                  :where [:and
                                          [:in :definition [:pg/params-list first-layer]]
                                          [:<> :type "reference"]
                                          [:or [:is :binding_strength nil] [:<> :binding_strength "example"]]
                                          [:not [:in :definition [:pg/params-list root-cn]]]
                                          [:= :package_id "d58c1dcd-a628-50ac-94de-757eee16627e"]]})
         (mapv :url)
         (into #{})))

  (def third-layer
    (->> (pg.repo/select context {:table "canonical_deps"
                                  :where [:and
                                          [:in :definition [:pg/params-list second-layer]]
                                          [:not [:in :definition [:pg/params-list (concat root-cn first-layer)]]]
                                          [:or [:is :binding_strength nil] [:<> :binding_strength "example"]]
                                          [:<> :type "reference"]
                                          [:= :package_id "d58c1dcd-a628-50ac-94de-757eee16627e"]]})
         (mapv :url)
         (into #{})))

  (def fifth-layer
    (->> (pg.repo/select context {:table "canonical_deps"
                                  :where [:and
                                          [:in :definition [:pg/params-list third-layer]]
                                          [:not [:in :definition [:pg/params-list (concat root-cn first-layer second-layer)]]]
                                          [:<> :type "reference"]
                                          [:= :package_id "d58c1dcd-a628-50ac-94de-757eee16627e"]]})
         (mapv :url)
         (into #{})))

  (def six-layer
    (->> (pg.repo/select context {:table "canonical_deps"
                                  :where [:and
                                          [:in :definition [:pg/params-list fifth-layer]]
                                          [:not [:in :definition [:pg/params-list (concat root-cn first-layer second-layer third-layer)]]]
                                          [:<> :type "reference"]
                                          [:= :package_id "d58c1dcd-a628-50ac-94de-757eee16627e"]]})
         (mapv :url)))

  (def seven-layer
    (->> (pg.repo/select context {:table "canonical_deps"
                                  :where [:and
                                          [:in :definition [:pg/params-list six-layer]]
                                          [:not [:in :definition [:pg/params-list (concat root-cn first-layer second-layer third-layer
                                                                                          fifth-layer
                                                                                          )]]]
                                          [:<> :type "reference"]
                                          [:= :package_id "d58c1dcd-a628-50ac-94de-757eee16627e"]]})
         (mapv :url)))

  (->> 
   (concat root-cn first-layer second-layer third-layer fifth-layer six-layer)
   (into #{})
   sort)

  (->> (pg.repo/select context {:table "canonical_deps"
                                :where [:and
                                        [:in :definition [:pg/params-list six-layer]]
                                        [:not [:in :definition [:pg/params-list (concat root-cn first-layer second-layer third-layer
                                                                                        fifth-layer
                                                                                        )]]]
                                        [:<> :type "reference"]
                                        [:= :package_id "d58c1dcd-a628-50ac-94de-757eee16627e"]]})
       (mapv :url))

  (count first-layer)
  (count second-layer)
  (count third-layer)


  (pg/execute! context {:dsql {:select [:pg/sql "id, url"]
                               :from :canonical
                               :where [:= :package_id
                                       "d58c1dcd-a628-50ac-94de-757eee16627e"
                                       ]}})

  (pg/execute! context {:dsql {:select :*
                               :from :package_dependency
                               :where [:= :package_id "454f267d-ea0f-5a07-927c-09a7b56ff302"]}})

  ;; dep

  )





(comment

  (def pkg-r5-bundle (far.package/package-bundle context pkgi-r5))

  (keys pkg-r5-bundle)

  (->> (get pkg-r5-bundle "structuredefinition")
       (keys)
       (sort)
       )

  (far.package/pkg-info context "hl7.fhir.r4.core")

  (def pt-sd (get-in pkg-r5-bundle ["structuredefinition" "http://hl7.org/fhir/StructureDefinition/Patient"]))

  (:version pt-sd)

  (fhir.schema.transpiler/translate (get-in pkg-r5-bundle ["structuredefinition" "http://hl7.org/fhir/StructureDefinition/Patient"]))



  )

