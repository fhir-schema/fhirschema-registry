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


  pkgi

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

  )

(defn get-canonical [context match]
  (system/get-context-cache
   context [:canonical match]
   (fn [] (pg.repo/select context {:table "canonical" :match match}))))

(defn get-deps-idx [context package_id]
  (let [pid (if (uuid? package_id) package_id (parse-uuid package_id))]
    (->> (pg.repo/read context {:table "package_version" :match {:id package_id}})
         (:all_dependencies)
         (reduce (fn [acc {id :id :as p}]
                   (assoc acc (parse-uuid id) p))
                 {pid {:level -1}}))))

(defn get-pkg-deps [context dep]
  (system/get-context-cache
   context [:pkgs (:package_id dep)]
   (fn [] (get-deps-idx context (:package_id dep)))))

(defn resolve-dep [context dep]
  (let [idx        (get-pkg-deps context dep)
        rdep  (->> (get-canonical context (if-let [v (:version dep)] {:url  (:url dep) :version v} {:url (:url dep)}))
                   (mapv (fn [r] (assoc r :level (:level (get idx (:package_id r))))))
                   (filter :level)
                   (sort-by :level)
                   (first))]
    (if rdep
      (assoc dep :dep_id (:id rdep) :dep_package_id (:package_id rdep) :status "resolved")
      (assoc dep :status "error"))))


(comment

  (system/stop-system context)

  (pg.repo/select context {:table "structuredefinition"
                           :match {:url "http://hl7.org/fhir/StructureDefinition/Patient"}
                           :limit 10})

  (pg.repo/select context {:table "package_dependency" :limit 10})

  (pg.repo/select context {:table "structuredefinition_element" :where [:pg/sql "resource->'binding' is not null"] :limit 100})


  (def canonical_deps "canonical_deps")

  (pg.repo/drop-repo context {:table canonical_deps})

  (pg.repo/register-repo context
   {:table  canonical_deps
    :primary-key [:defnition_id :type :url]
    :columns {:package_id {:type "uuid"}
              :defnition_id {:type "uuid"}
              :definition {:type "text"}
              :type {:type "text"}
              :package_name {:type "text"}
              :package_version {:type "text"}
              :definition_version {:type "text"}
              :url {:type "text"}
              :version {:type "text"}
              :status {:type "text"}
              :dep_id {:type "uuid"}
              :dep_package_id {:type "uuid"}}})


  (def dep
    {:package_name "hl7.fhir.us.mcode"
     :definition "http://hl7.org/fhir/us/mcode/StructureDefinition/mcode-human-specimen"
     :type :reference
     :package_version "4.0.0-ballot"
     :definition_version "4.0.0-ballot"
     :url "http://hl7.org/fhir/us/mcode/StructureDefinition/mcode-cancer-patient"
     :package_id "454f267d-ea0f-5a07-927c-09a7b56ff302"
     :defnition_id "a474135b-0c85-5713-a085-4989fc333d9a"})


  (def idx (get-deps-idx context (:package_id dep)))





  (system/ctx-get-log-level context)

  (system/info context ::test)

  ;; (count @(::cache context))

  (def context (system/new-context context))
  (def context (system/ctx-set-log-level context :error))
  (def context (system/ctx-set-log-level context :info))
  (time
   (do
     (pg.repo/truncate context {:table canonical_deps})
     (println :vs)
     (time
      (pg.repo/load
       context {:table canonical_deps}
       (fn [insert]
         (pg.repo/fetch
          context {:table "valueset"}
          (fn [sd]
            (doseq [d  (extract-deps sd)]
              (insert (resolve-dep context d))))))))

     (println :loadsd)
     (time
      (pg.repo/load
       context {:table canonical_deps}
       (fn [insert]
         (pg.repo/fetch
          context {:table "structuredefinition"}
          (fn [sd]
            (doseq [d  (extract-deps sd)]
              (insert (resolve-dep context d))))))))))

  (pg.repo/select context {:table canonical_deps :limit 100 })

  (pg/execute! context {:sql "select status, count(*) from canonical_deps group by 1"})

  (pg.repo/select context {:table canonical_deps :match {:status "error"} :limit 10})

  (pg.repo/select context {:table "canonical"
                           :match {:url   "http://nucc.org/provider-taxonomy"}})

  (pg.repo/select context {:table "package_version"
                           :match {:id #uuid "3f1e3f6c-0154-50d2-8de8-34bdeb77dcfa"}})

  (pg.repo/select context {:table canonical_deps
                           :match {:status "error"}
                           :limit 100})

  (pg.repo/select context {:table "canonical"
                           :match {:resource_type "CodeSystem"
                                   :package_id #uuid "d58c1dcd-a628-50ac-94de-757eee16627e"}})


  (->> (pg/execute! context {:sql "select * from canonical_deps where status = 'error' limit 10"})
       (count))

  (pg.repo/select context {:table "canonical"
                           :match {:url "http://terminology.hl7.org/ValueSet/v2-0116"}})

  (pg.repo/select context {:table canonical_deps
                           :match {:status "error"}
                           :select [:package_name :package_version :definition :url]
                           :limit 100 })


  (->> (pg.repo/read context {:table "package_version" :match {:id #uuid "94f17cb3-0d87-53b1-a85d-9988ac9844a2"}})
       (:all_dependencies))

  (pg.repo/select context {:table "package_version"})
  (pg.repo/select context {:table "canonical" :limit 10})

  (->> (pg.repo/select context {:table "structuredefinition"
                                :match
                                {:package_name "hl7.fhir.r4.core",
                                 :url "http://hl7.org/fhir/StructureDefinition/Group"}})
       (mapcat extract-deps))

  (->> (pg.repo/select context {:table "valueset"
                                :limit 100
                                :match {:package_name "hl7.fhir.r4.core"}})
       (mapcat extract-deps))

  (pg.repo/select context {:table canonical_deps :limit 10})

  (pg.repo/select context {:table "canonical" :match {:url "http://hl7.org/fhir/StructureDefinition/Period"
                                                      :package_id #uuid "d58c1dcd-a628-50ac-94de-757eee16627e"}})


  (def dep
    {:package_name "hl7.fhir.r4.core",
     :definition "http://hl7.org/fhir/StructureDefinition/patient-nationality",
     :dep_package_id nil,
     :dep_id nil,
     :type ":type",
     :package_version "4.0.1",
     :definition_version "4.0.1",
     :status "error",
     :url "http://hl7.org/fhir/StructureDefinition/CodeableConcept",
     :package_id #uuid "d58c1dcd-a628-50ac-94de-757eee16627e",
     :defnition_id #uuid "04ac822e-eb42-5ede-91b9-dd4f540a1a0a"})

  (def idx (get-deps-idx context "d58c1dcd-a628-50ac-94de-757eee16627e"))

  (resolve-dep context dep idx)

  ;; TODO process discriminator profiles
  ;; TODO make table
  ;; TODO impl pg.repo/fetch
  ;; TODO process and load deps
  ;; TODO we may resolve url into :id
  ;; TODO impl for VS

  ;; TODO: (canonical all-deps)
  ;; TODO: visualize deps
  )







