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

(t/deftest far-package-test
  (comment
    (far.package/drop-tables context)
    )
  (ensure-context)

  context

  (far.package/truncate context)

  (def pkgi (far.package/pkg-info context "hl7.fhir.us.mcode"))

  pkgi

  (def pkg-bundle (far.package/read-package pkgi))


  (def pkgi-r5 (far.package/pkg-info context "hl7.fhir.r5.core"))

  (def pkg-r5-bundle (far.package/read-package pkgi-r5))

  (into #{} (mapv :kind (:files (:index pkg-r5-bundle))))

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

  (time (far.package/resolve-all-deps context pkv))

  (far.package/print-deps-tree
   (far.package/deps-tree context pkv))

  (def mcode (pg.repo/read context {:table "package_version" :match {:name "hl7.fhir.us.mcode"}}))

  (far.package/deps-tree context pkv)

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

(defmulti extract-deps (fn [{rt :resourceType}] rt))

(defn deps-from-el-target-profile [el tp]
  (->> (:targetProfile tp)
       (map (fn [trgtpr]
              {:type :reference :path (:id el) :url trgtpr}))))

(defn deps-from-el-profile [el tp]
  (when (:profile tp)
    (concat (->> (:profile tp)
                 (map (fn [pr] {:type :type-profile :path (:id el) :url pr}))))))

(defn deps-from-type-code [el tp]
  [{:type :type :path (:id el)
    :url (if (str/starts-with?  (:code tp) "http")
           (:code tp)
           (str "http://hl7.org/fhir/StructureDefinition/" (:code tp)))}])

(defn deps-from-type [el]
  (->> (:type el)
       (mapcat (fn [tp]
                 (concat
                  (deps-from-type-code el tp)
                  (deps-from-el-target-profile el tp)
                  (deps-from-el-profile el tp))))))

(defn deps-from-additional-bindings [el]
  (->> (get-in el [:binding :additional])
       (mapv (fn [b]
               {:type :additional-binding  :path (:id el) :url (:valueSet b)}))))

(defn deps-from-binding [el]
  (when (and (:binding el) (not (= "example" (get-in el [:binding :strength]))))
    (concat [{:type :binding  :path (:id el) :url (:valueSet (:binding el))}]
            (deps-from-additional-bindings el))))

(defmethod extract-deps
  "StructureDefinition"
  [res]
  (let [base {:definition (:url res)
              :definition_version (:version res)
              :package_name (:package_name res)
              :package_version (:package_version res)
              :defnition_id (:id res)
              :package_id (:package_id res)}]
    (->> (get-in res [:differential :element])
         (mapcat
          (fn [el]
            (concat
             (deps-from-type el)
             (deps-from-binding el))))
         (into [{:type :baseDefinition :url (:baseDefinition res)}])
         (map (fn [dep] (merge base dep)))
         (map #(dissoc % :path))
         (into #{}))))

(defmethod extract-deps :default
  [res]
  (assert false (pr-str res)))

(comment

  (system/stop-system context)

  (pg.repo/select context {:table "structuredefinition" :limit 10})
  (pg.repo/select context {:table "package_dependency" :limit 10})

  (pg.repo/select context {:table "structuredefinition_element" :where [:pg/sql "resource->'binding' is not null"] :limit 100})

  (->>
   (pg.repo/fetch context {:table "structuredefinition" :limit 100}
                  (fn [sd]
                    (doseq [d  (extract-deps sd)]
                      (println (last (str/split (:definition d) #"/")) (:type d) :> (:url d))))))

  ;; TODO process discriminator profiles
  ;; TODO make table
  ;; TODO impl pg.repo/fetch
  ;; TODO process and load deps
  ;; TODO we may resolve url into :id
  ;; TODO impl for VS

  ;; TODO: (canonical all-deps)
  ;; TODO: visualize deps


  )
