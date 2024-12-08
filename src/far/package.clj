(ns far.package
  (:require [system]
            [fhir.package]
            [pg]
            [pg.repo]
            [cheshire.core]
            [utils.uuid]
            [clojure.string :as str]))


(def canonicals
  ["activitydefinition"
   "actordefinition"
   "capabilitystatement"
   "chargeitemdefinition"
   "citation"
   "codesystem"
   "compartmentdefinition"
   "conceptmap"
   "conditiondefinition"
   "eventdefinition"
   "evidence"
   "evidencereport"
   "evidencevariable"
   "examplescenario"
   "graphdefinition"
   "implementationguide"
   "library"
   "measure"
   "messagedefinition"
   "namingsystem"
   "observationdefinition"
   "operationdefinition"
   "plandefinition"
   "questionnaire"
   "requirements"
   "searchparameter"
   "specimendefinition"
   "structuredefinition"
   "structuremap"
   "subscriptiontopic"
   "terminologycapabilities"
   "testplan"
   "testscript"
   "valueset"])

(def other-tables ["package" "package_version" "package_dependency" "structuredefinition_element" "canonical"])

(def all-tables (concat canonicals other-tables))

(def canonical-columns
  {:id {:type "uuid" :required true}
   :package_id {:type "uuid" :required true}
   :package_name {:type "text" :required true}
   :package_version {:type "text" :required true}
   :url {:type "text" :required true}
   :version {:type "text" :required true}
   :resource {:type "jsonb"}})

(defn migrate [context]

  (pg.repo/register-repo
   context {:table "package"
            :primary-key [:name]
            :columns {:name {:type "text" :required true}
                      :resource {:type "jsonb"}}})

  (pg.repo/register-repo
   context {:table "package_version"
            :primary-key [:id]
            :columns {:id   {:type "uuid"}
                      :name {:type "text" :required true}
                      :version {:type "text" :required true}
                      :resource {:type "jsonb"}}})

  (pg.repo/register-repo
   context {:table "package_dependency"
            :primary-key [:package_id :dep_id]
            :columns {:package_id  {:type "uuid" :required true}
                      :dep_id      {:type "uuid"}
                      :name        {:type "text" :required true}
                      :version     {:type "text" :required true}
                      :dep_name    {:type "text"}
                      :dep_version {:type "text"}
                      :resource    {:type "jsonb"}}})

  (pg.repo/register-repo
   context {:table "structuredefinition_element"
            :primary-key [:defnition_id  :id]
            :columns {:package_id      {:type "uuid" :required true}
                      :package_name    {:type "text" :required true}
                      :package_version {:type "text" :required true}
                      :defnition_id    {:type "uuid" :required true}
                      :definition      {:type "text" :required true}
                      :version         {:type "text" :required true}
                      :id              {:type "text" :required true}
                      :path            {:type "text"}
                      :resource {:type "jsonb"}}})

  (pg.repo/register-repo
   context {:table "canonical"
            :primary-key [:id :resource_type]
            :columns {:id               {:type "uuid" :required true}
                      :package_id       {:type "uuid" :required true}
                      :package_name     {:type "text" :required true}
                      :package_version  {:type "text" :required true}
                      :resource_type    {:type "text" :required true}
                      :url              {:type "text" :required true}
                      :version          {:type "text" :required true}
                      :resource         {:type "jsonb"}
                      }})

  (doseq [tbl canonicals]
    (pg.repo/register-repo
     context {:table tbl
              :primary-key [:id]
              :columns canonical-columns})))


(defn loadable? [nm]
  (let [lnm (str/lower-case nm)]
    (and (str/ends-with? nm ".json")
         (or (contains? #{"package.json" ".index.json"} nm)
             (some (fn [x] (str/starts-with? lnm x)) canonicals)))))

(defn prepare-resource [nm pkgi res]
  (let [res (-> (assoc-in (dissoc res :text :snapshot :expansion) [:meta :file] nm)
                (assoc
                 :package_id (utils.uuid/uuid (:name pkgi) (:version pkgi))
                 :package_name (:name pkgi)
                 :package_version (:version pkgi)
                 :resource_id (:id res)
                 :version (or (:version res) (:version pkgi))))]
    (assoc res :id (utils.uuid/canonical-id res))))

(defn read-package
  "load package into hash-map with :package :index and <rt>"
  [package-info]
  (let [pkgi package-info]
    (->
     (fhir.package/reduce-package
      pkgi
      (fn [acc nm read-file]
        (if (loadable? nm)
          (let [res (read-file true)]
            (cond
              (= nm "package.json")
              (assoc acc
                     :package_version res
                     :package_dependency (:dependencies res))

              (= nm ".index.json")
              (assoc acc :index res)
              (and (:resourceType res) (:url res))
              (let [res (prepare-resource nm pkgi res)
                    path (->> [(str/lower-case (or (:resourceType res) nm)) (:url res)] (filter identity))]
                (assoc-in acc path res))
              :else
              (do #_(println :skip nm) acc)))
          (do
            #_(println :skip nm)
            acc))))
     (assoc :package pkgi))))

(defn canonical-elements [r]
  (select-keys r [:id :package_id :package_name :package_version
                  :url :version
                  ;;sd
                  :kind :derivation :status :expiremental :fhirVersion :kind :abstract :type :baseDefinition
                  ;;cs
                  :title :name :content
                  ;; vs
                  :immutable]))

(defn- load-canonicals [context pkg]
  (pg.repo/load
   context {:table "canonical"}
   (fn [insert-canonical]
     (doseq [cn canonicals]
       (when-let [rs (vals (get pkg cn))]
         (system/info context ::load (str cn " - " (count rs)))
         (pg.repo/load
          context {:table cn}
          (fn [insert]
            (doseq [r rs]
              (insert-canonical (-> (canonical-elements r)
                                    (assoc  :resource_type (:resourceType r))
                                    (update :version (fn [x] (or x (:package_version x))))))
              (insert r)))))))))

(declare load-package*)

(defn add-new-package [context pkg]
  (update context ::new-packages (fn [x] (conj (or x []) pkg))))

(defn get-new-packages [context]
  (get context ::new-packages))

(defn clear-new-packages [context]
  (assoc context ::new-packages []))

(defn- load-deps [context pkg pkgi opts]
  (->> (:package_dependency pkg)
       (reduce (fn [context [k v]]
                 (let [resource {:package_id (utils.uuid/uuid (:name pkgi) (:version pkgi))
                                 :dep_id (utils.uuid/uuid (name k) v)
                                 :name (:name pkgi)
                                 :version (:version pkgi)
                                 :dep_name (name k)
                                 :dep_version v}]
                   (pg.repo/insert context {:table "package_dependency" :resource resource})
                   (load-package* context (name k) v (update opts [:path] (fn [x] (conj (or x []) (:name pkgi)))))))
               context)))

(defn load-elements [context pkg-bundle]
  (pg.repo/load
   context {:table "structuredefinition_element"}
   (fn [insert]
     (doseq [[url sd] (get pkg-bundle "structuredefinition")]
       (doseq [el (get-in sd [:differential :element])]
         ;; TODO: do it from database - duplicating id logic
         (insert
          (assoc el
                 :defnition_id (utils.uuid/uuid url (or (:version sd) (:package_version sd)) (:package_name sd) (:package_version sd))
                 :definition   url
                 :version      (or (:version sd) (:package_version sd))
                 :package_id (utils.uuid/uuid (:package_name sd) (:package_version sd))
                 :package_name (:package_name sd)
                 :package_version (:package_version sd))))))))

(defn resolve-all-deps* [context pkv & [acc]]
  (let [acc (or acc {:deps {} :level 0})]
    (->> (:dependencies pkv)
         (reduce (fn [acc [k v]]
                   (let [dep-id (utils.uuid/uuid (name k) v)]
                     (if (contains? (:deps acc) dep-id)
                       acc
                       (let [dp (pg.repo/read context {:table "package_version" :match {:id dep-id}})
                             _ (assert dp (str "Could not resolve dep " k " " v " " dep-id))
                             prev-level (:level acc)
                             acc (-> acc
                                     (assoc-in [:deps dep-id] {:name (name k) :level (:level acc) :version v :id dep-id})
                                     (update :level inc))]
                         (-> (resolve-all-deps* context dp acc)
                             (assoc :level prev-level))))))
                 acc))))

(defn resolve-all-deps [context pkv]
  (->> (vals (:deps (resolve-all-deps* context pkv)))
       (into [])
       (sort-by :level)))

(defn deps-tree [context pkv]
  (->> (:dependencies pkv)
       (reduce (fn [deps [k v]]
                 (assoc deps k {:version v
                                :deps (let [dp (pg.repo/read context {:table "package_version" :match {:name (name k) :version v}})]
                                        (deps-tree context dp))}))
               {})))

(defn print-deps-tree [deps-tree & [ident]]
  (let [ident (or ident 0)]
    (doseq [[k v] deps-tree]
      (println (str/join " " (mapv (constantly " ") (range ident))) (name k) (:version v))
      (when (seq (:deps v))
        (print-deps-tree (:deps v) (inc (inc ident)))))))

(defn load-package*
  "load package recursively"
  [context package-name package-version & [opts]]
  (if (pg.repo/read context {:table "package_version" :match {:name package-name :version package-version}})
    context
    (do
      (system/info context ::load-package (str package-name "@" package-version))
      (let [pkgi (fhir.package/pkg-info (str package-name "@" package-version))
            pkg-bundle (read-package pkgi)
            context' (load-deps context pkg-bundle pkgi opts)
            package_version (:package_version pkg-bundle)
            all-deps (resolve-all-deps context package_version)]
        (pg.repo/upsert context {:table "package" :resource pkgi})
        (pg.repo/insert context {:table "package_version"
                                 :resource (assoc package_version
                                                  :all_dependencies all-deps
                                                  :id (utils.uuid/uuid (:name package_version) (:version package_version)))})
        (load-canonicals context pkg-bundle)
        (load-elements context pkg-bundle)
        (add-new-package context' {:package_name package-name, :package_version package-version})))))

(defn load-package
  "load package recursively"
  [context package-name package-version]
  (get-new-packages (load-package* context package-name package-version {})))

(defn truncate [context]
  (doseq [cn all-tables]
    (pg.repo/truncate context {:table cn})))

(defn drop-tables [context]
  (doseq [cn all-tables]
    (pg/execute! context {:sql (str "drop table if exists " (name cn))})))

(system/defmanifest
  {:description "create tables and save packages into this tables"
   :deps ["pg.repo"]})

(defn pkg-info [context package-name]
  (fhir.package/pkg-info package-name))

(system/defstart
  [context config]
  (migrate context))

(system/defstop
  [context state])
