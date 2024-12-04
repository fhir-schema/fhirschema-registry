(ns far.package
  (:require [system]
            [fhir.package]
            [pg]
            [pg.repo]
            [cheshire.core]
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

(def canonical-columns
  {:package_name {:type "text" :required true}
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
            :primary-key [:name :version :resource]
            :columns {:name {:type "text" :required true}
                      :version {:type "text" :required true}
                      :resource {:type "jsonb"}}})

  (pg.repo/register-repo
   context {:table "package_dependency"
            :primary-key [:name :version :dep_name :dep_version]
            :columns {:name {:type "text" :required true}
                      :version {:type "text" :required true}
                      :dep_name {:type "text"}
                      :dep_version {:type "text"}
                      :resource {:type "jsonb"}}})

  (pg.repo/register-repo
   context {:table "structuredefinition_element"
            :primary-key [:package_name :package_version :definition :version :id]
            :columns {:package_name    {:type "text" :required true}
                      :package_version {:type "text" :required true}
                      :definition      {:type "text" :required true}
                      :version         {:type "text" :required true}
                      :id              {:type "text" :required true}
                      :path            {:type "text"}
                      :resource {:type "jsonb"}}})

  (doseq [tbl canonicals]
    (pg.repo/register-repo
     context {:table tbl
              :primary-key [:url :version :package_name :package_version]
              :columns canonical-columns})))


(defn loadable? [nm]
  (let [lnm (str/lower-case nm)]
    (and (str/ends-with? nm ".json")
         (or (contains? #{"package.json" ".index.json"} nm)
             (some (fn [x] (str/starts-with? lnm x)) canonicals)))))

(defn prepare-resource [nm pkgi res]
  (-> (assoc-in (dissoc res :text :snapshot :expansion) [:meta :file] nm)
      (assoc :package_name (:name pkgi)
             :package_version (:version pkgi)
             :version (or (:version res) (:version pkgi)))))

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

(defn- load-canonicals [context pkg]
  (doseq [cn canonicals]
    (when-let [rs (vals (get pkg cn))]
      (system/info context ::load (str cn " - " (count rs)))
      (pg.repo/load
       context {:table cn}
       (fn [insert]
         (doseq [r rs]
           (insert r)))))))

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
                 (pg.repo/insert context {:table "package_dependency" :resource {:name (:name pkgi) :version (:version pkgi) :dep_name (name k) :dep_version v}})
                 (load-package* context (name k) v (update opts [:path] (fn [x] (conj (or x []) (:name pkgi))))))
               context)))

(defn load-elements [context pkg-bundle]
  (pg.repo/load
   context {:table "structuredefinition_element"}
   (fn [insert]
     (doseq [[url sd] (get pkg-bundle "structuredefinition")]
       (doseq [el (get-in sd [:differential :element])]
         (insert
          (assoc el
                 :definition   url
                 :version      (or (:version sd) (:package_version sd))
                 :package_name (:package_name sd)
                 :package_version (:package_version sd))))))))

(defn resolve-all-deps [context pkv & [deps]]
  (let [deps (or deps {})]
    (->> (:dependencies pkv)
         (reduce (fn [deps [k v]]
                   (if (contains? deps (name k))
                     deps
                     (let [dp (pg.repo/read context {:table "package_version" :match {:name (name k) :version v}})]
                       (resolve-all-deps context dp (update deps (name k) (fn [x]
                                                                            (if (and x (not (= x v)))
                                                                              (assert false "version conflict")
                                                                              v)))))))
                 deps))))

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
        (pg.repo/insert context {:table "package_version" :resource (assoc package_version :all_dependencies all-deps)})
        (load-canonicals context pkg-bundle)
        (load-elements context pkg-bundle)
        (add-new-package context' {:package_name package-name, :package_version package-version})))))

(defn load-package
  "load package recursively"
  [context package-name package-version]
  (get-new-packages (load-package* context package-name package-version {})))

(defn truncate [context]
  (doseq [cn canonicals]
    (pg.repo/truncate context {:table cn}))
  (pg.repo/truncate context {:table "package"})
  (pg.repo/truncate context {:table "package_version"})
  (pg.repo/truncate context {:table "package_dependency"})
  (pg.repo/truncate context {:table "structuredefinition_element"}))


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
