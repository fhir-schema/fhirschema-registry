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
  [context package-info]
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

(declare load-package)

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
                 (load-package context (name k) v (update opts [:path] (fn [x] (conj (or x []) (:name pkgi))))))
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

(defn load-package
  "load package recursively"
  [context package-name package-version & [opts]]
  (if (pg.repo/read context {:table "package_version" :match {:name package-name :version package-version}})
    context
    (do
      (system/info context ::load-package (str package-name "@" package-version))
      (let [pkgi (fhir.package/pkg-info (str package-name "@" package-version))
            pkg-bundle (read-package context pkgi)
            context' (load-deps context pkg-bundle pkgi opts)]
        (pg.repo/upsert context {:table "package" :resource pkgi})
        (pg.repo/insert context {:table "package_version" :resource (:package pkg-bundle)})
        (load-canonicals context pkg-bundle)
        (load-elements context pkg-bundle)
        (add-new-package context' {:package_name package-name, :package_version package-version})))))

(defn truncate [context]
  (doseq [cn canonicals]
    (pg.repo/truncate context {:table cn}))
  (pg.repo/truncate context {:table "package"})
  (pg.repo/truncate context {:table "package_version"})
  (pg.repo/truncate context {:table "package_dependency"})
  (pg.repo/truncate context {:table "structuredefinition_element"}))


(comment

  (def context (system/start-system {:services ["pg" "pg.repo" "gcs" "fhir-pkg"]
                                     :http {:port 7777}
                                     :pg (cheshire.core/parse-string (slurp "connection.json") keyword)}))

  (def pkgi (fhir.package/pkg-info "hl7.fhir.us.core"))

  pkgi

  (def result-context (load-package context (:name pkgi) (:version pkgi)))

  (get-new-packages result-context)

  ;; todo run terminology on new packages
  ;; todo run fhirschema on new packages

  (migrate context)

  (truncate context)

  (system/stop-system context)


  (pg.repo/select context {:table "structuredefinition" :limit 10})
  (pg.repo/select context {:table "structuredefinition_element" :limit 10})



  )
