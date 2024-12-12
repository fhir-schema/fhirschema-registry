(ns far.package.repos
  (:require [system]
            [fhir.package]
            [pg]
            [pg.repo]
            [cheshire.core]
            [utils.uuid]
            [far.package.canonical-deps :refer [extract-deps]]
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

(def other-tables ["package" "package_version" "package_dependency" "structuredefinition_element" "canonical" "canonical_deps"])

(def all-tables (concat canonicals other-tables))

(def canonical-columns
  {:id {:type "uuid" :required true}
   :package_id {:type "uuid" :required true :index true}
   :package_name {:type "text" :required true}
   :package_version {:type "text" :required true}
   :url {:type "text" :required true :index true}
   :version {:type "text" :required true :index true}
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
                      :dep_id      {:type "uuid" :index true}
                      :name        {:type "text" :required true}
                      :version     {:type "text" :required true}
                      :dep_name    {:type "text"}
                      :dep_version {:type "text"}
                      :resource    {:type "jsonb"}}})

  (pg.repo/register-repo
   context {:table "structuredefinition_element"
            :primary-key [:definition_id  :id]
            :columns {:package_id      {:type "uuid" :required true}
                      :package_name    {:type "text" :required true}
                      :package_version {:type "text" :required true}
                      :definition_id    {:type "uuid" :required true :index true}
                      :definition      {:type "text" :required true}
                      :version         {:type "text" :required true}
                      :id              {:type "text" :required true}
                      :path            {:type "text"}
                      :resource {:type "jsonb"}}})

  (pg.repo/register-repo
   context {:table "canonical"
            :primary-key [:id :resource_type]
            :columns {:id               {:type "uuid" :required true}
                      :package_id       {:type "uuid" :required true :index true}
                      :package_name     {:type "text" :required true}
                      :package_version  {:type "text" :required true}
                      :resource_type    {:type "text" :required true}
                      :url              {:type "text" :required true :index true}
                      :version          {:type "text" :required true :index true}
                      :resource         {:type "jsonb"}}})

  (pg.repo/register-repo
   context {:table  "canonical_deps"
            :primary-key [:definition_id :type :url]
            :columns {:package_id {:type "uuid" :index true :required true}
                      :definition_id {:type "uuid" :index true :required true}
                      :definition {:type "text" :required true}
                      :type {:type "text"}
                      :package_name {:type "text"}
                      :package_version {:type "text"}
                      :definition_version {:type "text"}
                      :resource_type {:type "text" :required true}
                      :url {:type "text" :index true}
                      :version {:type "text" :index true}
                      :status {:type "text"}
                      :dep_id {:type "uuid"}
                      :dep_package_id {:type "uuid" :index true}}})

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

(defn truncate [context]
  (doseq [cn all-tables]
    (pg.repo/truncate context {:table cn})))

(defn drop-tables [context]
  (doseq [cn all-tables]
    (pg/execute! context {:sql (str "drop table if exists " (name cn))})))
