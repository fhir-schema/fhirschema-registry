(ns far-cli
  (:require
   [cheshire.core :as json]
   [clojure.string :as str]
   [clojure.pprint :as pprint]
   [clojure.tools.cli :as cli]
   [far.package]
   [fhir.schema.transpiler]
   [fhir.schema.typeschema]
   [system])
  (:gen-class))

;; database less version?

(system/defmanifest
  {:description "create tables and save packages into this tables"
   :deps ["far-cli"]})

(system/defstop [_context _state]
  (shutdown-agents))

(def cli-options
  [["-p" "--package PACKAGE"
    :id :packages
    :multi true :default [] :update-fn conj]
   ;; ["-d" "--database" "database connection string"]

   ["-i" "--info" "print package info"]
   ["-l" "--list" "list packages"]

   ["-f" "--format FORMAT" "output format (text, json)"
    :default :text :parse-fn keyword
    :validate-fn #(contains? #{:text :json} %)]

   ["-v" "--verbose"]
   ["-h" "--help"]])

;; cli -package hl7.fhir.core.r4 output typeschema --file ts.ndjson

(defn- show-package-info [format pkg-info]
  (case format
    :text (str/trim (with-out-str (pprint/pprint pkg-info)))
    :json (json/generate-string pkg-info)))

(defn- show-package-list-item [format item]
  (case format
    :text (str (:id item)
               (when (:title item) (str " -- " (:title item))))
    :json (json/generate-string item)))

(defn- print-cli-errors [summary errors]
  (println "CLI errors:")
  (doall (map #(println (str "  " %)) errors))
  (println)
  (println "Help")
  (println summary))



(defn -main [& args]
  (let [{options :options
         summary :summary
         errors :errors
         :as opts} (cli/parse-opts args cli-options)

        {packages :packages
         format :format
         verbose? :verbose} options

        context (system/start-system {:services ["far-cli"]
                                      :system/log-level (system/log-levels :error)})]

    (when verbose? (pprint/pprint opts))

    (cond
      (some? errors) (print-cli-errors summary errors)
      (:help options) (println summary)

      (:list options)
      (doseq [package (far.package/list-packages context)]
        (println (show-package-list-item format package)))

      (:info options)
      (doseq [package packages]
        (let [[name version] (str/split package #"#" 2)
              {error :error :as pkg-info} (far.package/pkg-info context name)
              pkg-info (cond
                         (some? error) {:id package
                                        :error error}

                         (and (some? version)
                              (not (contains? (set (:versions pkg-info)) version)))
                         (assoc pkg-info :error
                                {:code "E404"
                                 :summary (str "Specific version not found: " package)
                                 :details (str "Available versions: " (str/join ", " (:versions pkg-info)))})

                         (and (some? version)
                              (contains? (set (:versions pkg-info)) version))
                         (assoc pkg-info :version version)

                         :else pkg-info)

              pkg-str (show-package-info format pkg-info)]
          (println pkg-str)))

      :else (println summary))

    (system/stop-system context)))

(defn to-type-schema [schema]

  )

(comment

  (def context (system/start-system {:services ["far-cli"]}))


  (def pkgi-r5 (far.package/pkg-info context "hl7.fhir.r5.core"))
  (def pkgi-tr5 (far.package/pkg-info context "hl7.terminology.r5"))

  (def pkg-r5-bundle (far.package/package-bundle context pkgi-r5))
  (def pkg-tr5-bundle (far.package/package-bundle context pkgi-tr5))

  (keys pkg-r5-bundle)

  (->> (get pkg-r5-bundle "structuredefinition")
       (keys)
       (sort)
       )

  (->> (get pkg-tr5-bundle "codesystem")
       (keys)
       (sort)
       )

  (far.package/pkg-info context "hl7.fhir.r4.core")

  (def pt-sd (get-in pkg-r5-bundle ["structuredefinition" "http://hl7.org/fhir/StructureDefinition/Patient"]))

  (:version pt-sd)

  pt-sd

  (def sch (fhir.schema.transpiler/translate (get-in pkg-r5-bundle ["structuredefinition" "http://hl7.org/fhir/StructureDefinition/Patient"])))

  sch

  (fhir.schema.typeschema/convert sch)

  ;; url -> version -> representation (sd,fs,ts) -> [resource + package-info]
  ;; resolve ->

  ;; * kind:code resource | profile | complex-type | primitive-type | logical | (inteface | pattern)
  ;; * name:Symbol {name, package, type}
  ;; * deps:Dep[]  {name, package, type}
  ;; * fields:Map(code,Field)
  ;; * choices:
  ;; * nestedTypes:

  ;; Field:
  ;; * order
  ;; * type:Symbol
  ;; * array:boolean
  ;; * biding


  {:kind "resource",
   :name {:name "Patient" :url "" :package {:packageId "" :versioin "" :uri ""} },
   :base {:name "DomainResource", :package {}},
   :deps
   #{{:name "BackboneElement", :package {}, :type "complex-type"} ;; valueset | primitive-type | resource
     {:name "DomainResource",  :package {}, :type "resource"}},
   :fields
   {:multipleBirthBoolean {:order 0 :type {:name "boolean", :url "" :package {}, :type "primitive-type"}},
    :address              {:order 1 :type {:name "Address", :url "" :package {}, :type "complex-type"}, :array true},
    :managingOrganization {:type {:name "Reference"} :refers #{}},
    :deceasedBoolean      {:type {:name "boolean"}},
    :name                 {:type {:name "HumanName", :package "", :type "complex-type"}, :array true},
    :maritalStatus        {:type {:name "CodeableConcept", :package "", :type "complex-type"},
                           :binding {:value-set {:name "", :package "", :url "url"},
                                     :strength "extensible"}}
    :link {:type {:name "PatientLink" :type "nested"}}},
   :choices
   {:multipleBirth {:choices ["multipleBirthBoolean" "multipleBirthInteger"]},
    :deceased {:choices ["deceasedBoolean" "deceasedDateTime"]}},

   :nested-types
   [{:kind "nested",
     :name {:name "PatientLink", :package "", :parent "Patient"},
     :base {:name "BackboneElement"},
     :fields
     {:other {:type {:name "Reference", :package "", :type "complex-type"}},
      :type  {:type {:name "code", :package "", :type "primitive-type"},
              :binding {:value-set {:name "", :package "", :url nil, :type "valueset"},
                        :strength "required"}}}}]})
