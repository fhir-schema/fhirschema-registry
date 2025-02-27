(ns far-cli
  (:require
   [clojure.string :as str]
   [clojure.tools.cli :as cli]
   [far-cli.operations :as far-cli]
   [far.package]
   [pg]
   [fhir.schema.transpiler]
   [fhir.schema.typeschema]
   [system])
  (:gen-class))

(defn defstart-far-cli [database verbosity]
  ;; NOTE: hmmm....
  (system/defmanifest
    {:description "create tables and save packages into this tables"
     :deps (concat (when database ["pg" "far.package"]))})

  (system/defstop [_context _state]
    (shutdown-agents))

  (system/start-system {:services (concat ["far-cli"]
                                          (when database ["pg" "far.package"]))
                        :system/log-level verbosity
                        :pg database}))

(def cli-options
  [["-p" "--package PACKAGE" "<name>[@<version>]"
    :id :packages
    :multi true :default [] :update-fn conj]

   ["-d" "--db URL" "database connection string"
    :id :database :parse-fn far-cli/parse-connection-string]

   ;; without db
   ["-i" "--info" "print package info"]
   ["-l" "--list" "list packages"]

   ;; with db
   [nil "--list-local" "list local packages"]
   [nil "--load"]
   ;; ["-c" "--canonical" "list canonicals"]

   ["-f" "--format FORMAT" "output format (text, json)"
    :default :text :parse-fn keyword
    :validate-fn #(contains? #{:text :json} %)]

   ["-v" nil "Verbosity level"
    :id :verbosity
    :default 0
    :update-fn inc]
   ["-h" "--help"]])

(defn -main [& args]
  (let [{options :options
         summary :summary
         errors :errors
         :as opts} (cli/parse-opts args cli-options)

        {packages :packages
         format :format
         database :database
         verbosity :verbosity} options

        context (defstart-far-cli database verbosity)]

    (system/debug context ::start opts)

    (cond
      (some? errors) (far-cli/print-cli-errors summary errors)
      (:help options) (println summary)

      (:load options)
      (doseq [package packages
              :let [[name ver] (str/split package #"@" 2)]]
        (assert (some? ver) "version is required")
        (when verbosity (println (str "Downloading " package)))
        (far.package/load-package context name ver))

      (:list options)
      (doseq [package (far.package/list-packages context)]
        (println (far-cli/show-package-list-item format package)))

      (:list-local options)
      (doseq [package (far.package/packages context {})]
        (prn package))

      (:info options)
      (doseq [package packages]
        (let [pkg-info (far.package/pkg-info context package)
              pkg-str (far-cli/show-package-info format pkg-info)]
          (println pkg-str)))

      :else (do (println "Please, specify action.")
                (println summary)))

    (system/stop-system context)))

(defn to-type-schema [schema])

(comment

  (def context (system/start-system {:services ["far-cli"]}))

  (def pkgi-r5 (far.package/pkg-info context "hl7.fhir.r5.core"))
  (def pkgi-tr5 (far.package/pkg-info context "hl7.terminology.r5"))

  (def pkg-r5-bundle (far.package/package-bundle context pkgi-r5))
  (def pkg-tr5-bundle (far.package/package-bundle context pkgi-tr5))

  (keys pkg-r5-bundle)

  (->> (get pkg-r5-bundle "structuredefinition")
       (keys)
       (sort))

  (->> (get pkg-tr5-bundle "codesystem")
       (keys)
       (sort))

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
   :name {:name "Patient" :url "" :package {:packageId "" :versioin "" :uri ""}},
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
