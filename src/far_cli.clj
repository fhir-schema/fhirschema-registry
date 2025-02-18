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

;; # FAR CLI
;;
;; Shortcut:
;;
;;      $ clj -M:far-cli --help
;;      $ far-cli
;;
;; Some operations require a database connection:
;;
;;     $ export DB=postgresql://fhirpackages:secret@localhost:5437/fhirpackages
;;
;;
;; ## Root Package
;;
;; FHIR Server should work with different types of resources, codesystems,
;; valuesets, etc. These elements may be defined in several packages. Root
;; package -- is an entity, which allows us to list required packages, their
;; version and custom resource definitions.
;;
;; We have several approaches to work with root package:
;;
;; 1. (aidbox) implicitly: use all installed packages
;;
;; 2. (far-cli-v1) anonymously: specify all packages in CLI
;;
;; 3. (far-cli-vn) explicitly, like we do it in npm via `package.json` and
;; `package-lock.json`:
;;
;;
;; ## Package Exploration
;;
;; Print all available packages (from Aidbox search tree):
;;
;;      $ far-cli search
;;
;; Print all available packages in json:
;;
;;      $ far-cli search --json
;;
;; Print package info (if db available -- provide installation info):
;;
;;      $ far-cli info --package us.nlm.vsac
;;
;; Print info for multiple packages with specific version:
;;
;;      $ far-cli info --package us.nlm.vsac@0.1.0 --package us.nlm.vsac@0.12.0
;;
;;
;; ## Package Management
;;
;; List all installed packages:
;;
;;     $ clj -M:far-cli --db $DB list
;;
;; Install packages & print all installed packages:
;;
;;     $ clj -M:far-cli --db $DB install -p us.nlm.vsac -p us.nlm.vsac@0.1.0
;;
;; Remove packages:
;;
;;     $ clj -M:far-cli --db $DB remove -p us.nlm.vsac -p us.nlm.vsac@0.1.0
;;
;;
;; ## Anonymous Root Package Management
;;
;; Print root-package info with all installed packages:
;;
;;     $ clj -M:far-cli info --root-package
;;
;; Print root-package with specific packages:
;;
;;     $ clj -M:far-cli info --root-package -p us.nlm.vsac@0.1.0
;;
;; Export root package content defined by package list to specific
;; path (implicitly set `--entity sd`, which means that we need
;; structure-definitions in the output):
;;
;;     $ clj -M:far-cli --db $DB export --output path -p us.nlm.vsac
;;
;; Export root package defined by canonicals list to ndjson as a FHIR Schema and
;; Type Schema set:
;;
;;     $ clj -M:far-cli --db $DB export --output file.ndjson \
;;         --entity fs --entity sd -c http://hl7.org/fhir/StructureDefinition/Patient
;;
;; Print only specific canonical definition to stdout in FHIR Schema and
;; structure-definitions format:
;;
;;     $ clj -M:far-cli --db $DB export --no-deps \
;;         --entity fs --entity sd -c http://hl7.org/fhir/StructureDefinition/Patient
;;
;; Export root package with custom resources:
;;
;;     $ clj -M:far-cli --db $DB export -p us.nlm.vsac@0.1.0 -a foo.json -a bar/*.json
;;
;; Print specific canonical definition log (version history) (implicitly: `--no-deps`):
;;
;;     $ clj -M:far-cli --db $DB export log \
;;         --entity fs --entity sd -c http://hl7.org/fhir/StructureDefinition/Patient
;;
;;
;; ## Explicit Root Package Management
;;
;; Create root package on the file system:
;;
;;     $ far-cli init
;;
;; Install package into current root package or fail if outside:
;;
;;     $ far-cli install fhir.r4
;;
;; Add custom resource definition:
;;
;;     $ far-cli add foo.json
;;
;; Export like above but much shorter:
;;
;;     $ far-cli export
;;
;; Generate lock file which fix version for transitive dependencies and interval
;; requirements:
;;
;;     $ far-cli lock
;;
;; FHIR packages is a npm package and it can be published:
;;
;;     $ far-cli publish
;;
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
