(ns fhir.schema.typeschema
  (:require [clojure.string :as str]))

(defn upcased?
  "Check if string starts with uppercase letter"
  [s]
  (and s
       (= (first s) (first (str/upper-case s)))))

(defn cap-case
  "Capitalize first letter of string"
  [s]
  (let [s (if (keyword? s) (name s) s)]
    (str (str/upper-case (subs (name s) 0 1)) (subs s 1))))

(defn extract-base
  "Extract base name from URL"
  [url]
  (last (str/split (str url) #"/")))

(defn ensure-dep
  "Ensure dependency exists in schema"
  [schema type-ref]
  (update schema :deps (fnil conj #{}) type-ref))


(defn add-field
  "Add a single field to the schema"
  [schema field-name type-ref field node]
  (let [result (cond-> {:type type-ref}
                 (:array field) (assoc :array true)
                 (:choice-of field) (assoc :choice-of (:choice-of field))
                 (some #(= % field-name) (:required node)) (assoc :required true))
        result (if-let [binding (:binding field)]
                (let [vs-ref {:name (extract-base (:value-set binding))
                             :package (get-in type-ref [:package] "")
                             :url (:value-set binding)
                             :type "valueset"}]
                  (-> result
                      (assoc :binding
                            {:value-set vs-ref
                             :strength (:strength binding)})))
                result)]
    (assoc-in schema [:fields field-name] result)))

(declare convert-field)

(defn add-fields
  "Add multiple fields to schema"
  [dest root type-schema path node]
  (reduce (fn [schema [field-name field]]
            (let [new-path (conj path (cap-case field-name))]
              (convert-field schema root type-schema new-path field-name field node)))
          dest (:elements node)))

(defn convert-field
  "Convert a FHIR schema field to type schema field"
  [dest root type-schema path field-name field node]
  (let [pkg-name (get-in root [:meta :package :name] "")]
    (cond
      (:choices field)
      (assoc-in dest [:choices field-name] {:choices (:choices field)})
      (:elements field)
      (let [typename (str/join "" path)
            nested-schema {:kind "nested"
                           :name {:name typename
                                  :package pkg-name
                                  :parent (:name root)}
                           :base {:name "BackboneElement"
                                  :package pkg-name}}]
        (-> dest
            (ensure-dep {:name "BackboneElement"
                        :package pkg-name
                        :type "complex-type"})
            (update :nested-types (fnil conj [])
                   (add-fields nested-schema root type-schema path field))))

      (:type field)
      (let [typename (:type field)
            type (if (upcased? typename)
                  "complex-type"
                  "primitive-type")
            type-ref {:name typename
                     :package pkg-name
                     :type type}]
        (add-field dest field-name type-ref field node))

      (:element-reference field)
      (let [ref-path (remove #(= % "elements") (rest (:element-reference field)))
            typename (str (:name (:name type-schema))
                         (str/join "" (map cap-case ref-path)))
            type-ref {:name typename
                     :package pkg-name
                     :type "nested"}]
        (add-field dest field-name type-ref field node))
      :else
      (add-field dest field-name {:name "unknown" :package pkg-name :type "unknown"} field node))))

(defn convert
  "Convert FHIR schema to type schema"
  [schema]
  (let [kind (cond
               (and (= (:kind schema) "resource")
                    (= (:derivation schema) "constraint")) "profile"
               (= (:kind schema) "resource") "resource"
               (= (:kind schema) "complex-type") "complex-type"
               (= (:kind schema) "primitive-type") "primitive-type"
               (= (:kind schema) "logical") "logical"
               :else (throw (ex-info "Unknown schema kind" {:kind (:kind schema) :derivation (:derivation schema)})))
        pkg-name (get-in schema [:meta :package :name] "")
        result {:kind kind
                :name {:name (:name schema)
                       :package pkg-name}}]
    (cond-> result
      (:base schema)
      (-> (assoc :base {:name (extract-base (:base schema))
                        :package pkg-name})
          (ensure-dep {:name (extract-base (:base schema)) :package pkg-name :type "resource"}))
      (:elements schema)
      (add-fields schema result [(:name schema)] schema))))

;; typeschema
