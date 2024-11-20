(ns fhirschema.compiler
  (:require [clojure.string :as str]))

(defn required-element?
  [element]
  (= 1 (:min element)))

(defn coerce-element-max-cardinality
  [^String value]
  (if (= "*" value)
    Integer/MAX_VALUE
    (when (string? value)
      (parse-long value))))

(defn array-element?
  [element]
  (or (= "*" (:max element))
      (and (:min element) (>= (:min element) 2))
      (and (:max element) (>= (coerce-element-max-cardinality (:max element)) 2))))


(defn translate-element [])

(defn capitalize [s]
  (if (seq s)
    (str (str/upper-case (subs s 0 1)) (subs s 1))
    s))

(defn translate-element [ctx el]
  (let [path (->> (rest (str/split (:path el) #"\.")) (mapv keyword) (interpose :elements))]
    (cond
      (empty? path)
      []
      ;; union type
      (str/ends-with? (name (last path)) "[x]")
      (let [prefix-path (into [] (butlast path))
            lst (str/replace (name (last path)) #"\[x\]$" "")]
        (->> (:type el)
             (mapv (fn [tp]
                     [(conj prefix-path (keyword (str lst (capitalize (:code tp)))))
                      {:type (:code tp) :choiceOf lst}]))
             (into [[(conj prefix-path (keyword lst))
                     {:choices (into #{} (mapv (fn [x] (str lst (capitalize (:code x)))) (:type el)))}]])))

      (:contentReference el)
      [[path (cond-> {:elementReference (mapv keyword (rest (str/split (str/replace (:contentReference el) #"^#" "") #"\.")))}
               (array-element? el) (assoc :array true)
               (required-element? el) (assoc :required true))]]

      ;; simple type
      (= 1 (count (:type el)))
      [[path (cond-> {:type (get-in el [:type 0 :code])}
               (array-element? el) (assoc :array true)
               (required-element? el) (assoc :required true)
               ;; todo remove example
               (:binding el) (assoc :binding (select-keys (:binding el) [:valueSet :strength])))]]
      :else
      (do
        ;; (assert (= 1 (count (:type el))) "expected one type")
        [[path [:TODO el]]]))))


(defn translate [ctx sd]
  (let [schema (select-keys sd [:derivation :name :type :kind :abstract])
        els  (->> (get-in sd [:differential :element])
                  (reduce (fn [acc el]
                            (->> (translate-element {} el)
                                 (reduce (fn [acc [pth schema-el]]
                                           (assoc-in acc pth schema-el))
                                         acc)))
                          {}))]
    (if (empty? els)
      schema
      (assoc schema :elements els))))
