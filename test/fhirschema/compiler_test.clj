(ns fhirschema.compiler-test
  (:require [clojure.test :refer [deftest testing]]
            [clojure.string :as str]
            [fhirschema.compiler :refer [translate-element translate]]
            [matcho.core :as matcho]))

;; scalar - we can set scalar on kind complex type or resource or logical model and max = 1

(def sd
  {:resourceType "StructureDefinition",
   :url "http://hl7.org/fhir/StructureDefinition/Questionnaire",
   :kind "resource",
   :description "A structured...",
   :id "Questionnaire",
   :name "Questionnaire",
   :type "Questionnaire",
   :abstract false,
   :baseDefinition "http://hl7.org/fhir/StructureDefinition/DomainResource"
   :derivation "specialization",
   :version "4.0.1",
   :differential
   {:element
    [{:id "Questionnaire" :path "Questionnaire", :min 0, :max "*"}
     {:id "Patient.extension:race"
      :path "Patient.extension",
      :min 0, :max "1",
      :type [{:code "Extension", :profile ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"]}],
      :mustSupport false,
      :sliceName "race"}
     ;; singular field
     {:path "Questionnaire.url", :min 0, :type [{:code "uri"}], :max "1", :id "Questionnaire.url", :isSummary true}
     ;; array
     {:id "Questionnaire.identifier", :path "Questionnaire.identifier", :min 0, :type [{:code "Identifier"}], :max "*", :isSummary true}
     ;; required
     {:id "Questionnaire.status", :path "Questionnaire.status", , :type [{:code "code"}],
      :min 1 :max "1"
      :binding {:strength "required", :valueSet "http://hl7.org/fhir/ValueSet/publication-status|4.0.1"}}
     {:path "Questionnaire.item", :id "Questionnaire.item", :min 0, :type [{:code "BackboneElement"}], :max "*",}
     {:path "Questionnaire.item.linkId", :type [{:code "string"}], :max "1", :id "Questionnaire.item.linkId",}
     ;; union
     {:id "Questionnaire.item.enableWhen.answer[x]",
      :path "Questionnaire.item.enableWhen.answer[x]",
      :type [{:code "dateTime"}
             {:code "Coding"}
             {:code "Quantity"}
             {:code "Reference", :targetProfile ["http://hl7.org/fhir/StructureDefinition/Resource"]}
             ],
      :binding {:strength "example", :valueSet "http://hl7.org/fhir/ValueSet/questionnaire-answers"},
      :min 1, :max "1",}
     ;; reference
     {:path "Questionnaire.item.item",
      :min 0,
      :contentReference "#Questionnaire.item",
      :max "*",
      :id "Questionnaire.item.item"}]}})

;; ctx
;; slicing -> slice mode
;; sliceName -> push slice (slice-path)
;; sliceName -> replace slice
;; path < slice-path pop slice
;; path < slicling push
(def pf
  {:url "http://hl7.org/fhir/StructureDefinition/Profile",
   :baseDefinition "http://hl7.org/fhir/StructureDefinition/Observation"
   :kind "resource",
   :derivation "specialization",
   :differential
   {:element
    [{:id "Observation", :path "Observation", :short "US Core Blood Pressure Profile"}
     {:id "Observation.code", :path "Observation.code", :short "Blood Pressure", :type [{:code "CodeableConcept"}], :patternCodeableConcept {:coding [{:system "http://loinc.org", :code "85354-9"}]}, :mustSupport true}
     {:id "Observation.component", :path "Observation.component", :slicing {:discriminator [{:type "pattern", :path "code"}], :ordered false, :rules "open"}, :short "Component observations", :min 2, :max "*", :mustSupport true}
     {:id "Observation.component:systolic", :path "Observation.component", :sliceName "systolic", :short "Systolic Blood Pressure", :min 1, :max "1", :mustSupport true}
     {:id "Observation.component:systolic.code", :path "Observation.component.code", :short "Systolic Blood Pressure Code", :min 1, :max "1", :patternCodeableConcept {:coding [{:system "http://loinc.org", :code "8480-6"}]}, :mustSupport true}
     {:id "Observation.component:systolic.valueQuantity", :path "Observation.component.valueQuantity", :short "Vital Sign Component Value", :type [{:code "Quantity"}], :mustSupport true}
     {:id "Observation.component:systolic.valueQuantity.value", :path "Observation.component.valueQuantity.value", :min 1, :max "1", :type [{:code "decimal"}], :mustSupport true}
     {:id "Observation.component:systolic.valueQuantity.unit", :path "Observation.component.valueQuantity.unit", :min 1, :max "1", :type [{:code "string"}], :mustSupport true}
     {:id "Observation.component:systolic.valueQuantity.system", :path "Observation.component.valueQuantity.system", :min 1, :max "1", :type [{:code "uri"}], :fixedUri "http://unitsofmeasure.org", :mustSupport true}
     {:id "Observation.component:systolic.valueQuantity.code", :path "Observation.component.valueQuantity.code", :min 1, :max "1", :type [{:code "code"}], :fixedCode "mm[Hg]", :mustSupport true}
     {:id "Observation.component:diastolic", :path "Observation.component", :sliceName "diastolic", :short "Diastolic Blood Pressure", :min 1, :max "1", :mustSupport true}
     {:id "Observation.component:diastolic.code", :path "Observation.component.code", :short "Diastolic Blood Pressure Code", :min 1, :max "1", :patternCodeableConcept {:coding [{:system "http://loinc.org", :code "8462-4"}]}, :mustSupport true}
     {:id "Observation.component:diastolic.valueQuantity", :path "Observation.component.valueQuantity", :short "Vital Sign Component Value", :type [{:code "Quantity"}], :mustSupport true}
     {:id "Observation.component:diastolic.valueQuantity.value", :path "Observation.component.valueQuantity.value", :min 1, :max "1", :type [{:code "decimal"}], :mustSupport true}
     {:id "Observation.component:diastolic.valueQuantity.unit", :path "Observation.component.valueQuantity.unit", :min 1, :max "1", :type [{:code "string"}], :mustSupport true}
     {:id "Observation.component:diastolic.valueQuantity.system", :path "Observation.component.valueQuantity.system", :min 1, :max "1", :type [{:code "uri"}], :fixedUri "http://unitsofmeasure.org", :mustSupport true}
     {:id "Observation.component:diastolic.valueQuantity.code", :path "Observation.component.valueQuantity.code", :min 1, :max "1", :type [{:code "code"}], :fixedCode "mm[Hg]", :mustSupport true}]}})


(def fs
  {
   :name "Questionnaire",
   :type "Questionnaire",
   :kind "resource",
   :derivation "specialization",
   :abstract false,

   :extensions
   {:race {:url "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race" :max 1}}

   :elements
   {:url {:type "uri"},
    :identifier {:type "Identifier", :array true},
    :status {:type "code", :required true, :binding {:valueSet "http://hl7.org/fhir/ValueSet/publication-status|4.0.1", :strength "required"}},
    :item {:type "BackboneElement",
           :array true,
           :elements
           {:linkId {:type "string" :scalar true},
            :item {:elementReference [:item], :array true}
            :enableWhen {:elements
                         {:answer {:choices #{"answerDateTime" "answerCoding" "answerQuantity" "answerReference"}},
                          :answerQuantity  {:type "Quantity", :choiceOf "answer"},
                          :answerDateTime  {:type "dateTime", :choiceOf "answer"},
                          :answerCoding    {:type "Coding", :choiceOf "answer"}
                          :answerReference {:type "Reference", :choiceOf "answer"
                                            #_#_:referes ["http://hl7.org/fhir/StructureDefinition/Resource"]
                                            }
                          }}}}}}
  )

(deftest test-converter
  (matcho/match (translate {} sd) fs)

  )


(comment
  (dissoc (cheshire.core/parse-string (slurp "/Users/niquola/fhirshcema-registry/tmp/node_modules/hl7.fhir.r4.core/StructureDefinition-Questionnaire.json") keyword) :text :shapshot)

  [
   {:path "a"}
   {:path "b"}
   {:path "b.d"}
   {:path "b.d" :sliceName "s1"} ;; [{:el :b} {:el :d :sn "s1"}]
   {:path "b.d.c" :type "x"}     ;; [{:el :b} {:el :d} {:el "c"}] => merge [{:el :b} {:el :d :sn "s1"} {:el "c"}]
   {:path "b.d.g" :type "y"}
   {:path "b.d" :sliceName "s2"} ;; [{:el :b} {:el :d :sn "s2"}]
   {:path "b.d.c" :type "x2"}
   {:path "b.d.g" :type "y2"}
   {:path "e" :type "e"}         ;; [{:el :e}]
   ]
  ;; length should be the same or smaller to react
  ;; 1. merge prev with current
  ;; 2. emit events

  ;; prev [{:el :b} {:el :d :sn "s1"} {:el "c"}] 
  ;; next [{:el :b} {:el :d :sn "s2"}]
  ;; exit everything from tail
  ;; compare last ->
  ;; * [:exit  {:el "c"}]
  ;; * [:exit  {:sn "s1"}]
  ;; * [:enter {:sn "s2"}]

  

  )

(defn parse-path [p el]
  (let [path (->> (str/split p #"\.")
                  (mapv (fn [x] {:el (keyword x)})))]
    (update path (dec (count path)) merge (select-keys el [:slicing :sliceName]))))

(parse-path "a.b" {:slicing {}})


(defn common-path [p1 p2]
  (loop [[px & pxs] p1
         [py & pxy] p2
         cp []]
    (if (or (nil? px) (nil? py) (not (= (:el px) (:el py))))
      cp
      (recur pxs pxy (conj cp px)))))

(defn enrich-path [p1 p2]
  (loop [[px & pxs] p1
         [py & pxy :as p2] p2
         cp []]
    (if (or (nil? px) (nil? py) (not (= (:el px) (:el py))))
      (if py (into cp p2) (into cp pxy))
      (recur pxs pxy (conj cp (merge py (select-keys px [:slicing :sliceName])))))))

(enrich-path [{:el :a} {:el :b} {:el :c}] [{:el :a} {:el :b} {:el :d}])

(common-path [:a :b :c :d] [:a :b :f :g])

(defn actions [prev-path new-path]
  (let [prev-c (count prev-path)
        new-c  (count new-path)
        cp     (common-path prev-path new-path)
        cp-c   (count cp)
        exits  (loop [i prev-c exits []]
                 (if (<= i cp-c)
                   exits
                   (recur (dec i) (conj exits {:type :exit :el (:el (nth prev-path (dec i)))}))))
        enters (loop [i cp-c  enters exits]
                 (if (>= i new-c)
                   enters
                   (recur (inc i) (conj exits {:type :enter :el (:el (nth new-path i))}))))]
    (println :gen-actions prev-path new-path enters)
    enters))

(defn apply-actions [value-stack actions value]
  (println :actions actions)
  (->> actions
       (reduce (fn [stack {tp :type el :el}]
                 (cond
                   (= :enter tp) (conj stack value)
                   (= :exit tp) (let [v (peek stack)
                                      prev-stack (pop stack)
                                      vv (peek prev-stack)
                                      prev-prev-stack (pop prev-stack)]
                                  (println :assoc [:elements el] )
                                  (conj prev-prev-stack (assoc-in vv [:elements el] v)))
                   :else (assert false)))
               value-stack)))



(defn build [els]
  (loop [value-stack [{}]
         prev-path []
         els els]
    (if (empty? els)
      (let [acs (actions prev-path [])
            new-value-stack (apply-actions value-stack acs {})]
        (println [] acs new-value-stack)
        (first new-value-stack))
      (let [e (first els)
            new-path (enrich-path prev-path (parse-path (:path e) e))
            acs (actions prev-path new-path)
            new-value-stack (apply-actions value-stack acs (select-keys e [:v]))]
        (println prev-path :> new-path :> acs)
        (recur new-value-stack new-path (rest els))))))

(def els
  [{:path "a" :v :a}
   {:path "b" :v :b}
   {:path "c" :v :c}
   {:path "c.d" :v :c.d}
   {:path "c.d.f" :v :c.d.f}
   {:path "c.d.i" :v :c.d.i}
   {:path "x" :v :x}
   {:path "x" :slicing {}}
   {:path "x" :sliceName "s1"}
   {:path "x.a" :type "a"}
   {:path "x.b" :type "b"}
   {:path "x" :sliceName "s2"}
   {:path "x.a" :type "a2"}
   {:path "x.b" :type "a2"}

   ])

;; migrate to [{:el :k}] pathes

(clojure.pprint/pprint (build els))
