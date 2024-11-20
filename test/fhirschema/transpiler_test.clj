(ns fhirschema.transpiler-test
  (:require [clojure.test :refer [deftest testing]]
            [fhirschema.transpiler :refer [parse-path get-common-path calculate-actions translate]]
            [matcho.core :as matcho]))


(deftest test-algorythm
  (matcho/match
   (parse-path "R.a" {})
   [{:el :a}])

  (matcho/match
   (parse-path "R.a.b" {})
   [{:el :a} {:el :b}])


  (matcho/match
   (calculate-actions (parse-path "R.a" {}) (parse-path "R.b" {}))
   [{:type :exit, :el :a} {:type :enter, :el :b}])

  (matcho/match
   (calculate-actions (parse-path "R.a.b.c" {}) [])
   [{:type :exit, :el :c} {:type :exit, :el :b} {:type :exit, :el :a}])

  (matcho/match
   (get-common-path
    (conj (parse-path "R.a.c" {:sliceName "s1"}) {:el :b})
    (conj (parse-path "R.a.c" {:sliceName "s2"})))
   [{:el :a} {:el :c}])


  (matcho/match
   (calculate-actions
    []
    (conj (parse-path "R.a" {:sliceName "s1"}) {:el :b}))
   [{:type :enter, :el :a}
    {:type :enter-slice, :sliceName "s1"}
    {:type :enter, :el :b}])

  (matcho/match
   (calculate-actions
    (conj (parse-path "R.a" {:sliceName "s1"}) {:el :b})
    (conj (parse-path "R.a" {:sliceName "s2"})))
   [{:type :exit, :el :b}
    {:type :exit-slice  :sliceName "s1"}
    {:type :enter-slice :sliceName "s2"}])

  (matcho/match
   (calculate-actions
    (conj (parse-path "R.a" {:sliceName "s1"}) {:el :b})
    [])
   [{:type :exit, :el :b}
    {:type :exit-slice, :sliceName "s1"}
    {:type :exit, :el :a}])

  (calculate-actions
   [{:sliceName :s1, :el :x} {:el :b}]
   [{:sliceName :s1, :el :x} {:el :b, :sliceName :z1}]
   )

  (def els
    [{:path "R"       :short :a}
     {:path "R.a"     :short :a}
     {:path "R.b"     :short :b}
     {:path "R.c"     :short :c}
     {:path "R.c.d"   :short :c.d}
     {:path "R.c.d.f" :short :c.d.f}
     {:path "R.c.d.i" :short :c.d.i}
     {:path "R.x"     :short :x}
     {:path "R.x" :slicing {:discriminator [{:short "pattern" :path "a"}]}}
     {:path "R.x" :sliceName "s1"}
     {:path "R.x.a"   :short :x.s1.a :patternString "s1"}
     {:path "R.x.b"   :short :x.s1.b}
     {:path "R.x" :sliceName "s2"}
     {:path "R.x.a"   :short :x.s2.a :patternString "s2"}
     {:path "R.x.b"   :short :x.s2.b}])

  ;; FOCUS
  (matcho/match
   (translate {:differential {:element els}})
   {:elements
    {:a {:short :a},
     :b {:short :b},
     :c {:short :c,
         :elements {:d {:short :c.d,
                        :elements {:f {:short :c.d.f}
                                   :i {:short :c.d.i}}}}},
     :x {:short :x,
         :slicing {:slices {:s1 {:match {:a "s1"}
                                 :elements {:a {:short :x.s1.a}
                                            :b {:short :x.s1.b}}}
                            :s2 {:match {:a "s2"}
                                 :elements {:a {:short :x.s2.a}
                                            :b {:short :x.s2.b}}}}}}}})

  (def els-sl
    [{:path "R.x"        :short :x  :slicing {:discriminator [{:type "pattern" :path "a"}]}}
     {:path "R.x"        :sliceName "s1"}
     {:path "R.x.a"      :short :x.s1.a :patternString "s1"}
     {:path "R.x.b"      :short :x.s1.b :slicing {:discriminator [{:type "pattern" :path "f.ff"}]}}
     {:path "R.x.b"      :sliceName "z1"}
     {:path "R.x.b.f"    :short :x.s1.b.z1.f}
     {:path "R.x.b.f.ff" :short :x.s1.b.z1.ff :patternCoding {:code "z1"}}
     {:path "R.x.b"      :sliceName "z2"}
     {:path "R.x.b.f"    :short :x.s1.b.z2.f}
     {:path "R.x.b.f.ff" :short :x.s1.b.z2.ff :patternCoding {:code "z2"}}
     {:path "R.x"        :sliceName "s2"}
     {:path "R.x.a"      :short :x.s2.a :patternString "s2"}
     {:path "R.x.b"      :short :x.s2.b}
     {:path "R.z"        :short :z}])

  (matcho/match
   (translate {:differential {:element els-sl}})
   {:elements
    {:z {:short :z}
     :x {:short :x,
         :slicing
         {:slices
          {:s1
           {:match {:a "s1"}
            :elements
            {:a {:short :x.s1.a},
             :b {:short :x.s1.b,
                 :slicing {:slices
                           {:z1 {:match {:f {:ff {:code "z1"}}}
                                 :elements {:f {:short :x.s1.b.z1.f, :elements {:ff {:short :x.s1.b.z1.ff}}}}},
                            :z2 {:match {:f {:ff {:code "z2"}}}
                                 :elements {:f {:short :x.s1.b.z2.f, :elements {:ff {:short :x.s1.b.z2.ff}}}}}}}}}},
           :s2 {:match {:a "s2"}
                :elements {:a {:short :x.s2.a}, :b {:short :x.s2.b}}}}}}}})

  (def union-els
    [{:path "R.value[x]"  :type [{:code "string"} {:code "Quantity"}]}
     {:path "R.valueQuantity.unit" :short "unit" }])

  (matcho/match
   (translate {:differential {:element union-els}})
   {:elements
    {:value         {:choices ["valueString" "valueQuantity"]},
     :valueString   {:type {:code "string"} :choiceOf "value"},
     :valueQuantity {:type {:code "Quantity"} :choiceOf "value"
                     :elements {:unit {:short "unit"}}},}})

  ;; (clojure.pprint/pprint (translate els))

  (translate
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

  )

;; migrate to [{:el :k}] pathes

