(ns far.package.canonical-deps-test
  (:require [far.package.canonical-deps :as sut]
            [clojure.test :as t]
            [matcho.core :as matcho]))

#_(map (fn [x] (dissoc x :short :mapping :definition :isSummary :requirements :definition :isModifier :meaningWhenMissing :alias
                     :constraint
                     :isModifierReason :isModifier :comment)) els)

(def pt-sd
  {:fhirVersion "4.0.1",
   :name "Patient",
   :abstract false,
   :type "Patient",
   :resourceType "StructureDefinition",
   :package_name "hl7.fhir.r4.core",
   :package_version "4.0.1",
   :status "active",
   :id #uuid "f239fae9-de76-5054-9cf6-69f12a4ba986",
   :package_id #uuid "d58c1dcd-a628-50ac-94de-757eee16627e",
   :kind "resource",
   :derivation "specialization",
   :url "http://hl7.org/fhir/StructureDefinition/Patient",
   :version "4.0.1",
   :baseDefinition "http://hl7.org/fhir/StructureDefinition/DomainResource"
   :differential
   {:element
    [{:id "Patient", :max "*", :min 0, :path "Patient"}
     {:path "Patient.identifier", :min 0, :type [{:code "Identifier"}], :max "*", :id "Patient.identifier"}
     {:path "Patient.active", :min 0, :type [{:code "boolean"}], :max "1", :id "Patient.active"}
     {:path "Patient.name", :min 0, :type [{:code "HumanName"}], :max "*", :id "Patient.name"}
     {:path "Patient.telecom", :min 0, :type [{:code "ContactPoint"}], :max "*", :id "Patient.telecom"}
     {:path "Patient.gender", :min 0, :type [{:code "code"}], :max "1", :id "Patient.gender"
      :binding {:strength "required", :valueSet "http://hl7.org/fhir/ValueSet/administrative-gender|4.0.1"}}
     {:path "Patient.birthDate", :min 0, :type [{:code "date"}], :max "1", :id "Patient.birthDate"}
     {:path "Patient.deceased[x]", :min 0, :type [{:code "boolean"} {:code "dateTime"}], :max "1", :id "Patient.deceased[x]"}
     {:path "Patient.address", :min 0, :type [{:code "Address"}], :max "*", :id "Patient.address"}
     {:path "Patient.maritalStatus", :min 0, :type [{:code "CodeableConcept"}], :max "1", :id "Patient.maritalStatus"
      :binding {:strength "extensible", :valueSet "http://hl7.org/fhir/ValueSet/marital-status",},}
     {:path "Patient.multipleBirth[x]", :min 0, :type [{:code "boolean"} {:code "integer"}], :max "1", :id "Patient.multipleBirth[x]"}
     {:path "Patient.photo", :min 0, :type [{:code "Attachment"}], :max "*", :id "Patient.photo"}
     {:path "Patient.contact", :min 0, :type [{:code "BackboneElement"}] :max "*", :id "Patient.contact"}
     {:path "Patient.contact.relationship", :min 0, :type [{:code "CodeableConcept"}], :max "*", :id "Patient.contact.relationship"
      :binding {:strength "extensible", :valueSet "http://hl7.org/fhir/ValueSet/patient-contactrelationship",},}
     {:path "Patient.contact.name", :min 0, :type [{:code "HumanName"}], :max "1", :id "Patient.contact.name"}
     {:path "Patient.contact.telecom", :min 0, :type [{:code "ContactPoint"}], :max "*", :id "Patient.contact.telecom"}
     {:path "Patient.contact.address", :min 0, :type [{:code "Address"}], :max "1", :id "Patient.contact.address"}
     {:path "Patient.contact.gender", :min 0, :type [{:code "code"}],
      :binding {:strength "required", :valueSet "http://hl7.org/fhir/ValueSet/administrative-gender|4.0.1",},
      :max "1", :id "Patient.contact.gender"}
     {:path "Patient.contact.organization", :min 0, :type [{:code "Reference", :targetProfile ["http://hl7.org/fhir/StructureDefinition/Organization"]}], :max "1", :id "Patient.contact.organization",}
     {:id "Patient.contact.period", :max "1", :min 0, :path "Patient.contact.period", :type [{:code "Period"}]}
     {:path "Patient.communication", :min 0, :type [{:code "BackboneElement"}], :max "*", :id "Patient.communication"}
     {:path "Patient.communication.language", :min 1, :type [{:code "CodeableConcept"}],
      :binding
      {:strength "preferred", :valueSet "http://hl7.org/fhir/ValueSet/languages",
       :extension
       [{:url "http://hl7.org/fhir/StructureDefinition/elementdefinition-maxValueSet", :valueCanonical "http://hl7.org/fhir/ValueSet/all-languages"}
        {:url "http://hl7.org/fhir/StructureDefinition/elementdefinition-bindingName", :valueString "Language"}
        {:url "http://hl7.org/fhir/StructureDefinition/elementdefinition-isCommonBinding", :valueBoolean true}]}
      :max "1",
      :id "Patient.communication.language"}
     {:path "Patient.communication.preferred", :min 0, :type [{:code "boolean"}], :max "1", :id "Patient.communication.preferred"}
     {:path "Patient.generalPractitioner", :min 0,
      :type [{:code "Reference", :targetProfile ["http://hl7.org/fhir/StructureDefinition/Organization" "http://hl7.org/fhir/StructureDefinition/Practitioner" "http://hl7.org/fhir/StructureDefinition/PractitionerRole"]}],
      :max "*",
      :id "Patient.generalPractitioner"}
     {:path "Patient.managingOrganization", :min 0, :type [{:code "Reference", :targetProfile ["http://hl7.org/fhir/StructureDefinition/Organization"]}], :max "1", :id "Patient.managingOrganization"}
     {:path "Patient.link", :min 0, :type [{:code "BackboneElement"}], :max "*", :id "Patient.link"}
     {:path "Patient.link.other", :min 1,
      :type [{:code "Reference",
              :extension [{:url "http://hl7.org/fhir/StructureDefinition/structuredefinition-hierarchy", :valueBoolean false}],
              :targetProfile ["http://hl7.org/fhir/StructureDefinition/Patient" "http://hl7.org/fhir/StructureDefinition/RelatedPerson"]}],
      :max "1", :id "Patient.link.other"}
     {:path "Patient.link.type", :min 1, :type [{:code "code"}], :max "1", :id "Patient.link.type"
      :binding
      {:strength "required",
       :valueSet "http://hl7.org/fhir/ValueSet/link-type|4.0.1",
       :extension
       [{:url "http://hl7.org/fhir/StructureDefinition/elementdefinition-bindingName", :valueString "LinkType"}],},}]}
   })

(t/deftest test-sd

  (def deps (sut/extract-deps pt-sd))

  (t/is (every? :resource_type deps))

  (matcho/match
   (->> (mapv :url deps) (sort))
   ["http://hl7.org/fhir/StructureDefinition/Address"
    "http://hl7.org/fhir/StructureDefinition/Attachment"
    "http://hl7.org/fhir/StructureDefinition/BackboneElement"
    "http://hl7.org/fhir/StructureDefinition/CodeableConcept"
    "http://hl7.org/fhir/StructureDefinition/ContactPoint"
    "http://hl7.org/fhir/StructureDefinition/DomainResource"
    "http://hl7.org/fhir/StructureDefinition/HumanName"
    "http://hl7.org/fhir/StructureDefinition/Identifier"
    "http://hl7.org/fhir/StructureDefinition/Organization"
    "http://hl7.org/fhir/StructureDefinition/Patient"
    "http://hl7.org/fhir/StructureDefinition/Period"
    "http://hl7.org/fhir/StructureDefinition/Practitioner"
    "http://hl7.org/fhir/StructureDefinition/PractitionerRole"
    "http://hl7.org/fhir/StructureDefinition/Reference"
    "http://hl7.org/fhir/StructureDefinition/RelatedPerson"
    "http://hl7.org/fhir/StructureDefinition/boolean"
    "http://hl7.org/fhir/StructureDefinition/code"
    "http://hl7.org/fhir/StructureDefinition/date"
    "http://hl7.org/fhir/StructureDefinition/dateTime"
    "http://hl7.org/fhir/StructureDefinition/integer"
    "http://hl7.org/fhir/ValueSet/administrative-gender"
    "http://hl7.org/fhir/ValueSet/languages"
    "http://hl7.org/fhir/ValueSet/link-type"
    "http://hl7.org/fhir/ValueSet/marital-status"
    "http://hl7.org/fhir/ValueSet/patient-contactrelationship"])


  (def duplicates-sd
    {:description "Record of delivery of what is supplied.",
     :package_name "hl7.fhir.r5.core",
     :date "2023-03-26T15:21:02+11:00",
     :derivation "specialization",
     :meta
     {:file "StructureDefinition-SupplyDelivery.json",
      :lastUpdated "2023-03-26T15:21:02.749+11:00"},
     :publisher "Health Level Seven International (Orders and Observations)",
     :fhirVersion "5.0.0",
     :name "SupplyDelivery",
     :abstract false,
     :type "SupplyDelivery",
     :experimental false,
     :resourceType "StructureDefinition",
     :package_version "5.0.0",
     :status "draft",
     :id #uuid "f3547414-1e4a-5277-8be1-6e52dbf64131",
     :kind "resource",
     :url "http://hl7.org/fhir/StructureDefinition/SupplyDelivery",
     :package_id #uuid "de285a0f-b21c-5227-82be-946deea4d3d5",
     :version "5.0.0",
     :differential
     {:element

      [
       {:path "SupplyDelivery.type",
        :min 0,
        :type [{:code "CodeableConcept"}],
        :binding {:strength "required",
                  :valueSet "http://hl7.org/fhir/ValueSet/supplydelivery-supplyitemtype|5.0.0",
                  :extension [{:url "http://hl7.org/fhir/StructureDefinition/elementdefinition-bindingName", :valueString "SupplyDeliverySupplyItemType"}],
         :description "The type of supply dispense."},
        :max "1"}
       {:path "SupplyDelivery.suppliedItem.item[x]",
        :min 0,
        :type [{:code "CodeableConcept"} {:code "Reference", :targetProfile ["http://hl7.org/fhir/StructureDefinition/Medication" "http://hl7.org/fhir/StructureDefinition/Substance" "http://hl7.org/fhir/StructureDefinition/Device" "http://hl7.org/fhir/StructureDefinition/BiologicallyDerivedProduct" "http://hl7.org/fhir/StructureDefinition/NutritionProduct" "http://hl7.org/fhir/StructureDefinition/InventoryItem"]}],
        :binding {:strength "example",
                  :valueSet "http://hl7.org/fhir/ValueSet/supplydelivery-supplyitemtype",
                  :extension [{:url "http://hl7.org/fhir/StructureDefinition/elementdefinition-bindingName",
                               :valueString "SupplyDeliverySupplyItemType"}]},
        :max "1"}]},
     :resource_id "SupplyDelivery",
     :baseDefinition "http://hl7.org/fhir/StructureDefinition/DomainResource"})

  (t/is (nil? (->> (group-by :url (sut/extract-deps duplicates-sd)) (vals) (filter (fn [[_ xs]] (> (count xs) 1))) first)))

  )
