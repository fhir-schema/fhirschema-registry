(ns far.package.canonical-deps
  (:require [clojure.string :as str]))

(defn parse-canonical [url]
  (let [parts (str/split url #"\|" 2)]
    {:url (first parts)
     :version (second parts)}))


(defmulti extract-deps (fn [{rt :resourceType}] rt))

(defn deps-from-el-target-profile [el tp]
  (->> (:targetProfile tp)
       (map (fn [trgtpr] (merge {:type "reference" :path (:id el) :resource_type "StructureDefinition"} (parse-canonical trgtpr))))))

(defn deps-from-el-profile [el tp]
  (->> (:profile tp)
       (map (fn [pr] (merge {:type "type-profile" :path (:id el) :resource_type "StructureDefinition"} (parse-canonical pr))))))

(defn deps-from-type-code [el tp]
  [(if (str/starts-with?  (:code tp) "http")
     (merge {:type :type :path (:id el) :resource_type "StructureDefinition"} (parse-canonical (:code tp)))
     {:type "type" :path (:id el)
      :resource_type "StructureDefinition"
      :url (str "http://hl7.org/fhir/StructureDefinition/" (:code tp))})])

(defn deps-from-type [el]
  (->> (:type el)
       (mapcat (fn [tp]
                 (concat
                  (deps-from-type-code el tp)
                  (deps-from-el-target-profile el tp)
                  (deps-from-el-profile el tp))))))

(defn deps-from-additional-bindings [el]
  (->> (get-in el [:binding :additional])
       (mapcat (fn [b]
                 (when-let [vs (:valueSet b)]
                   [(merge {:type "additional-binding"  :path (:id el) :resource_type "ValueSet"} (parse-canonical vs))])))))

(defn deps-from-binding [el]
  (when (and (:binding el) (:valueSet (:binding el)))
    (concat [(merge {:type "binding"  :path (:id el)
                     :resource_type "ValueSet"
                     :binding_strength (:strength (:binding el))}
                    (parse-canonical (:valueSet (:binding el))))]
            (deps-from-additional-bindings el))))

(defn basedef-deps [res]
  (when (:baseDefinition res)
    [(merge {:type "baseDefinition" :resource_type "StructureDefinition"} (parse-canonical (:baseDefinition res)))]))

(defn dep-base [res]
  {:definition_id (:id res)
   :definition (:url res)
   :definition_resource_type (:resourceType res)
   :definition_version (:version res)
   :package_name (:package_name res)
   :package_version (:package_version res)
   :package_id (:package_id res)})

(defmethod extract-deps "StructureDefinition" [res]
  (let [base (dep-base res)]
    (->> (get-in res [:differential :element])
         (mapcat (fn [el] (concat (deps-from-type el) (deps-from-binding el))))
         (concat (basedef-deps res))
         (map (fn [dep] (merge base dep)))
         (map #(dissoc % :path))
         (group-by (fn [x] (select-keys x [:definition_id :type :url])))
         (vals)
         (map (fn [xs] (first xs)))
         (remove (fn [x] (str/starts-with? (:url x) "http://hl7.org/fhirpath")))
         (into #{}))))

(defn extract-compose [base type xs]
  (->> xs
       (mapcat
        (fn [{s :system vs :valueSet f :filter cs :concept :as item}]
          (when-not cs
            (cond
              s [(merge base {:type (str type "-cs") :resource_type "CodeSystem"} (parse-canonical s))]
              vs  (->> vs (map (fn [v] (merge base {:type (str type "-vs") :resource_type "ValueSet"} (parse-canonical v)))))))))))

(defmethod extract-deps "ValueSet" [res]
  (let [base (dep-base res)]
    (into #{}
          (concat
           (->> (get-in res [:compose :include])
                (extract-compose base "vs/include"))
           (->> (get-in res [:compose :exclude])
                (extract-compose base "vs/exclude"))))))

(defmethod extract-deps :default [res] (assert false (pr-str res)))
