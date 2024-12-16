(ns far.schema
  (:require
   [fhir.schema.transpiler :as transpiler]
   [clojure.string :as str]
   [pg.repo]))

;; process schema

(defn type-url [tp]
  (if (str/starts-with? tp "http")
    tp
    (str "http://hl7.org/fhir/StructureDefinition/" tp)))

(defn unversion [x]
  (when x (first (str/split x #"\|"))))

(defn enrich-node [schema deps-idx]
  (cond-> schema
    (:base schema) (assoc :base_ref (if-let [dep (get-in deps-idx [(:base schema)])]
                                      {:id (:dep_id dep) :resourceType (:resource_type dep)}
                                      (println :missed (:base schema))))

    (:type schema) (assoc :type_ref (if-let [dep (get-in deps-idx [(type-url (:type schema))])]
                                      {:id (:dep_id dep) :resourceType (:resource_type dep)}
                                      (println :missed (type-url (:type schema)) (keys deps-idx))))

    (:binding schema) (update :binding (fn [b]
                                         (if-let [dep (get-in deps-idx [(unversion (:valueSet b))])]
                                           (assoc b :valueSet_ref {:id (:dep_id dep) :resourceType (:resource_type dep)})
                                           b)))

    (:extensions schema) (assoc :extensions (->> (:extensions schema)
                                                 (reduce (fn [acc [k v]]
                                                           (assoc acc k (cond-> (enrich-node v deps-idx)
                                                                          (:url v)
                                                                          (assoc :extension_ref
                                                                                 (if-let [dep (get-in deps-idx [(:url v)])]
                                                                                             {:id (:dep_id dep) :resourceType (:resource_type dep)}
                                                                                             (println :missed (:url v) (keys deps-idx))))

                                                                          ))) {})))

    (:elements schema) (assoc :elements (->> (:elements schema)
                                             (reduce (fn [acc [k v]] (assoc acc k (enrich-node v deps-idx))) {})))))

(defn get-deps-idx [context schema]
  (->>
   (pg.repo/select context {:table "canonical_deps" :match {:definition_id (:id schema)}})
   (reduce (fn [acc {url :url :as dep}]
             (when-let [edep (get acc url)]
               (when (not (= (:dep_id dep) (:dep_id edep)))
                 (throw (Exception. (str "ups")))))
             (assoc acc url dep))
           {})))

(defn enrich [context schema]
  (let [deps-idx (get-deps-idx context schema)]
    (enrich-node schema deps-idx)))

(comment
  (def context far/context)

  (def id "3204c076-36c3-5f21-9508-244e319170f1")

  (def id "f7bd9266-0711-56e2-ba49-6c4009fb3802")

  (def sd (pg.repo/read context {:table "structuredefinition" :match {:id id}}))

  (def sch (transpiler/translate sd))

  (get-deps-idx context sch)


  (enrich context sch)

  )
