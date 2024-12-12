(ns far.tx.concepts
  (:require [system]
            [pg.repo]
            [cheshire.core]
            [clj-yaml.core]
            [utils.uuid]
            [clojure.string :as str]))


(defn get-value [p]
  (->> p (reduce (fn [acc [k v]] (if (str/starts-with? (name k) "value") v acc)) {})))

(defn concept-properties [prs]
  (->> prs
       (reduce (fn [c p] (assoc c (keyword (:code p)) (get-value p))) {})))

(defn escape-string [s]
  (str/replace s #"(\n|\t|\r)" " "))

(escape-string "a\nb")
(escape-string "a\tb")

(defn collect-concepts [acc path concepts hierarchy-attr base]
  (->> concepts
       (reduce
        (fn [acc {cs :concept prs :property code :code :as concept}]
          (let [acc (conj acc
                          (-> (dissoc concept :concept)
                              (merge base)
                              (assoc
                               :id (utils.uuid/uuid (:package_name base) (:package_version base) (:system base) (:version base) code)
                               :logical_id (utils.uuid/uuid (:system base) code)
                               )
                              (cond->
                                  (seq path) (assoc hierarchy-attr path)
                                  prs (assoc :property (concept-properties prs))
                                  (:description concept) (update :description escape-string)
                                  (:display concept) (update :display escape-string))))]
            (if cs
              (collect-concepts acc (conj path code) cs hierarchy-attr base)
              acc)))
        acc)))

(defn make-concept-base [cs]
  {:canonical_id (:id cs)
   :canonical_url (:url cs)
   :canonical_version (or (:version cs) (:package_version cs))
   :package_id (:package_id cs)
   :package_name (:package_name cs)
   :package_version (:package_version cs)})

(defn extract-concept-cs [cs]
  (assert (:content cs))
  (when (= "complete" (:content cs))
    (let [hierarchy-attr (keyword (or (:hierarchyMeaning cs) "is-a"))
          base (assoc (make-concept-base cs) :system (:url cs))]
      (collect-concepts [] [] (:concept cs) hierarchy-attr base))))

(defn extract-concept-compose [base compose-type concepts]
  (->> concepts
       (mapcat
        (fn [ {system :system  concept :concept :as incl}]
          (->> concept
               (mapv (fn [{code :code :as concept}]
                       (-> (merge base concept)
                           (assoc :system system
                                  :compose compose-type
                                  :id (utils.uuid/uuid (:package_name base) (:package_version base) (:canonical_url base) system (:version base) code)
                                  :logical_id (utils.uuid/uuid system code))))))))))

(defn extract-concept-vs [cs]
  (let [base (make-concept-base cs)]
    (concat
     (->> (get-in cs [:compose :include])
          (extract-concept-compose base "include"))
     (->> (get-in cs [:compose :exclude])
          (extract-concept-compose base "exclude")))))

(defn extract-concepts [cs-or-vs]
  (case (:resourceType cs-or-vs)
    "CodeSystem" (extract-concept-cs cs-or-vs)
    "ValueSet"   (extract-concept-vs cs-or-vs)))

(defn load-concepts-from-cs [context]
  (time
   (pg.repo/load
    context {:table "concept"}
    (fn [insert]
      (pg.repo/fetch
       context {:table "codesystem" :fetch-size 1000}
       (fn [cs]
         (println (:url cs))
         (->> (extract-concept-cs cs)
              (mapv (fn [c] (insert c))))))))))

(defn load-concepts-from-vs [context]
  (let [concept-ids (atom {})]
    (time
     (pg.repo/load
      context {:table "concept"}
      (fn [insert]
        (pg.repo/fetch
         context {:table "valueset" :fetch-size 1000}
         (fn [cs]
           (->> (extract-concept-vs cs)
                (mapv (fn [c]
                        (if-let [cc (get @concept-ids (:id c))]
                          (do (println (:url cs) (:package_name cs) (:package_version cs) (:code c)))
                          (do (swap! concept-ids assoc (:id c) c)
                              (insert c)))))))))))))
