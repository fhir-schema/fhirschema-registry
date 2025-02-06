(ns far.package.index
  (:require
   [cheshire.core :as json]
   [clojure.java.io :as io]
   [clojure.set]
   [org.httpkit.client :as http])
  (:import
   (java.util.zip GZIPInputStream)))

(def ^:const default-fuzz-index-search-suffix
  "https://storage.googleapis.com/fhir-schema-registry/1.0.0/fuzzy-search-index.ndjson.gz")

(defn- get-fuzz-search-index []
  ;; source: sansara/far.s3.gcp
  ;;
  ;; NOTE: better to use http://packages2.fhir.org API, but it's looks slow and
  ;; require auth.
  ;;
  ;; TODO: add disk cache
  (let [stream (-> default-fuzz-index-search-suffix
                   (http/get)
                   (deref)
                   :body
                   (GZIPInputStream.)
                   (io/reader))]
    (loop [acc {}]
      (if-let [l (.readLine stream)]
        (recur (let [data (json/parse-string l keyword)
                     id (str (:name data) "#" (:version data))]
                 (assoc acc id (assoc data :id id))))
        acc))))

(defn list-packages []
  (->> (get-fuzz-search-index)
       (vals)
       (map (fn [m]
              (assert (= (:id m) (str (:name m) "#" (:version m)))
                      "id should be equal to name#version")
              {:name    (:name m)
               :title   (:title m)
               :version (:version m)
               :id      (:id m)}))
       (sort-by :name)
       (doall)))

(comment
  (get-fuzz-search-index)
  (list-packages))
