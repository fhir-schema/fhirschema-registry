(ns far.package
  (:require [system]
            [pg]
            [pg.repo]
            [cheshire.core]
            [utils.uuid]
            [far.package.canonical-deps :refer [extract-deps]]
            [far.package.repos :as repos]
            [far.package.loader :as loader]
            [clojure.string :as str]))


;; should accept package-name and package-version
(defn load-package
  "load package recursively"
  [context package-name package-version]
  (loader/load-package context package-name package-version))

(defn pkg-info [context package-name]
  (loader/pkg-info context package-name))

(defn package-bundle [pkgi]
  (assert (and (map? pkgi) (:name pkgi)))
  (loader/read-package pkgi))

(defn truncate [context]
  (repos/truncate context))

(defn drop-tables [context]
  (repos/drop-tables context))


(defn package-deps [context pkv]
  (loader/resolve-all-deps context pkv))

(defn package-deps-print [context pkv]
  (loader/print-deps-tree
   (loader/deps-tree context pkv)))

(defn package-by-name [context name]
  (pg.repo/read context {:table "package_version" :match {:name name}}))


(defn packages [context {match :match limit :limit :as opts}]
  (pg.repo/select context (merge {:table "package_version" :order-by [:pg/list :name :version]} opts)))

(defn package [context package-name package-version]
  (pg.repo/read context {:table "package_version" :match {:name package-name :version package-version}}))

(defn canonicals [context {match :match limit :limit :as opts}]
  (pg.repo/select context (merge {:table "canonical" :order-by [:pg/list :url :version]} opts)))


(defn resolve-canonical-deps [context deps-idx can-id path & [details]]
  (if (contains? deps-idx can-id)
    deps-idx
    (let [deps-idx (assoc deps-idx can-id (merge {:path path} details))]
      (->> (pg.repo/select context {:table "canonical_deps" :match {:definition_id can-id}})
           (reduce (fn [deps-idx {dep-id :dep_id :as dep}]
                     (if (or (contains? deps-idx dep-id) (nil? dep-id)
                             ;; skip references
                             (= ":reference" (:type dep)))
                       deps-idx
                       (resolve-canonical-deps context deps-idx dep-id (conj path [(:definition dep) (:type dep)]) dep)))
                   deps-idx)))))

(defn canonical-deps [context canonical & [max-deps]]
  (resolve-canonical-deps context {} (:id canonical) [] {}))

(system/defmanifest
  {:description "create tables and save packages into this tables"
   :deps ["pg.repo"]})


(system/defstart
  [context config]
  (repos/migrate context))

(system/defstop
  [context state])
