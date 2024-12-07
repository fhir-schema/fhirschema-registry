(ns utils.uuid
  (:require [clojure.string :as str]
            [clj-uuid]))

(def default-ns #uuid"6ba7b811-9dad-11d1-80b4-00c04fd430c8")
;; TODO uuid 5

(defn uuid [& parts]
  (clj-uuid/v5 default-ns (str/join "/" parts)))

(defn cannonical-id [cs]
  (uuid (:package_name cs) (:package_version cs) (:url cs) (:version cs)))

(defn canonical-id [cs]
  (uuid (:package_name cs) (:package_version cs) (:url cs) (:version cs)))



