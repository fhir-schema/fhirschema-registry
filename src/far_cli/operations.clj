(ns far-cli.operations
  (:require
   [cheshire.core :as json]
   [clojure.string :as str]
   [clojure.pprint :as pprint]
   [far.package]
   [pg]
   [fhir.schema.transpiler]
   [fhir.schema.typeschema]
   [system]))


(defn parse-connection-string [conn-str]
  (let [[_ user password host port db]
        (re-find #"postgresql://(\w+):(\w+)@(\w+):(\d+)/(\w+)" conn-str)]
    {:host host :port port :database db :user user :password password}))

(comment
  (parse-connection-string "postgresql://postgres:postgres@localhost:5437/fhirpackages")
  (parse-connection-string "postgresql://fhirpackages:secret@localhost:5437/fhirpackages"))


(defn show-package-info [format pkg-info]
  (case format
    :text (str/trim (with-out-str (pprint/pprint pkg-info)))
    :json (json/generate-string pkg-info)))

(defn show-package-list-item [format item]
  (case format
    :text (str (:id item)
               (when (:title item) (str " -- " (:title item))))
    :json (json/generate-string item)))


(defn print-cli-errors [summary errors]
  (println "CLI errors:")
  (doall (map #(println (str "  " %)) errors))
  (println)
  (println "Help")
  (println summary))
