(ns fhir-pkg
  (:require
   [system]
   [cheshire.core]
   [clojure.string :as str]
   [clojure.java.shell :refer [sh]]
   [gcs]
   [utils.ndjson])
  (:import
   [java.net URL]
   [java.io File FileOutputStream InputStream BufferedOutputStream]
   [org.apache.commons.compress.archivers.tar TarArchiveEntry TarArchiveInputStream]
   [org.apache.commons.compress.compressors.gzip GzipCompressorInputStream]))


(def SIMPLIFIER_REPO "https://packages.simplifier.net")
(def HL7_REPO "https://packages2.fhir.org/packages")


(defn read-tar [^String url cb]
  (let [^URL input-stream-url (URL. url)]
    (with-open [^InputStream input-stream (.openStream input-stream-url)
                ^GzipCompressorInputStream gzip-compressor-input-stream (GzipCompressorInputStream. input-stream)
                ^TarArchiveInputStream tar-archive-input-stream (TarArchiveInputStream. gzip-compressor-input-stream)]
      (loop []
        (when-let [^TarArchiveEntry entry (.getNextTarEntry tar-archive-input-stream)]
          (let [^String nm (str/replace (.getName entry) #"package/" "")
                read-fn (fn [& [json]]
                          (let [content (byte-array (.getSize entry))]
                            (.read tar-archive-input-stream content)
                            (if json
                              (cheshire.core/parse-string (String. content) keyword)
                              (String. content))))]
            (cb nm read-fn))
          (recur))))))

(defn reduce-tar [^String url cb]
  (let [^URL input-stream-url (URL. url)]
    (with-open [^InputStream input-stream (.openStream input-stream-url)
                ^GzipCompressorInputStream gzip-compressor-input-stream (GzipCompressorInputStream. input-stream)
                ^TarArchiveInputStream tar-archive-input-stream (TarArchiveInputStream. gzip-compressor-input-stream)]
      (loop [acc {}]
        (if-let [^TarArchiveEntry entry (.getNextTarEntry tar-archive-input-stream)]
          (let [^String nm (str/replace (.getName entry) #"package/" "")
                read-fn (fn [& [json]]
                          (let [content (byte-array (.getSize entry))]
                            (.read tar-archive-input-stream content)
                            (if json
                              (cheshire.core/parse-string (String. content) keyword)
                              (String. content))))]
            (recur (cb acc nm read-fn)))
          acc)))))

(system/defmanifest
  {:description "work with fhir packages"
   :config {:registry {:type "string" :required true :default SIMPLIFIER_REPO}}})

(defn- bash [cmd] (-> (sh "bash" "-c" cmd)))

(defn- from-json [x]
  (cheshire.core/parse-string x keyword))

(defn get-registry [context]
  (system/get-system-state context [:registry]))

(defn pkg-info
  "get package information pkg could be just name a.b.c or versioned name a.b.c@1.0.0"
  [context pkg]
  (let [cmd (str "npm --registry " (get-registry context) " view --json " pkg)
        out (-> (bash cmd) :out)]
    (system/info context ::pkg-info cmd)
    (try
      (from-json out)
      (catch Exception e
        (throw (Exception. out))))))

(defn pkg-deps
  [context pkg]
  (let [cmd (str "npm --registry " (get-registry context) " view dependencies --json " pkg)
        out (-> (bash cmd) :out)]
    (system/info context ::pkg-info cmd)
    (try
      (from-json out)
      (catch Exception e
        (throw (Exception. out))))))

(defn read-package
  [context pkg on-file]
  (system/info context ::read-package (:url pkg))
  (read-tar (:url pkg) (fn [nm read-resource] (on-file nm read-resource))))

(defn reduce-package
  [context pkg on-file]
  (system/info context ::read-package (:url pkg))
  (reduce-tar (:url pkg) (fn [acc nm read-resource] (on-file acc nm read-resource))))


(system/defstart [context config] config)

(system/defstop [context state])

(comment
  (def context (system/start-system {:services ["fhir-pkg"]}))

  context

  (get-registry context)
  (pkg-info context "hl7.fhir.fi.base")

  (def pkgi (pkg-info context "hl7.fhir.us.core@3.0.1"))

  pkgi

  ;; TODO: reduce-package

  (read-package context pkgi (fn [nm read-file]
                              ;; (println nm)
                              (when (= ".index.json" nm)
                                (println (read-file)))
                              #_(when (str/starts-with? nm "Search")
                                (let [res (read-file true)]
                                  (println (:name res) (:expression res))))))


  )
