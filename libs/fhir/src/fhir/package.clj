(ns fhir.package
  (:require
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

(defn get-registry [] SIMPLIFIER_REPO)

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


(defn- bash [cmd] (-> (sh "bash" "-c" cmd)))

(defn- from-json [x]
  (cheshire.core/parse-string x keyword))

(defn pkg-info
  "get package information pkg could be just name a.b.c or versioned name a.b.c@1.0.0"
  [pkg & [registry]]
  (assert (string? pkg))
  (let [cmd (str "npm --registry " (or registry (get-registry)) " view --json " pkg)
        out (-> (bash cmd) :out)]
    (try
      (from-json out)
      (catch Exception e
        (throw (Exception. out))))))

(defn pkg-deps
  [pkg & [registry]]
  (let [cmd (str "npm --registry " (or registry (get-registry)) " view dependencies --json " pkg)
        out (-> (bash cmd) :out)]
    (try
      (from-json out)
      (catch Exception e
        (throw (Exception. out))))))

(defn read-package
  [pkg on-file]
  (read-tar (:url pkg) (fn [nm read-resource] (on-file nm read-resource))))

(defn reduce-package
  [pkg on-file]
  (reduce-tar (:url pkg) (fn [acc nm read-resource] (on-file acc nm read-resource))))
