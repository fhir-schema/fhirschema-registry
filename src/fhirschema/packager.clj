(ns fhirschema.packager
  (:require
   [clojure.java.io :as io]
   [cheshire.core]
   [clojure.string :as str]
   [clojure.java.shell :refer [sh]]
   [pjson.core :as pjson]
   [svs.gcp]
   [utils.ndjson])
  (:import
   [java.nio.file Path Paths Files]
   [java.net URL]
   [java.io File FileOutputStream InputStream BufferedOutputStream]
   [org.apache.commons.compress.archivers.tar TarArchiveEntry TarArchiveInputStream]
   [org.apache.commons.compress.compressors.gzip GzipCompressorInputStream]))


(def reg "https://packages.simplifier.net")

(def reg "https://packages2.fhir.org/packages")

(defn bash [cmd]
  (-> (sh "bash" "-c" cmd)))

(defn pkg-info [pkg]
  (-> (bash (str "npm --registry " reg " view --json " pkg))
      :out
      (cheshire.core/parse-string keyword)))

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

(defn clean-up [res]
  (let [res (dissoc res :text)]
    (cond (= "StructureDefinition" (:resourceType res))
          (dissoc res :snapshot)
          :else res)))

(defn dump-package [storage pkg]
  (println :dump (:name pkg) (:version pkg))
  (let [bucket "fs.get-ig.org"
        prefix (str "p/" (:name pkg) "/" (:version pkg) "/")
        stats (atom {})
        errors (atom [])
        url-wr (gcp/blob-ndjson-writer storage bucket (str prefix "urls.ndjson.gz"))
        writers (atom {"url" url-wr})
        get-writer (fn [rt]
                     (let [rt (str/lower-case rt)]
                       (if-let [wr (get @writers rt)]
                         wr
                         (let [wr (gcp/blob-ndjson-writer storage bucket (str prefix rt ".ndjson.gz"))]
                           ;; (println :open rt)
                           (swap! writers assoc rt wr)
                           wr))))]
    (try
      (read-tar (:url pkg)
                (fn [nm read-resource]
                  (when (and (not (str/includes? nm "/")) (not (= ".index.json" nm)) (str/ends-with? nm ".json"))
                    (try
                      (let [res (read-resource true)]
                        (when (:url res)
                          (let [res (assoc-in res [:meta :package] {:name (:name pkg) :version (:version pkg) :file nm})
                                res (if (= nm "package.json") (assoc res :resourceType "Package") res)
                                res-json (cheshire.core/generate-string (clean-up res))]
                            (swap! stats update (:resourceType res) (fn [x] (inc (or x 0))))
                            (when (:url res)
                              (.write url-wr (cheshire.core/generate-string {:url (:url res)}))
                              (.write url-wr "\n"))
                            (if (= nm "package.json")
                              (gcp/text-blob storage bucket (str prefix "package.json") (cheshire.core/generate-string (merge pkg res)))
                              (let [rtwrt (get-writer (:resourceType res))]
                                (.write rtwrt res-json)
                                (.write rtwrt "\n"))))))
                      (catch Exception e
                        (swap! errors conj (str nm "-" (.getMessage e)))
                        (println :error (:name pkg) (:version pkg) nm  (subs (.getMessage e) 0 50)))))))
      (gcp/text-blob storage bucket (str prefix "stats.json") (cheshire.core/generate-string @stats))
      (when (seq @errors)
        (gcp/text-blob storage bucket (str prefix "errors.txt")  (str/join @errors "\n")))
      (doseq [[rt wr] @writers]
        ;; (println :close rt)
        (.close wr))
      (catch Exception e
        (gcp/text-blob storage bucket (str prefix "errors.txt") (str (.getMessage e) "\n" (str/join @errors "\n")))
        (doseq [[rt wr] @writers]
          ;; (println :close rt)
          (.close wr))
        (throw e)))))


(comment

  (def packages (-> (slurp "packages.txt")
                    (str/split #"\n")
                    (->> (mapv str/trim))))

  packages

  (count packages)


  (def storage (gcp/mk-storage))

  (->> (for [pkg-name packages] (when-let [pkg (pkg-info pkg-name)] pkg))
       (filter identity)
       (mapv (fn [pkg]
               (println (:name pkg) (:url pkg) (:version pkg))
               (doseq [v (:versions pkg)]
                 (when-let [pkgv (pkg-info (str (:name pkg) "@" v))]
                   (println (str (:name pkg) "@" v) :> (:name pkgv) (:version pkgv) (:url pkgv))
                   (try
                     (time (dump-package storage pkgv))
                     (catch Exception e (println :ERROR (str (:name pkgv) "@" (:version pkgv)) (.getMessage e))))))
               pkg))
       (into []))

  (into [] resolved-pkgs)

  (-> (bash "npm --registry https://packages.simplifier.net view hl7.fhir.us.core versions")
      :out
      (str/split #",")
      (->> (mapv str/trim)))

  (-> (bash "npm --registry https://packages.simplifier.net view hl7.fhir.us.core")
      :out
      (str/split #"\n"))


  (pkg-info "hl7.fhir.fi.base")


  (dump-package storage (pkg-info "hl7.fhir.fi.base"))

  (dump-package storage (pkg-info "hl7.fhir.be.lab@1.0.0"))

  (read-tar (get-in (pkg-info "hl7.fhir.r4.core") [:url])
            (fn [nm _] (println nm)))

  (def pkgs (pkg-info "hl7.fhir.us.core"))
  (def pkgs (pkg-info "hl7.fhir.us.sdoh-clinicalcare"))

  (def pkgs (pkg-info "hl7.fhir.r4.core"))


  pkgs


  (pkg-info "hl7.fhir.r4.core@4.0.1")

  (dump-package storage (pkg-info "hl7.fhir.be.lab@1.0.0"))

  ;; process package
  ;; mk ndjson put into s3
  ;; hl7.fhir.r4.core
  ;;   /@4.0.1
  ;;     /package.json
  ;;     /canonicals.ndjson.gz
  ;;     /structredefinition.ndjson.gz
  ;;        /extensions.ndjson.gz
  ;;        /profiles.ndjson.gz
  ;;        /logicals.ndjson.gz
  ;;     /searchparam.ndjson.gz
  ;;     /valueset.ndjson.gz
  ;;     /codesystem.ndjson.gz
  ;; update
  ;;     /+fhirschema.ndjson.gz
  ;;     /+concepts.ndjson.gz

  ;; filter **/*valueset.ndjson.gz -> database

  ;;  hl7.fhir.be.lab@1.0.0
  ;;  hl7.fhir.au.core@0.2.1-preview

  )
