(ns svs.gcp
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [utils.ndjson :as ndjson]
            [system]
            [cheshire.core])
  (:import [com.google.cloud.storage StorageOptions
            BlobInfo BlobId
            Storage Bucket Blob Storage$BucketGetOption
            Blob$BlobSourceOption
            Storage$BlobListOption
            Storage$BlobGetOption
            Blob$BlobSourceOption
            Storage$BlobWriteOption]
           [java.util.zip GZIPInputStream GZIPOutputStream]
           [java.nio.channels Channels]
           [java.io BufferedReader InputStream InputStreamReader
            BufferedWriter OutputStreamWriter]))


(defn gz-stream [^InputStream str]
  (GZIPInputStream. str))

(defn mk-service [cfg]
  (.getService (StorageOptions/getDefaultInstance)))

(def DEFAULT_BUCKET "fs.get-ig.org")

(defn start [system & [cfg]]
  (system/start-service system
   {:svc (mk-service cfg)
    :bucket (or (:default/bucket cfg) DEFAULT_BUCKET)}))

(defn stop [system]
  (println :gcp/stop))

(defn get-service [ctx]
  (:svc (system/get-state-from-ctx ctx)))

(defn get-bucket-name [ctx]
  (:bucket (system/get-state-from-ctx ctx)))

;; focus
(defn get-bucket [ctx & [bucket-name]]
  (.get (get-service ctx)
        ^String (or bucket-name (get-bucket-name ctx))
        ^"[Lcom.google.cloud.storage.Storage$BucketGetOption;" (into-array Storage$BucketGetOption [])))

(defn package-file-name [package version file]
  (str "p/" package "/" version "/" file))

(defn get-blob
  ([ctx file] (get-blob ctx (get-bucket-name ctx) file))
  ([ctx bucket file]
   (let [bid (BlobId/of bucket file)
         blb (.get (get-service ctx) bid (into-array Storage$BlobGetOption []))]
     (assert blb (str "FILE NOT EXISTS:" bucket "/" file))
     blb)))

(defn objects
  ([ctx] (objects ctx (get-bucket-name ctx)))
  ([ctx ^String bucket]
   (println ctx bucket)
   (let [bucket (get-bucket ctx bucket)
         page   (.list bucket (into-array Storage$BlobListOption []))]
     (loop [page page acc (into [] (.getValues page))]
       (if-let [next-page (.getNextPage page)]
         (recur next-page (into acc (.getValues page)))
         (into acc (.getValues page)))))))


(defn blob-content [^Blob blob & [{json :json}]]
  (let [res (String. (.getContent blob (into-array Blob$BlobSourceOption [])))]
    (if json
      (cheshire.core/parse-string res keyword)
      res)))

(defn blob-input-stream [^Blob blob & [{gz :gzip}]]
  (let [input-stream (Channels/newInputStream (.reader blob (into-array Blob$BlobSourceOption [])))]
    (if gz
      (gz-stream input-stream)
      input-stream)))

(defn blob-process-ndjson [^Blob blob process-fn]
  (with-open [s (blob-input-stream blob)]
    (ndjson/process-stream s process-fn)))

(defn file-output-stream [ctx ^String file  & [{gz :gzip}]]
  (let [bid (BlobId/of (get-bucket-name ctx) file)
        binfo (BlobInfo/newBuilder bid)
        ch (.writer ^Storage (get-service ctx) ^BlobInfo (.build binfo) (into-array Storage$BlobWriteOption []))
        os (Channels/newOutputStream ch)]
    (if gz (GZIPOutputStream. os) os)))

(set! *warn-on-reflection* false)

;; (defn write-blob [storage bucket file cb]
;;   (with-open
;;     [os (blob-ndjson-writer storage bucket file)
;;      outz (GZIPOutputStream. os)
;;      w (BufferedWriter. (OutputStreamWriter. outz))]
;;     (cb w)))



;; (defn package-file [package version file]
;;   (let [b (get-blob (str "p/" package "/" version "/" file))]
;;     (assert b (str "no file " b))
;;     b))


;; (defn text-blob [storage bucket file content]
;;   (let [bid (BlobId/of bucket file)
;;         binfo (BlobInfo/newBuilder bid)]
;;     (with-open [ch (.writer ^Storage storage ^BlobInfo (.build binfo) (into-array Storage$BlobWriteOption []))
;;                 os (Channels/newOutputStream ch)
;;                 w (BufferedWriter. (OutputStreamWriter. os))]
;;       (.write w content))))

;; (defn write-ndjson-gz [filename cb]
;;   (with-open [writer (-> filename
;;                          (io/output-stream)
;;                          (GZIPOutputStream.)
;;                          (io/writer))]
;;     (cb writer)))


(comment

  (def system (system/new-system {}))

  (start system {})

  (def ctx (system/new-context system))
  ctx

  (get-service ctx)

  (get-bucket ctx)

  (get-bucket-name ctx)

  (count (objects ctx))

  (package-file-name "hl7.fhir.r4.core" "4.0.1" "package.json")

  (def pkg-blob (get-blob ctx (package-file-name "hl7.fhir.r4.core" "4.0.1" "package.json")))

  (blob-content pkg-blob {:json true})

  (def pkgs
    (time
     (->> (objects ctx)
      (filterv (fn [x] (str/ends-with? (.getName x) "package.json")))
      (pmap (fn [x] (blob-content x {:json true})))
      (into []))))

  (count pkgs)
  (first pkgs)

  (def sd-blob (get-blob ctx (package-file-name "hl7.fhir.r4.core" "4.0.1" "structuredefinition.ndjson.gz")))

  sd-blob

  ;; focus
  (with-open [s (blob-input-stream sd-blob)]
    (ndjson/process-stream s (fn [res ln] (println ln (:url res)))))

  (blob-process-ndjson sd-blob (fn [res ln] (println ln (:url res))))

  ;; (with-open [os (file-output-stream ctx "_test/json")]
  ;;   (ndjson/write-stream os
  ;;    (fn [write]
  ;;      (doseq [i (range 100)]
  ;;        (write {:i i})))))

  ;; (write-content ctx "_test/json" {:hello "ok"})
  ;; (write-ndjson ctx "_test/test.ndjson.gz" (fn [write]))



  )
