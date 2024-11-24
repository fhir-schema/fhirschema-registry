(ns gcp
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
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

(defn mk-storage []
  (.getService (StorageOptions/getDefaultInstance)))

(defn mk-bucket [^Storage storage]
  (.get storage "fhir-schema-registry"
        (into-array Storage$BucketGetOption [])))


(defn gz-stream [^InputStream str]
  (GZIPInputStream. str))

(defn blob-stream [^Blob blob {gz :gzip} read-fn]
  (with-open [input-stream (Channels/newInputStream (.reader blob (into-array Blob$BlobSourceOption [])))]
    (if gz
      (with-open [gz-stream (gz-stream input-stream)]
        (read-fn gz-stream))
      (read-fn input-stream))))

(defn read-ndjson-blob [^Blob blob]
  (with-open [reader (-> (blob-stream {:gzip true} blob) InputStreamReader. BufferedReader.)]
    (loop [line (.readLine reader)
           line-number 0
           acc []]
      (if line
        (let [res (cheshire.core/parse-string line keyword)]
          (recur (.readLine reader) (inc line-number) (conj acc res)))
        acc))))

(defn process-ndjson-blob [^Blob blob process-fn]
  (with-open [input-stream (Channels/newInputStream (.reader blob (into-array Blob$BlobSourceOption [])))
              gz-stream (GZIPInputStream. input-stream)
              reader (-> gz-stream InputStreamReader. BufferedReader.)]
    (loop [line (.readLine reader)
           line-number 0]
      (when line
        (let [res (cheshire.core/parse-string line keyword)]
          (process-fn res line-number)
          (recur (.readLine reader) (inc line-number)))))))

(defn read-blob [^Blob blob process-fn]
  (with-open [input-stream (Channels/newInputStream (.reader blob (into-array Blob$BlobSourceOption [])))
              gz-stream (GZIPInputStream. input-stream)
              reader (-> gz-stream InputStreamReader. BufferedReader.)]
    (loop [line (.readLine reader)
           line-number 0
           acc {}]
      (if line
        (let [res (cheshire.core/parse-string line keyword)
              acc (process-fn acc res line-number)]
          (recur (.readLine reader) (inc line-number) acc))
        acc))))

(defn blob-content [^Blob blob & [{json :json}]]
  (let [res (String. (.getContent blob (into-array Blob$BlobSourceOption [])))]
    (if json
      (cheshire.core/parse-string res keyword)
      res)))

(defn blob-ndjson-writer [storage bucket file]
  (let [bid (BlobId/of bucket file)
        binfo (BlobInfo/newBuilder bid)
        ch (.writer ^Storage storage ^BlobInfo (.build binfo) (into-array Storage$BlobWriteOption []))
        os (Channels/newOutputStream ch)
        outz (GZIPOutputStream. os)]
    (BufferedWriter. (OutputStreamWriter. outz))))

(defn write-blob [storage bucket file cb]
  (with-open
    [os (blob-ndjson-writer storage bucket file)
     outz (GZIPOutputStream. os)
     w (BufferedWriter. (OutputStreamWriter. outz))]
    (cb w)))

(def get-ig-bucket "fs.get-ig.org")

(defonce default-storage (atom nil))
(defn get-default-storage []
  (or @default-storage (reset! default-storage (mk-storage))))

(defn get-blob
  ([file] (get-blob (get-default-storage) get-ig-bucket file))
  ([bucket file] (get-blob (get-default-storage) bucket file))
  ([storage bucket file]
   (let [bid (BlobId/of bucket file)]
     (.get storage bid (into-array Storage$BlobGetOption [])))))

(defn package-file [package version file]
  (let [b (get-blob (str "p/" package "/" version "/" file))]
    (assert b (str "no file " b))
    b))

(defn text-blob [storage bucket file content]
  (let [bid (BlobId/of bucket file)
        binfo (BlobInfo/newBuilder bid)]
    (with-open [ch (.writer ^Storage storage ^BlobInfo (.build binfo) (into-array Storage$BlobWriteOption []))
                os (Channels/newOutputStream ch)
                w (BufferedWriter. (OutputStreamWriter. os))]
      (.write w content))))

(defn write-ndjson-gz [filename cb]
  (with-open [writer (-> filename
                         (io/output-stream)
                         (GZIPOutputStream.)
                         (io/writer))]
    (cb writer)))

(set! *warn-on-reflection* false)

(defn objects [^String bucket]
  (let [svc (.getService (StorageOptions/getDefaultInstance))
        bucket (.get svc bucket ^"[Lcom.google.cloud.storage.Storage$BucketGetOption;" (into-array Storage$BucketGetOption []))
        page (.list bucket (into-array Storage$BlobListOption []))]
    (loop [page page acc (into [] (.getValues page))]
      (println ".")
      (if-let [next-page (.getNextPage page)]
        (recur next-page (into acc (.getValues page)))
        (into acc (.getValues page))))))


(comment
  (def storage (.getService (StorageOptions/getDefaultInstance)))

  (def pkgs
    (time
     (->> (objects "fs.get-ig.org")
          (filterv (fn [x] (str/ends-with? (.getName x) "package.json")))
          (pmap (fn [x] (blob-content x {:json true})))
          (into []))))


  (defn get-blob [storage bucket file]
    (let [bid (BlobId/of bucket (str/replace file #"^/" ""))]
      (.get storage bid (into-array Storage$BlobGetOption []))))

  (def blb (get-blob storage get-ig-bucket "p/hl7.fhir.r4.core/4.0.1/structuredefinition.ndjson.gz"))

  (def res (read-ndjson-blob blb))

  (first res)

  (count pkgs)
  (mapv (fn [x] [(:name x) (:version x)]) pkgs)

  (def ^Bucket bucket (.get storage get-ig-bucket (into-array Storage$BucketGetOption [])))


  (def page (.list bucket (into-array Storage$BlobListOption [])))

  (def pkg
    (time
     (loop [page page acc (into [] (.getValues page))]
       (println ".")
       (if-let [next-page (.getNextPage page)]
         (recur next-page (into acc (.getValues page)))
         (into acc (.getValues page))))))

  pkg


  (count pkg)

  (str/split (:name (bean (first pkg))) #"/")

  (let [blb (second pkg)]
    (read-blob blb (fn [acc x i]
                     (let [acc (if (= i 0)
                                 {:url (:name x) :version (:version x)}
                                 acc)
                           res (cond
                                 (= i 0)
                                 (assoc x :resourceType "Package"
                                        :meta {:package acc} :url (:name x)
                                        :dependencies (reduce (fn [acc dpe]
                                                                (let [[pkg ver] (str/split dpe #"#")]
                                                                  (assoc acc pkg ver)))
                                                              {} (:dependencies x)))
                                 (:resourceType x)
                                 (-> x
                                     (dissoc  :package-meta)
                                     (assoc-in [:meta :package] acc))

                                 (and (not (:resourceType x)) (:package-meta x))
                                 (-> x
                                     (assoc :resourceType "FHIRSchema")
                                     (dissoc :package-meta)
                                     (assoc-in [:meta :package] acc))
                                 :else nil)]
                       (when res
                         (println res))
                       acc))))



  (write-ndjson-gz
   "resources.ndjson.gz"
   (fn [aw]
     (doseq [blb pkg]
       (let [[_ pkg file] (str/split (.getName blb) #"/")]
         (when (= file "package.ndjson.gz")
           (let [[pkg-name version] (str/split pkg #"#")]
             (println pkg-name version)
             (write-blob
              storage "fhir-packages" (str "v1/" pkg-name "/v" version ".ndjson.gz")
              (fn [w]
                (read-blob blb (fn [acc x i]
                                 (let [acc (if (= i 0)
                                             {:url (:name x) :version (:version x)}
                                             acc)
                                       res (cond
                                             (= i 0)
                                             (assoc x :resourceType "Package"
                                                    :meta {:package acc}
                                                    :url (:name x)
                                                    :dependencies (reduce (fn [acc dpe]
                                                                            (let [[pkg ver] (str/split dpe #"#")]
                                                                              (assoc acc pkg ver)))
                                                                          {} (:dependencies x)))
                                             (:resourceType x)
                                             (-> x
                                                 (dissoc  :package-meta)
                                                 (assoc-in [:meta :package] acc))

                                             (and (not (:resourceType x)) (:package-meta x))
                                             (-> x
                                                 (assoc :resourceType "FHIRSchema")
                                                 (dissoc :package-meta)
                                                 (assoc-in [:meta :package] acc))
                                             :else nil)]
                                   (when res
                                     (.write w (cheshire.core/generate-string res))
                                     (.write w "\n")
                                     (.write aw (cheshire.core/generate-string res))
                                     (.write aw "\n"))
                                   acc)))))))))))


  (do

    (def bid (BlobId/of "fhir-packages" "test.pkg/v0.0.5.ndjson.gz"))

    (def binfo (BlobInfo/newBuilder bid))

    ;; (.setContentEncoding binfo "gzip")
    ;; (.setContentType binfo "application/gzip")

    (.build binfo)
    (def ch (.writer ^Storage storage ^BlobInfo (.build binfo) (into-array Storage$BlobWriteOption [])))

    (def os (Channels/newOutputStream ch))
    (def outz (GZIPOutputStream. os))
    (def w (BufferedWriter. (OutputStreamWriter. outz)))

    (dotimes [i 100]
      (.write w (cheshire.core/generate-string {:resourceType "hello" :i i}))
      (.write w "\n"))

    (.close w)
    (.close outz)
    (.close os)
    )


  ;; writer.write(line);
  ;; writer.write("\n");


  )
