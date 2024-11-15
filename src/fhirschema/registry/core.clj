(ns fhirschema.registry.core
  (:require [clojure.string :as str]
            [cheshire.core])
  (:import [com.google.cloud.storage StorageOptions
            BlobInfo BlobId
            Storage Bucket Blob Storage$BucketGetOption
            Storage$BlobListOption
            Blob$BlobSourceOption
            Storage$BlobWriteOption]
           [java.util.zip GZIPInputStream GZIPOutputStream]
           [java.nio.channels Channels]
           [java.io BufferedReader InputStreamReader
            BufferedWriter OutputStreamWriter]))

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

(defn write-blob [storage bucket file cb]
  (let [bid (BlobId/of bucket file)
        binfo (BlobInfo/newBuilder bid)
        ch (.writer ^Storage storage ^BlobInfo (.build binfo) (into-array Storage$BlobWriteOption []))]
    (with-open
      [os (Channels/newOutputStream ch)
       outz (GZIPOutputStream. os)
       w (BufferedWriter. (OutputStreamWriter. outz))]
      (cb w))))


(comment
  (def storage (.getService (StorageOptions/getDefaultInstance)))

  (def ^Bucket bucket (.get storage "fhir-schema-registry" (into-array Storage$BucketGetOption [])))


  (def page (.list bucket (into-array Storage$BlobListOption [])))

  (def pkg
    (time
     (loop [page page acc (into [] (.getValues page))]
       (println ".")
       (if-let [next-page (.getNextPage page)]
         (recur next-page (into acc (.getValues page)))
         (into acc (.getValues page))))))

  (.getNextPage page)

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
                                          (assoc x :resourceType "Package" :meta {:package acc} :url (:name x))
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
                                  (.write w "\n"))
                                acc)))))))))


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

