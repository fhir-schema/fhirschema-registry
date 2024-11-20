(ns ndjson
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [cheshire.core])
  (:import [java.util.zip GZIPInputStream GZIPOutputStream]
           [java.nio.channels Channels]
           [java.io BufferedReader InputStreamReader
            BufferedWriter OutputStreamWriter]))

(defn write-ndjson-gz [filename cb]
  (with-open [writer (-> filename (io/output-stream) (GZIPOutputStream.) (io/writer))]
    (let [write (fn [^String s] (.write writer s) (.write writer "\n"))]
      (cb write))))
