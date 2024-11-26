(ns fhirschema.terminology
  (:require [svs.pg :as pg]
            [svs.gcp]
            [cheshire.core]
            [clj-yaml.core]
            [clojure.string :as str]
            [utils.ndjson]))


(defn dump [x]
  (spit "/tmp/dump.yaml" (clj-yaml.core/generate-string x)))

(defn get-value [p]
  (->> p
       (reduce (fn [acc [k v]]
                 (if (str/starts-with? (name k) "value") v acc))
               {})))

(defn get-props [c]
  (->> (:property c)
       (reduce (fn [c p]
                 (assoc c (keyword (:code p)) (get-value p)))
               {})))

(defn tiny-concepts [acc hm cs & [parent parents]]
  (->> cs
       (reduce (fn [acc c]
                 (let [acc (conj acc (cond-> (merge (get-props c) (select-keys c [:code :display]))
                                       parent (assoc hm parent :parent parent :parents parents)))]
                   (if (:concept c)
                     (tiny-concepts acc hm (:concept c) (:code c) (conj (or parents []) (:code c)))
                     acc)))
               acc)))

(defn tiny-codesystem [cs]
  (let [hm (or (:hierarchyMeaning cs) "is-a")
        concepts (tiny-concepts [] hm (:concept cs))]
    (->
     (select-keys cs [:description :content :url :name :valueSet :version])
     (cond-> (:experimental cs) (assoc :experimental true)
             (not (= (:status cs) "active")) (assoc :status (:status cs)))
     (assoc :concept concepts :numberOfConcepts (count concepts)))))

(defn vs-pattern [vs]
  (cond-> {}
    (get-in vs [:compose :include])
    (assoc :include (into #{} (mapv keys (get-in vs [:compose :include]))))
    (get-in vs [:compose :exclude])
    (assoc :exclude (into #{} (mapv keys (get-in vs [:compose :exclude]))))))

(defn resolve-i [xs idx]
  (->> xs
       (reduce (fn [acc {s :system c :concept f :filter v :valueSet}]
                 (cond-> acc
                   s (assoc :s "*")
                   (and s (not c) (nil? (get idx s))) (assoc :! "ERROR")
                   c (assoc :c "*")
                   f (assoc :f "*")
                   v (assoc :v "*")))
               {})))

(defn vs-pattern-with-resolve [vs idx]
  (cond-> {}
    (get-in vs [:compose :include])
    (assoc :i (resolve-i (get-in vs [:compose :include])  idx))
    (get-in vs [:compose :exclude])
    (assoc :e (resolve-i (get-in vs [:compose :exclude]) idx))))

(defn tiny-vs [vs & [truncate]]
  (let [acc  (->> (:include (:compose vs))
                  (reduce
                   (fn [acc {s :system v :valueSet f :filter cs :concept :as item}]
                     (cond
                       cs (update acc :include-concept into (->> cs (mapv (fn [c] (assoc (select-keys c [:code :display]) :system s)))))
                       f  (update acc :include-filter conj item)
                       s  (update acc :include-system conj s)
                       v (update acc :include-valueset into v)))
                   {}))
        acc  (->> (:exclude (:compose vs))
                  (reduce
                   (fn [acc {s :system v :valueSet f :filter cs :concept :as item}]
                     (cond
                       cs (update acc :exclude-concept into (->> cs (mapv (fn [c] (assoc c :system s)))))
                       f  (update acc :exclude-filter conj item)
                       s  (update acc :exclude-system conj s)
                       v (update acc  :exclude-valueset into v)))
                   acc))
        tvs  (merge (select-keys vs [:url :name :version]) acc)]
    (cond-> tvs 
      (and truncate (:include-concept tvs)) (update :include-concept (fn [xs] (take 10 xs))))))

(comment

  (def storage (gcp/mk-storage))

  (def blb (gcp/get-blob storage gcp/get-ig-bucket "p/hl7.fhir.r4.core/4.0.1/codesystem.ndjson.gz"))

  (def codesystems (gcp/read-ndjson-blob blb))

  (count codesystems)
  (mapv :url codesystems)
  (mapv (fn [x]
          [(:url x) (:version x) (:content x)]
          ) codesystems)


  (->> codesystems
       (reduce (fn [acc {c :content}]
                 (update acc c (fn [x] (inc (or x 0)))))
               {}))

  (->> codesystems
       (filterv (fn [x] (not (= "complete" (:content x)))))
       (mapv (fn [x] [(:url x) (:content x)])))

  (->> codesystems
       (filterv (fn [x] (= "complete" (:content x))))
       (mapv (fn [x] [(:url x) (:name x)]))
       (sort-by first))


  (->> codesystems
       (filterv (fn [x] (= "complete" (:content x))))
       (mapv (fn [x] [(:url x) (count (:concept x))]))
       (sort-by second)
       (reverse))

  (def cs-idx
    (->> codesystems
         (reduce (fn [acc x] (assoc acc (:url x) x)) {})))

  (count cs-idx)

  (keys cs-idx)

  (get cs-idx "http://dicom.nema.org/resources/ontology/DCM")

  (def complete-cs (->> codesystems (filterv (fn [x] (= "complete" (:content x))))))

  (count complete-cs)

  (def complete-cs-idx (reduce (fn [acc x] (assoc acc (:url x) x)) {} complete-cs))

  (count complete-cs-idx)

  (->> codesystems
       (filterv (fn [x] (= "complete" (:content x))))
       (mapv (fn [x] (-> x (select-keys [:name :content]) (assoc :numberOfConcepts (count (:concept x))))))
       (sort-by :numberOfConcepts)
       (reverse)
       clj-yaml.core/generate-string
       (spit "/tmp/cs.yaml"))

  (dump (mapv tiny-codesystem codesystems))


  (dump (take 10 codesystems))


  (def vs-blb (gcp/get-blob storage gcp/get-ig-bucket "p/hl7.fhir.r4.core/4.0.1/valueset.ndjson.gz"))
  (def valuesets (gcp/read-ndjson-blob vs-blb))

  (dump
   (->> valuesets
        (mapv (fn [x]
                (when (contains? sandbox/required-bindings (:url x))
                  (select-keys x [:url :compose]))))
        (filter identity)))

  (vs-pattern-with-resolve (nth valuesets 1) complete-cs-idx) 

  (dump
   (->> valuesets
        (reduce (fn [acc x] (update acc (vs-pattern x) (fn [x] (inc (or x 0))))) {})
        (mapv (fn [[pat cnt]] {:pattern (cheshire.core/generate-string pat) :count cnt}))
        (sort-by :count)
        (reverse)))

  ;; - {pattern: '{"include":[["system"]]}', count: 1072}
  ;; - {pattern: '{"include":[["system","filter"]]}', count: 95}
  ;; - {pattern: '{"include":[["system","concept"]]}', count: 86}
  ;; - {pattern: '{"include":[["system","filter"]],"exclude":[["system","concept"]]}', count: 22}
  ;; - {pattern: '{"include":[["system","concept"],["system"]]}', count: 8}
  ;; - {pattern: '{"include":[["system","filter"],["system"]]}', count: 5}
  ;; - {pattern: '{"include":[["system","version","concept"]]}', count: 5}
  ;; - {pattern: '{"include":[["valueSet"]]}', count: 5}
  ;; - {pattern: '{"include":[["system","concept"],["system","filter"]]}', count: 4}
  ;; - {pattern: '{"include":[["extension","system","concept"]]}', count: 2}
  ;; - {pattern: '{"include":[["system","concept"],["valueSet"],["system"]]}', count: 2}
  ;; - {pattern: '{"include":[["valueSet"],["system"]]}', count: 2}
  ;; - {pattern: '{"include":[["system","concept"],["valueSet"]]}', count: 2}
  ;; - {pattern: '{"include":[["system","concept"],["system","filter"],["system"]]}', count: 2}
  ;; - {pattern: '{"include":[["system","concept"],["system","filter"],["system"]],"exclude":[["system","concept"]]}', count: 1}
  ;; - {pattern: '{"include":[["system","version"]]}', count: 1}
  ;; - {pattern: '{"include":[["system","filter"],["valueSet"]]}', count: 1}
  ;; - {pattern: '{"include":[["system","filter"],["valueSet"],["system"]]}', count: 1}


  (dump
   (->> valuesets
        (reduce (fn [acc x] (update acc (vs-pattern-with-resolve x complete-cs-idx) (fn [x] (inc (or x 0))))) {})
        (mapv (fn [[pat cnt]] {:pattern (pr-str pat) :count cnt}))
        (sort-by :count)
        (reverse)))

  ;; - {pattern: '{:i {:s "*"}}', count: 1034}
  ;; - {pattern: '{:i {:s "*", :c "*"}}', count: 101}
  ;; - {pattern: '{:i {:s "*", :f "*"}}', count: 51}
  ;; - {pattern: '{:i {:s "*", :! "ERROR", :f "*"}}', count: 49}
  ;; - {pattern: '{:i {:s "*", :! "ERROR"}}', count: 39}
  ;; - {pattern: '{:i {:s "*", :f "*"}, :e {:s "*", :c "*"}}', count: 21}
  ;; - {pattern: '{:i {:v "*"}}', count: 5}
  ;; - {pattern: '{:i {:s "*", :! "ERROR", :f "*", :c "*"}}', count: 4}
  ;; - {pattern: '{:i {:s "*", :v "*", :c "*"}}', count: 4}
  ;; - {pattern: '{:i {:s "*", :v "*"}}', count: 2}
  ;; - {pattern: '{:i {:s "*", :c "*", :f "*"}}', count: 2}
  ;; - {pattern: '{:i {:v "*", :s "*", :! "ERROR", :f "*"}}', count: 2}
  ;; - {pattern: '{:i {:s "*", :! "ERROR", :f "*"}, :e {:s "*", :c "*"}}', count: 1}
  ;; - {pattern: '{:i {:s "*", :f "*", :c "*"}, :e {:s "*", :c "*"}}', count: 1}


  (def lbld (->> valuesets
                 (mapv (fn [x] (assoc x :pattern (vs-pattern-with-resolve x complete-cs-idx))))))

  ;; include only system/vs to :include-system
  ;; exclude only system/vs to :exclude-system
  ;; include concept to include-concept
  ;; exclude concept to exclude-concept
  ;; filter to include-filter

  (dump
   (->> lbld
        (filter (fn [x] (= (:pattern x) {:i {:s "*", :! "ERROR", :f "*"}})))
        (mapv tiny-vs)))

  (dump
   (->> lbld
        (filter (fn [x] (= (:pattern x) {:i {:s "*", :! "ERROR"}})))
        (mapv tiny-vs)))


  (def required-vs
    (->> valuesets
         (mapv (fn [x] (when (contains? sandbox/resource-binding-set (:url x)) (select-keys x [:url :compose]))))
         (filter identity)
         (mapv (fn [x] (assoc x :pattern (vs-pattern-with-resolve x complete-cs-idx))))))

  (dump
   (->> required-vs
        (reduce (fn [acc x] (update acc (:pattern x) (fn [x] (inc (or x 0))))) {})
        (mapv (fn [[pat cnt]] {:pattern (pr-str pat) :count cnt}))
        (sort-by :count)
        (reverse)))

  (dump
   (->> lbld
        (filter (fn [x] (= (:pattern x) {:i {:s "*", :! "ERROR"}})))
        (mapv tiny-vs)))


  (dump
   (->> required-vs
        #_(filter (fn [x] (= (:pattern x) {:i {:s "*", :c "*"}})))
        (mapv #(tiny-vs % true))))

  ;; 1. classify cs in internal vs external
  ;;   * load into database
  ;; 2. classify vs into internal vs external
  ;;   * load into database
  ;;   * by resolving cs and vs and checking internal or external
  ;; 3. expand internal vs
  ;;   * calcualte number of concepts
  ;;   * in database with all deps
  ;; 4. we can generate internal package cs and vs which could be used to enrich fhir schema
  ;; 5. we can build efficient index for validation (prefix index)

  )


