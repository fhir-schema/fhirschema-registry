(ns fhirschema.transpiler
  (:require [clojure.string :as str]))

(defn required-element?
  [element]
  (= 1 (:min element)))

(defn coerce-element-max-cardinality
  [^String value]
  (if (= "*" value)
    Integer/MAX_VALUE
    (when (string? value)
      (parse-long value))))

(defn array-element?
  [element]
  (or (= "*" (:max element))
      (and (:min element) (>= (:min element) 2))
      (and (:max element) (>= (coerce-element-max-cardinality (:max element)) 2))))


(defn parse-path [p el]
  (let [path (->> (str/split p #"\.")
                  (rest)
                  (mapv (fn [x] {:el (keyword x)})))]
    (update path (dec (count path)) merge (select-keys el [:slicing :sliceName]))))

;; calculate common path
(defn get-common-path [p1 p2]
  (loop [[px & pxs] p1
         [py & pxy] p2
         cp []]
    (if (or (nil? px) (nil? py)
            (not (= (:el px) (:el py))))
      cp
      (recur pxs pxy (conj cp px)))))

;; copy slices from previous path
(defn enrich-path [p1 p2]
  (loop [[px & pxs] p1
         [py & pxy :as p2] p2
         cp []]
    (if (or (nil? px) (nil? py) (not (= (:el px) (:el py))))
      (if py (into cp p2) (into cp pxy))
      (recur pxs pxy (conj cp (merge (select-keys px [:slicing :sliceName]) py ))))))

;; TBD: tidy
(defn calculate-exits [prev-c cp-c prev-path new-path]
  (loop [i prev-c exits []]
    (if (= i cp-c)
      (if (< 0 i)
        (let [pi (nth prev-path (dec i))
              ni (nth new-path (dec i))]
          (if (and (:sliceName pi) (:sliceName ni) (not (= (:sliceName pi) (:sliceName ni))))
            (conj exits {:type :exit-slice :sliceName (:sliceName pi) :slicing (:slicing pi)})
            exits))
        exits)
      (recur (dec i)
             (let [pi (nth prev-path (dec i))]
               (-> exits
                   (cond-> (:sliceName pi) (conj {:type :exit-slice :sliceName (:sliceName pi) :slicing (:slicing pi)}))
                   (conj {:type :exit :el (:el pi)})))))))

;; TBD: tidy
(defn calculate-enters [cp-c new-c exits prev-path new-path]
  (loop [i cp-c  enters exits]
    (if (= i new-c)
      (let [pi (nth prev-path (dec i) nil)
            ni (nth new-path (dec i) nil)]
        (if (and (:sliceName ni) (not (= (:sliceName pi) (:sliceName ni))))
          (conj enters {:type :enter-slice :sliceName (:sliceName ni)})
          enters))
      (let [pi (nth new-path i)]
        (recur (inc i)
               (cond-> (conj enters {:type :enter :el (:el (nth new-path i))})
                 (:sliceName pi) (conj {:type :enter-slice :sliceName (:sliceName pi)})))))))

(defn calculate-actions [prev-path new-path]
  (let [prev-length          (count prev-path)
        new-length           (count new-path)
        common-path-length   (count (get-common-path prev-path new-path))
        exits                (calculate-exits prev-length common-path-length prev-path new-path)
        enters               (calculate-enters common-path-length new-length exits prev-path new-path)]
    enters))

(defn pop-and-update
  "peek from stack and update value on top of stack"
  [stack update-fn]
  (let [peek-v (peek stack)
        prev-stack (pop stack)
        last-v (peek prev-stack)
        prev-prev-stack (pop prev-stack)]
    (conj prev-prev-stack (update-fn last-v peek-v))))

;; todo handle $this
;; todo honor type
;; if no discriminator - add :dynamic instruction
;; todo support reslicing
(defn build-slice [{ sn :sliceName  sl :slicing} last-v peek-v]
  (let [match (->> (:discriminator sl)
                   (reduce (fn [match {tp :type p :path}]
                             (let [pattern-p (mapv keyword (str/split p #"\."))
                                   pp (into [:elements] (interpose :elements pattern-p))
                                   v  (get-in peek-v (conj pp :pattern))]
                               (assoc-in match pattern-p (:value v)))) {}))]
    (assoc-in last-v [:slicing :slices (keyword sn)] (assoc peek-v :match match))))

(defn apply-actions [value-stack actions value]
  (->> actions
       (reduce
        (fn [stack {tp :type el :el sn :sliceName  sl :slicing :as item}]
          ;; (println :? tp (or el sn)) (doseq [x (reverse stack)] (println "  " :* x))
          (case tp
            :enter
            (conj stack value)

            :enter-slice
            (conj stack value)

            :exit
            (pop-and-update stack (fn [last-v peek-v] (assoc-in last-v [:elements el] peek-v)))

           ;; FOCUS
            :exit-slice
            (pop-and-update stack (fn [last-v peek-v] (build-slice item last-v peek-v)))))
        value-stack)))


;; alorythm
;; enrich path from previous
;; calcluate enter/exit from path
;; execute enter/exit logic while building final structure
(def EMPTY_PATH [])

(defn choice? [e]
  (str/ends-with? (:path e) "[x]"))

(defn capitalize [s]
  (if (seq s) (str (str/upper-case (subs s 0 1)) (subs s 1)) s))

(defn union-elements [{p :path :as e}]
  (let [prefix (str/replace p #"\[x\]" "")
        fs-prefix (second (str/split prefix #"\." 2))]
    (->> (:type e)
         (mapv (fn [{c :code :as tp}]
                 (assoc e :path (str prefix (capitalize c)) :type tp :choiceOf fs-prefix)))
         (into [(-> (assoc e :path prefix)
                    (dissoc :type)
                    (assoc :choices (->> (:type e) (mapv (fn [{c :code}] (str fs-prefix (capitalize c)))))))]))))

(defn process-patterns [e]
  (->> e
       (reduce
        (fn [acc [k v]]
          (cond
            (str/starts-with? (name k) "pattern")
            (assoc acc :pattern {:type (str/replace (name k) #"^pattern" "") :value v})

            (str/starts-with? (name k) "fixed")
            (assoc acc :pattern {:type (str/replace (name k) #"^fixed" "") :value v})

            :else (assoc acc k v)))
        {})))

(defn process-element [e]
  (-> (dissoc e :path :slicing :sliceName :id :min :max)
      (cond->
          (array-element? e)    (assoc :array true) ;; todo min/max
          (required-element? e) (assoc :required true))
      (process-patterns)))

;; TODO: context {elements for elements, elements for resoruce}
;; TODO: extensions
(defn translate [structure-definition]
  (let [res (select-keys structure-definition [:derivation :name :type :kind :abstract])]
    (loop [value-stack [res]
           prev-path EMPTY_PATH
           els (->> (get-in structure-definition [:differential :element])
                    (filterv (fn [{p :path}] (str/includes? p "."))))]
      (if (empty? els)
        (let [actions (calculate-actions prev-path EMPTY_PATH)
              new-value-stack (apply-actions value-stack actions {})]
          (first new-value-stack))
        (let [e (first els)]
          (if (choice? e)
            (recur value-stack prev-path (into (union-elements e) (rest els)))
            (let [new-path        (enrich-path prev-path (parse-path (:path e) e))
                  actions         (calculate-actions prev-path new-path)
                  new-value-stack (apply-actions value-stack actions (process-element e))]
              (recur new-value-stack new-path (rest els)))))))))
