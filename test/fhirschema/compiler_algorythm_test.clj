(ns fhirschema.compiler-algorythm-test
  (:require [clojure.test :refer [deftest testing]]
            [clojure.string :as str]
            [matcho.core :as matcho]))

(defn parse-path [p el]
  (let [path (->> (str/split p #"\.")
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

(defn union-elements [e]
  (let [prefix (str/replace (:path e) #"\[x\]" "")]
    (->> (:type e)
         (mapv (fn [{c :code :as tp}] (assoc e :path (str prefix (capitalize c)) :type tp)))
         (into [(assoc e :path prefix)]))))

(defn process-element [e]
  (->> (dissoc e :path :slicing :sliceName)
       (reduce
        (fn [acc [k v]]
          (if (str/starts-with? (name k) "pattern")
            (assoc acc :pattern {:type (str/replace (name k) #"^pattern" "") :value v})
            (assoc acc k v)))
        {})))

(process-element {:patternString "a"})

(defn build [els]
  (loop [value-stack [{}]
         prev-path EMPTY_PATH
         els els]
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
            (recur new-value-stack new-path (rest els))))))))



(deftest test-algorythm
  (matcho/match
   (parse-path "a" {})
   [{:el :a}])

  (matcho/match
   (parse-path "a.b" {})
   [{:el :a} {:el :b}])


  (matcho/match
   (calculate-actions (parse-path "a" {}) (parse-path "b" {}))
   [{:type :exit, :el :a} {:type :enter, :el :b}])

  (matcho/match
   (calculate-actions (parse-path "a.b.c" {}) [])
   [{:type :exit, :el :c} {:type :exit, :el :b} {:type :exit, :el :a}])

  (matcho/match
   (get-common-path
    (conj (parse-path "a.c" {:sliceName "s1"}) {:el :b})
    (conj (parse-path "a.c" {:sliceName "s2"})))
   [{:el :a} {:el :c}])


  (matcho/match
   (calculate-actions
    []
    (conj (parse-path "a" {:sliceName "s1"}) {:el :b}))
   [{:type :enter, :el :a}
    {:type :enter-slice, :sliceName "s1"}
    {:type :enter, :el :b}])

  (matcho/match
   (calculate-actions
    (conj (parse-path "a" {:sliceName "s1"}) {:el :b})
    (conj (parse-path "a" {:sliceName "s2"})))
   [{:type :exit, :el :b}
    {:type :exit-slice  :sliceName "s1"}
    {:type :enter-slice :sliceName "s2"}])

  (matcho/match
   (calculate-actions
    (conj (parse-path "a" {:sliceName "s1"}) {:el :b})
    [])
   [{:type :exit, :el :b}
    {:type :exit-slice, :sliceName "s1"}
    {:type :exit, :el :a}])

  (calculate-actions
   [{:sliceName :s1, :el :x} {:el :b}]
   [{:sliceName :s1, :el :x} {:el :b, :sliceName :z1}]
   )

  (def els
    [{:path "a" :type :a}
     {:path "b" :type :b}
     {:path "c" :type :c}
     {:path "c.d" :type :c.d}
     {:path "c.d.f" :type :c.d.f}
     {:path "c.d.i" :type :c.d.i}
     {:path "x" :type :x}
     {:path "x" :slicing {:discriminator [{:type "pattern" :path "a"}]}}
     {:path "x" :sliceName "s1"}
     {:path "x.a" :type :x.s1.a :patternString "s1"}
     {:path "x.b" :type :x.s1.b}
     {:path "x" :sliceName "s2"}
     {:path "x.a" :type :x.s2.a :patternString "s2"}
     {:path "x.b" :type :x.s2.b}])

  ;; FOCUS
  (matcho/match
   (build els)
   {:elements
    {:a {:type :a},
     :b {:type :b},
     :c {:type :c,
         :elements {:d {:type :c.d,
                        :elements {:f {:type :c.d.f}
                                   :i {:type :c.d.i}}}}},
     :x {:type :x,
         :slicing {:slices {:s1 {:match {:a "s1"}
                                 :elements {:a {:type :x.s1.a}
                                            :b {:type :x.s1.b}}}
                            :s2 {:match {:a "s2"}
                                 :elements {:a {:type :x.s2.a}
                                            :b {:type :x.s2.b}}}}}}}})

  (def els2
    [{:path "x" :type :x}
     {:path "x" :sliceName "s1" :slicing {:discriminator [{:type "pattern" :path "a"}]}}
     {:path "x.a" :type :x.s1.a :patternString "s1"}
     {:path "x.b" :type :x.s1.b :slicing {:discriminator [{:type "pattern" :path "f.ff"}]}}
     {:path "x.b" :sliceName "z1"}
     {:path "x.b.f" :type :x.s1.b.z1.f}
     {:path "x.b.f.ff" :type :x.s1.b.z1.ff :patternCoding {:code "z1"}}
     {:path "x.b" :sliceName "z2"}
     {:path "x.b.f" :type :x.s1.b.z2.f}
     {:path "x.b.f.ff" :type :x.s1.b.z2.ff :patternCoding {:code "z2"}}
     {:path "x" :sliceName "s2"}
     {:path "x.a" :type :x.s2.a :patternString "s2"}
     {:path "x.b" :type :x.s2.b}
     {:path "z" :type :z}])

  (matcho/match
   (build els2)
   {:elements
    {:z {:type :z}
     :x {:type :x,
         :slicing
         {:slices
          {:s1
           {:match {:a "s1"}
            :elements
            {:a {:type :x.s1.a},
             :b {:type :x.s1.b,
                 :slicing {:slices
                           {:z1 {:match {:f {:ff {:code "z1"}}}
                                 :elements {:f {:type :x.s1.b.z1.f, :elements {:ff {:type :x.s1.b.z1.ff}}}}},
                            :z2 {:match {:f {:ff {:code "z2"}}}
                                 :elements {:f {:type :x.s1.b.z2.f, :elements {:ff {:type :x.s1.b.z2.ff}}}}}}}}}},
           :s2 {:match {:a "s2"}
                :elements {:a {:type :x.s2.a}, :b {:type :x.s2.b}}}}}}}})

  (def union-els
    [{:path "value[x]" :type [{:code "string"} {:code "Quantity"}]}
     {:path "valueQuanity.unit" :short "unit" }])

  (matcho/match
   (build union-els)
   {:elements
    {:value         {:type [{:code "string"} {:code "Quantity"}]},
     :valueString   {:type {:code "string"}},
     :valueQuantity {:type {:code "Quantity"}},
     :valueQuanity  {:short "unit", :elements {:unit {:short "unit"}}}}})

  ;; (clojure.pprint/pprint (build els))
  )

;; migrate to [{:el :k}] pathes

