(ns far.tx
  (:require [system]
            [gcs]
            [pg]
            [pg.repo]
            [cheshire.core]
            [clj-yaml.core]
            [clojure.string :as str]))

(defn get-value [p]
  (->> p
       (reduce (fn [acc [k v]]
                 (if (str/starts-with? (name k) "value") v acc))
               {})))

(defn get-props [c]
  (->> (:property c)
       (reduce (fn [c p] (assoc c (keyword (:code p)) (get-value p))) {})))

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
     (select-keys cs [:description :content :url :name :valueSet :version :package_name :package_version])
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
                   (fn [acc {s :system vs :valueSet f :filter cs :concept :as item}]
                     (cond
                       cs (update acc :include-concept into (->> cs (mapv (fn [c] (assoc (select-keys c [:code :display]) :system s)))))
                       f  (update acc :deps conj (assoc item :type "include-filter" :url s))
                       s  (update acc :deps conj (assoc item :type "include-system" :url s))
                       vs  (->> vs
                                (reduce (fn [acc v]
                                          (update acc :deps conj (assoc item :type "include-valueset" :url v)))
                                        acc)))
                     )
                   {:deps []}))
        acc  (->> (:exclude (:compose vs))
                  (reduce
                   (fn [acc {s :system v :valueSet f :filter cs :concept :as item}]
                     (cond
                       cs (update acc :exclude-concept into (->> cs (mapv (fn [c] (assoc c :system s)))))
                       f  (update acc :deps conj (assoc item :type "exclude-filter" :url s))
                       s  (update acc :deps conj (assoc item :type "exclude-system" :url s))
                       v  (->> vs
                               (reduce (fn [acc v]
                                         (update acc :deps conj (assoc item :type "exclude-valueset" :url v)))
                                       acc))
                     ))
                   acc))
        tvs  (merge (select-keys vs [:url :name :version :package_name :package_version]) acc)]
    (cond-> tvs
      (and truncate (:include-concept tvs)) (update :include-concept (fn [xs] (take 10 xs))))))

(defn migrate [context]
  (pg.repo/register-repo context
   {:table "tiny_codesystem"
    :primary-key [:package_name :package_version :url :version]
    :columns {:package_name    {:type "text" :required true}
              :package_version {:type "text" :required true}
              :url             {:type "text" :required true}
              :version         {:type "text" :required true}
              :content         {:type "text" :required true}
              :resource        {:type "jsonb"}}})

  (pg.repo/register-repo context
                         {:table "tiny_codesystem_concept"
                          :primary-key [:package_name :package_version :url :version :code]
                          :columns {:package_name    {:type "text" :required true}
                                    :package_version {:type "text" :required true}
                                    :url             {:type "text" :required true}
                                    :version         {:type "text" :required true}
                                    :code            {:type "text" :required true}
                                    :diplay          {:type "text"}
                                    :resource        {:type "jsonb"}}})

  (pg.repo/register-repo context
                         {:table "tiny_valueset"
                          :primary-key [:package_name :package_version :url :version]
                          :columns {:package_name    {:type "text" :required true}
                                    :package_version {:type "text" :required true}
                                    :url             {:type "text" :required true}
                                    :version         {:type "text" :required true}
                                    :resource        {:type "jsonb"}}})

  (pg.repo/register-repo context
                         {:table "tiny_valueset_concept"
                          :primary-key [:package_name :package_version :url :version :system :code]
                          :columns {:package_name    {:type "text" :required true}
                                    :package_version {:type "text" :required true}
                                    :url             {:type "text" :required true}
                                    :version         {:type "text" :required true}
                                    :system          {:type "text" :required true}
                                    :code            {:type "text" :required true}
                                    :mode            {:type "text"}
                                    :resource        {:type "jsonb"}}})

  )

(comment

  (def context (system/start-system {:services ["pg" "pg.repo"] :pg (cheshire.core/parse-string (slurp "connection.json") keyword)}))

  context

  (pg/execute! context {:sql "drop table if exists tiny_codesystem"})

  (pg/execute! context {:sql "drop table if exists tiny_valueset"})

  (migrate context)

  (pg/execute! context {:sql "select url, resource->>'content' from codesystem limit 10"})

  (->> (pg.repo/select context {:table "codesystem" :limit 10})
       (mapv tiny-codesystem))

  (pg.repo/load context {:table "tiny_codesystem"}
                (fn [write]
                  (->> (pg.repo/select context {:table "codesystem"})
                       (mapv tiny-codesystem)
                       (mapv write))))

  (pg.repo/load context {:table "tiny_valueset"}
                (fn [write]
                  (->> (pg.repo/select context {:table "valueset"})
                       (mapv tiny-vs)
                       (mapv write))))

  (->> (pg.repo/select context {:table "tiny_valueset" :limit 10}))
  (->> (pg.repo/select context {:table "valueset" :limit 10})
       (mapv tiny-vs)
       (mapcat (fn [vs]
                 (->> (:deps vs)
                      (mapv (fn [i] (merge i (select-keys vs [:url :version :package_name :package_version]))))))))

  (pg/execute! context {:sql "select resource->>'content', count(*) from codesystem group by 1 order by 2 desc"})
  (pg/execute! context {:sql "select * from tiny_codesystem limit 20"})
  (pg/execute! context {:sql "select * from tiny_valueset limit 20"})

  (pg.repo/select context {:table "tiny_valueset" :limit 100})

  [{:?column? "complete", :count 2989}
   {:?column? "not-present", :count 295}
   {:?column? "example", :count 11}
   {:?column? "fragment", :count 9}
   {:?column? "supplement", :count 2}]



  ;; TODO:

  ;; codesysystem - content provided

  ;; valueset (you are doing expansion and check that all codesystems content provided)
  ;;   depends on system x - content
  ;;   depends on valueset - (eval valuest) - content provided?
  ;;   content - provided or dynamic

  ;; what is expansion
  ;; if codesystem - add to cs (codesystem should be resolved in transitive deps)
  ;; if valueset (expand it) -> add [cs]
  ;; if concept - skip


  (pg.repo/select context {:table "package_dependency" :limit 100})

  (pg.repo/select context {:table "package_version" :limit 100})

  )
