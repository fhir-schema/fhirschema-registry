(ns far.tx
  (:require [system]
            [gcs]
            [pg]
            [pg.repo]
            [cheshire.core]
            [clj-yaml.core]
            [utils.uuid]
            [far.package]
            [far.tx.concepts]
            [clojure.string :as str]))


(defn classify-vs [context vs]
  (let [deps (far.package/canonical-deps context vs)
        cs-deps  (->> deps (filter (fn [{cn :canonical}] (= "CodeSystem" (:resource_type cn)))))
        all-resolved (or (empty? cs-deps) (= #{"resolved"} (->> cs-deps (mapv (fn [{st :status}] st)) (into #{}))))
        static (and all-resolved
                    (or (empty? cs-deps) (= #{"complete"} (->> cs-deps (mapv (fn [{cn :canonical}] (:content cn))) (into #{})))))]
    {:id (:id vs)
     :url (:url vs)
     :static static
     :resolved all-resolved}))


(defn expand-include [context {system :system filter :filter concept :concept valuesets :valueSet :as incl}]
  (cond
    concept (->> concept (mapv (fn [c] (assoc c :system system))))))

(defn resolve-canonical [context canonical url]
  (let [[url version] (str/split url #"\|" 2)]
    (when-let [dep (pg.repo/read context {:table "canonical_deps"
                                          :match (cond->{:definition_id (:id canonical)
                                                         :url url}
                                                   version (assoc :version version))})]

      (when-let [dip (:dep_id dep)]
        (when-let [cn (pg.repo/read context {:table "canonical" :match {:id dip}})]
          (pg.repo/read context {:table (:resource_type cn) :match {:id dip}}))))))

(def concept-select [:pg/list :system :code :display])

(defn build-filter [flt]
  (->> flt
       (mapv (fn [flt]
               (cond
                 (= "is-a" (:op flt))            [:pg/include-op [:resource-> :is-a] [:pg/jsonb [(:value flt)]]]
                 (= "="    (:op flt))            [:= [:resource#>> [:property (keyword (:property flt))]] [:pg/param (:value flt)]]
                 ;; TODO: fix logic - we have to find the concept and test for intersection
                 (= "descendent-of" (:op flt))   [:pg/include-op [:resource-> :is-a] [:pg/jsonb [(:value flt)]]]
                 :else (assert false (pr-str flt)))))
       (into [:and])))

(comment
  (pg/format-dsql (build-filter {:op "is-a", :value "AGNT", :property "concept"}))
  (pg/format-dsql (build-filter {:op "=", :value "a", :property "canonical"}))

  )
(defn expand-dsql [context vs]
  (let [include (get-in vs [:compose :include])]
    (->> include
         (reduce
          (fn [acc {system :system filter :filter concept :concept valuesets :valueSet :as incl}]
            (cond
              concept (conj acc {:select concept-select
                                 :comment system
                                 :from :concept
                                 :where {:valueset [:= :canonical_id [:pg/param (:id vs)]]}})
              filter  (let [cs (resolve-canonical context vs system)]
                        (conj acc {:select concept-select
                                   :comment system
                                   :from :concept
                                   :where {:system-filter [:and [:= :canonical_id [:pg/param (:id cs)]] (build-filter filter)]}}))
              system  (let [cs (resolve-canonical context vs system)]
                        (conj acc {:select concept-select
                                   :comment system
                                   :from :concept
                                   :where {:system [:= :canonical_id [:pg/param (:id cs)]]}}))
              valuesets (->> valuesets
                             (reduce (fn [acc vs-url]
                                       (if-let [ivs (resolve-canonical context vs vs-url)]
                                         (into acc (expand-dsql context ivs))
                                         (throw (Exception. (str "Could not resolve " vs-url)))))
                                     acc))
              :else (assert false)))
          []))))

(defn expand-query [context vs]
  (->> (expand-dsql context vs)
       (reduce
        (fn [q {c :comment :as s}]
          (assoc q (keyword (last (str/split (or c "ups") #"/")))
                 (-> (dissoc s :comment) (assoc :ql/type :pg/select))))
        {:ql/type :pg/union-all})))

(defn expand [context vs]
  (pg/execute! context {:dsql (expand-query context vs)}))

;; algorithm
;; include
;;    concepts     (select concept where vs-id = vs-id)
;;    cs           (select concept where cs-id = cs-id)
;;    cs + filter  (select concept where cs-id = cs-id and filter)
;;    valueset     recur
;;
;; exclude
;;    cs, cs+filter, valuest
;; vs -> one big union

(defn get-vs [context url]
  (pg.repo/select context {:table "valueset" :match {:url url}}))

(defn get-cs [context url]
  (pg.repo/select context {:table "codesystem" :match {:url url}}))


(defn migrate [context]
  (pg.repo/register-repo
   context {:table "concept"
            :primary-key [:id]
            :columns {:id              {:type "uuid"}
                      :package_name    {:type "text" :required true}
                      :package_version {:type "text" :required true}
                      :package_id      {:type "uuid" :required true :indexed true}
                      :logical_id      {:type "uuid" :required true :indexed true}
                      :canonical_id    {:type "uuid" :required true :indexed true}
                      :canonical_url   {:type "text" :required true :indexed true}
                      :canonical_version   {:type "text"}
                      :system          {:type "text" :requried true}
                      :code            {:type "text" :requried true}
                      :display         {:type "text"}
                      :resource        {:type "jsonb"}}}))

(defn drop-tables [context]
  (pg.repo/drop-repo context {:table "concept"}))

(defn truncate [context]
  (pg.repo/truncate context {:table "concept"}))


(defn load-concepts [context]
  (far.tx.concepts/load-concepts-from-cs context)
  (far.tx.concepts/load-concepts-from-vs context))

(comment

  (def context (system/start-system {:services ["pg" "pg.repo"] :pg (cheshire.core/parse-string (slurp "connection.json") keyword)}))
  (migrate context)

  (drop-tables context)

  (truncate context)

  (load-concepts context)

  (pg.repo/select context {:table "concept" :limit 100})

  (classify-vs context {:id #uuid "9af72049-0c52-5afd-a983-273c649e2536"})
  (->> (pg.repo/select context {:table "valueset" :limit 100})
       (mapv :compose))

  (resolve-canonical
   context
   {:id #uuid "85741cc6-1803-5ad0-b234-90e593efea1c"}
   "http://terminology.hl7.org/CodeSystem/coverageeligibilityresponse-ex-auth-support")

  (->> (pg.repo/select context {:table "valueset" :limit 100})
       (mapv #(classify-vs context %)))

  (->> (pg.repo/select context {:table "valueset" :limit 100})
       (mapv (fn [vs]
               (println (:url vs))
               (println (mapv :code (expand context vs))))))

  ;; HERE

  ;;TODO write tests for this guys
  (def vsurl "http://terminology.hl7.org/ValueSet/v3-RoleClassAgent")
  (def vsurl "http://hl7.org/fhir/ValueSet/provider-taxonomy")
  (def vsurl "http://hl7.org/fhir/ValueSet/all-time-units")
  (def vsurl "http://terminology.hl7.org/ValueSet/v3-AudioMediaType")

  (far.tx.concepts/extract-concept-cs
   (first (get-cs context "http://terminology.hl7.org/CodeSystem/v3-mediaType"))
   )

  (pg.repo/select context {:table "concept" :match {:canonical_url  "http://terminology.hl7.org/CodeSystem/v3-mediaType"}})


  (->> (get-vs context vsurl)
       (first)
       ;;(expand context)
       ;; (expand-dsql context)
       ;; (expand-query context)
       )

  (->> (get-vs context "http://terminology.hl7.org/ValueSet/v3-ConfidentialityClassification")
       (mapv :compose))

  (:id (first (get-vs context "http://hl7.org/fhir/ValueSet/security-labels")))

  (resolve-canonical
   context
   {:id #uuid "9af72049-0c52-5afd-a983-273c649e2536"}
   "http://terminology.hl7.org/ValueSet/v3-ConfidentialityClassification")

  (pg.repo/select context {:table "canonical"  :match {:resource_type "CodeSystem"} :limit 10})
  (pg.repo/select context {:table "canonical_deps"  :limit 10})

  context

  (pg/execute! context {:sql "drop table if exists tiny_codesystem"})
  (pg/execute! context {:sql "drop table if exists tiny_valueset"})

  (migrate context)

  (pg/execute! context {:sql "select url, resource->>'content' from codesystem limit 10"})

  (->> (pg.repo/select context {:table "codesystem" :limit 10})
       (mapv (fn [x]
               {:id (utils.uuid/uuid (:package_name x) (:package_version x) (:url x) (:version x))
                :package_id (utils.uuid/uuid (:package_name x) (:package_version x))}))
       #_(mapv tiny-codesystem))

  (pg.repo/load context {:table "tiny_codesystem"}
                (fn [write]
                  (->> (pg.repo/select context {:table "codesystem"})
                       (mapv tiny-codesystem)
                       (mapv write))))

  (->> (pg.repo/select context {:table "tiny_codesystem" :limit 10}))

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


  (pg.repo/drop-repo
   context {:table "valueset_deps"})

  (pg.repo/register-repo
   context
   {:table "valueset_deps"
    :primary-key [:id :type :url]
    :columns {:id {:type "uuid" :required true}
              :package_id {:type "uuid" :required true}
              :valueset_url {:type "text" :required true}
              :type {:type "text" :required true}
              :url {:type "text" :required true}}})

  (pg.repo/truncate context {:table "valueset_deps"})

  (pg.repo/load
   context {:table "valueset_deps"}
   (fn [w]
     (->> (pg.repo/select context {:table "valueset"})
          (mapv vs-deps)
          (mapv (fn [ds]
                  (doseq [d ds]
                    (w d)))))))

  (pg.repo/select context {:table "valueset_deps" :limit 100})

  (pg/execute! context {:sql "select type, count(*) from valueset_deps group by 1 order by 2 desc"})
  (pg/execute! context {:sql "select valueset_url, count(*) from valueset_deps group by 1 order by 2 desc limit 10"})

  (pg.repo/select context {:table "package_version" :limit 10})

  (pg.repo/select context {:table "valueset_deps" :match {:id #uuid "85741cc6-1803-5ad0-b234-90e593efea1c"}})

  (defn resolve-vs-deps [vs-id]
    (let [deps (pg.repo/select context {:table "valueset_deps" :match {:id vs-id}})]
      (concat
       (->> deps (filter (fn [x] (= (:type x) "codesystem"))))
       (->> deps
            (filter (fn [x] (= (:type x) "valueset")))
            (mapcat (fn [{}]))
            ))))


  (pg.repo/select context {:table "package_version" :limit 10})

  (pg.repo/select context {:table "package_dependency" :limit 10})

  ;; valuest-id
  ;; get deps
  ;; add codesystems
  ;; for every valuest dep
  ;;    get-deps

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
  (pg.repo/select context {:table "capabilitystatement" :limit 100})
  (pg.repo/select context {:table "terminologycapabilities" :limit 100})

  (pg.repo/select context {:table "package_version" :limit 100})
  (pg.repo/select context {:table "package_version" :limit 100})


  (pg.repo/select context {:table "canonical" :limit 100 :match {:resource_type "ValueSet"}})

  

  (pg.repo/select context {:table "canonical_deps" :limit 10})

  (defn get-all-deps [context canonical]
    (pg/execute! context {:sql [deps-sql (:id canonical)]}))

  (doseq [vs  (pg.repo/select context {:table "valueset" :select [:id :url] :limit 100})]
    (println vs)
    (println " * " (str/join "\n * "(get-all-deps (system/ctx-set-log-level  context :error) vs))))

  ;; get all deps for valueset


  )
