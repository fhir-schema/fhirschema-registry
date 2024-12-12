(ns far.package.loader
  (:require [system]
            [pg]
            [pg.repo]
            [cheshire.core]
            [utils.uuid]
            [fhir.package]
            [far.package.canonical-deps :refer [extract-deps]]
            [far.package.repos :as repos]
            [clojure.string :as str]))

(defn loadable? [nm]
  (let [lnm (str/lower-case nm)]
    (and (str/ends-with? nm ".json")
         (or (contains? #{"package.json" ".index.json"} nm)
             (some (fn [x] (str/starts-with? lnm x)) repos/canonicals)))))

(defn prepare-resource [nm pkgi res]
  (let [res (-> (assoc-in (dissoc res :text :snapshot :expansion) [:meta :file] nm)
                (assoc
                 :package_id (utils.uuid/uuid (:name pkgi) (:version pkgi))
                 :package_name (:name pkgi)
                 :package_version (:version pkgi)
                 :resource_id (:id res)
                 :version (or (:version res) (:version pkgi))))]
    (assoc res :id (utils.uuid/canonical-id res))))

(defn read-package
  "load package into hash-map with :package :index and <rt>"
  [package-info]
  (let [pkgi package-info]
    (->
     (fhir.package/reduce-package
      pkgi
      (fn [acc nm read-file]
        (if (loadable? nm)
          (let [res (read-file true)]
            (cond
              (= nm "package.json")
              (assoc acc
                     :package_version res
                     :package_dependency (:dependencies res))

              (= nm ".index.json")
              (assoc acc :index res)
              (and (:resourceType res) (:url res))
              (let [res (prepare-resource nm pkgi res)
                    path (->> [(str/lower-case (or (:resourceType res) nm)) (:url res)] (filter identity))]
                (assoc-in acc path res))
              :else
              (do #_(println :skip nm) acc)))
          (do
            #_(println :skip nm)
            acc))))
     (assoc :package pkgi))))

(defn canonical-elements [r]
  (select-keys r [:id :package_id :package_name :package_version
                  :url :version
                  ;;sd
                  :kind :derivation :status :expiremental :fhirVersion :kind :abstract :type :baseDefinition
                  ;;cs
                  :title :name :content
                  ;; vs
                  :immutable]))

(defn- load-canonicals [context pkg]
  (pg.repo/load
   context {:table "canonical"}
   (fn [insert-canonical]
     (doseq [cn repos/canonicals]
       (when-let [rs (vals (get pkg cn))]
         (system/info context ::load (str cn " - " (count rs)))
         (pg.repo/load
          context {:table cn}
          (fn [insert]
            (doseq [r rs]
              (insert-canonical (-> (canonical-elements r)
                                    (assoc  :resource_type (:resourceType r))
                                    (update :version (fn [x] (or x (:package_version x))))))
              (insert r)))))))))

(declare load-package*)

(defn add-new-package [context pkg]
  (update context ::new-packages (fn [x] (conj (or x []) pkg))))

(defn get-new-packages [context]
  (get context ::new-packages))

(defn clear-new-packages [context]
  (assoc context ::new-packages []))

(defn- load-deps [context pkg pkgi opts]
  (->> (:package_dependency pkg)
       (reduce (fn [context [k v]]
                 (let [resource {:package_id (utils.uuid/uuid (:name pkgi) (:version pkgi))
                                 :dep_id (utils.uuid/uuid (name k) v)
                                 :name (:name pkgi)
                                 :version (:version pkgi)
                                 :dep_name (name k)
                                 :dep_version v}]
                   (pg.repo/insert context {:table "package_dependency" :resource resource})
                   (load-package* context (name k) v (update opts [:path] (fn [x] (conj (or x []) (:name pkgi)))))))
               context)))

(defn load-elements [context pkg-bundle]
  (pg.repo/load
   context {:table "structuredefinition_element"}
   (fn [insert]
     (doseq [[url sd] (get pkg-bundle "structuredefinition")]
       (doseq [el (get-in sd [:differential :element])]
         ;; TODO: do it from database - duplicating id logic
         (insert
          (assoc el
                 :definition_id (utils.uuid/uuid url (or (:version sd) (:package_version sd)) (:package_name sd) (:package_version sd))
                 :definition   url
                 :version      (or (:version sd) (:package_version sd))
                 :package_id (utils.uuid/uuid (:package_name sd) (:package_version sd))
                 :package_name (:package_name sd)
                 :package_version (:package_version sd))))))))

(defn resolve-all-deps* [context pkv & [acc]]
  (let [acc (or acc {:deps {} :level 0})]
    (->> (:dependencies pkv)
         (reduce (fn [acc [k v]]
                   (let [dep-id (utils.uuid/uuid (name k) v)]
                     (if (contains? (:deps acc) dep-id)
                       acc
                       (let [dp (pg.repo/read context {:table "package_version" :match {:id dep-id}})
                             _ (assert dp (str "Could not resolve dep " k " " v " " dep-id))
                             prev-level (:level acc)
                             acc (-> acc
                                     (assoc-in [:deps dep-id] {:name (name k) :level (:level acc) :version v :id dep-id})
                                     (update :level inc))]
                         (-> (resolve-all-deps* context dp acc)
                             (assoc :level prev-level))))))
                 acc))))

(defn resolve-all-deps [context pkv]
  (->> (vals (:deps (resolve-all-deps* context pkv)))
       (into [])
       (sort-by :level)))

(defn deps-tree [context pkv]
  (->> (:dependencies pkv)
       (reduce (fn [deps [k v]]
                 (assoc deps k {:version v
                                :deps (let [dp (pg.repo/read context {:table "package_version" :match {:name (name k) :version v}})]
                                        (deps-tree context dp))}))
               {})))

(defn print-deps-tree [deps-tree & [ident]]
  (let [ident (or ident 0)]
    (doseq [[k v] deps-tree]
      (println (str/join " " (mapv (constantly " ") (range ident))) (name k) (:version v))
      (when (seq (:deps v))
        (print-deps-tree (:deps v) (inc (inc ident)))))))


(defn get-canonical [context match]
  (system/get-context-cache
   context [:canonical match]
   (fn [] (pg.repo/select context {:table "canonical" :match match}))))

(defn get-deps-idx [context package_id]
  (let [pid (if (uuid? package_id) package_id (parse-uuid package_id))]
    (->> (pg.repo/read context {:table "package_version" :match {:id package_id}})
         (:all_dependencies)
         (reduce (fn [acc {id :id :as p}]
                   (assoc acc (parse-uuid id) p))
                 {pid {:level -1}}))))

(defn get-pkg-deps [context dep]
  (system/get-context-cache
   context [:pkgs (:package_id dep)]
   (fn [] (get-deps-idx context (:package_id dep)))))

(defn resolve-dep [context dep]
  (let [idx        (get-pkg-deps context dep)
        rdep  (->> (get-canonical context (if-let [v (:version dep)] {:url  (:url dep) :version v} {:url (:url dep)}))
                   (mapv (fn [r] (assoc r :level (:level (get idx (:package_id r))))))
                   (filter :level)
                   (sort-by :level)
                   (first))]
    (if rdep
      (assoc dep :dep_id (:id rdep) :dep_package_id (:package_id rdep) :status "resolved")
      (assoc dep :status "error"))))

(defn load-canonical-deps [context pkgi]
  (let [pid (utils.uuid/uuid (:name pkgi) (:version pkgi))]
    (system/info context ::valueset-deps)
    (time
     (pg.repo/load
      context {:table "canonical_deps"}
      (fn [insert]
        (pg.repo/fetch
         context {:table "valueset" :match {:package_id pid}}
         (fn [sd]
           (doseq [d  (extract-deps sd)]
             (insert (resolve-dep (system/ctx-set-log-level context :error) d))))))))

    (system/info context ::valueset-deps)
    (time 
     (pg.repo/load
      context {:table "canonical_deps"}
      (fn [insert]
        (pg.repo/fetch
         context {:table "structuredefinition" :match {:package_id pid}}
         (fn [sd]
           (doseq [d  (extract-deps sd)]
             (insert (resolve-dep (system/ctx-set-log-level context :error) d))))))))))

(defn load-package*
  "load package recursively"
  [context package-name package-version & [opts]]
  (if (pg.repo/read context {:table "package_version" :match {:name package-name :version package-version}})
    context
    (do
      (system/info context ::load-package (str package-name "@" package-version))
      (let [pkgi (fhir.package/pkg-info (str package-name "@" package-version))
            pkg-bundle (read-package pkgi)
            context' (load-deps context pkg-bundle pkgi opts)
            package_version (:package_version pkg-bundle)
            all-deps (resolve-all-deps context package_version)]
        (pg.repo/upsert context {:table "package" :resource pkgi})
        (pg.repo/insert context {:table "package_version"
                                 :resource (assoc package_version
                                                  :all_dependencies all-deps
                                                  :id (utils.uuid/uuid (:name package_version) (:version package_version)))})
        (load-canonicals context pkg-bundle)
        (load-elements context pkg-bundle)
        (load-canonical-deps context pkgi)
        (add-new-package context' {:package_name package-name, :package_version package-version})))))

(defn load-package
  "load package recursively"
  [context package-name package-version]
  (get-new-packages (load-package* context package-name package-version {})))


(defn pkg-info [context package-name]
  (fhir.package/pkg-info package-name))