(ns far.resolution)


(defn build-package-tree []

  )

(comment

  (def context far/context)

  (->> (pg/execute! context {:dsql {:select :* :from :package_version }})
       (mapv (fn [x] (assoc (select-keys x [:id :name :version])
                           :dependencies (get-in x [:resource :dependencies])))))

  (def mcode-local-idx 
    (->> (pg/execute! context {:dsql {:select :* :from :canonical :where [:= :package_id [:pg/param  #uuid "454f267d-ea0f-5a07-927c-09a7b56ff302"]]}})
         (reduce (fn [acc x]
                   (-> acc
                       (assoc (utils.uuid/uuid (:url x)) (:id x))
                       (assoc (utils.uuid/uuid (:url x) (:version x)) (:id x)))
                   ) {})
         #_(mapv (fn [x] [ (:url x) (utils.uuid/uuid (:url x) (:version x)) (:id x)]))
         ))

  (->> (pg/execute! context {:dsql {:select :* :from :canonical_deps
                                    :where [:= :package_id [:pg/param  #uuid "454f267d-ea0f-5a07-927c-09a7b56ff302"]]}})
       (map (fn [x]
              (if-let [v (:version x)]
                (utils.uuid/uuid (:url x) v)
                (utils.uuid/uuid (:url x)))))
       (remove (fn [url] (contains? mcode-local-idx url)))
       (sort)
       (dedupe)
       #_(count)
       )

  (assoc (utils.uuid/uuid (:url x)) (:id x))

  (def pkgs
    [{:id 1 :name "a" :version 1
      :deps [{:name "b" :version 1}
             {:name "c" :version-gt-or-eq 2}]}
     {:id 2 :name "b" :version 1
      :deps [{:name "d" :latest true}]}

     {:id 3 :name "d" :version 1}
     {:id 4 :name "d" :version 2}
     {:id 5 :name "d" :version 3}

     {:id 6 :name "c" :versoin 3}
     {:id 7 :name "c" :versoin 2
      :deps [{:name "d" :version 1}]}])

  {"b" {:v 1 :deps {"d" {:v 5}}}
   "c" {:v 3 :deps {"d" {:v 1}}}}
  ;;=>

  [] [["b"] ["c"]]
  ["b"] [{"b" 1} {"d" 5}]
  ["c"] [{"c" 3} {"d" 1}]

  ;; resolve -> path [".." "b" "d"]

  ;; resolution
  ;; we should resolve in c and after that in d(v1)
  {:path ["c"]}
  ;; we should resolve in b and after that in d(v5)
  {:path ["b"]}

  [{:name "b" :deps [{:name "c"}]}
   {:name "d" :deps [{:name "c"}]}]

  {:name "b"
   :version "v"
   :symbols {:url-hash {}
             :url-with-version-hash {}}}

  ;; name.ndjson - all packages
  ;; name@version.ndjson.gz
  ;; package.json
  ;; index.json
  ;; external-deps.json
  ;; canonical-1.json
  ;; canonical-2.json
  ;; ...

  ;; package.json
  ;; load deps
  ;; all indexes in memory
  ;; all external deps

  ;; fhir build - first level deps and its deps


  )
