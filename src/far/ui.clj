(ns far.ui
  (:require
   [system]
   [clojure.string :as str]
   [pg.repo]
   [http]
   [pg]
   [far.ui.helpers :as h]
   [far.package]
   [far.tx]
   [cheshire.core]
   [fhir.schema.transpiler]))

(system/defmanifest {:description "UI for far"})


(defn elipse [txt & [cnt]]
  (when txt
    (subs txt 0 (min (count txt) (or cnt 70)))))

(defn render-packages [pkgs]
  (->> pkgs
       (map (fn [p]
              [:a.px-4.py-2.border-b.space-x-2.flex.items-baseline {:href (str "/packages/" (:id p))}
               [:i.fa-regular.fa-folder.text-gray-500]
               [:span (:name p)] "@" [:span (:version p)]
               [:span.text-sm.text-gray-400 (elipse (:description p))]]))
       (into [:div#search-results])))

(defn packages-html [context req]
  (let [pkgs (pg.repo/select context {:table "package_version" :order-by [:pg/desc :name :version]})]
    (h/hiccup-response
     [:div
      (h/search-input {:path ["packages" "search"]})
      [:br]
      (render-packages pkgs)])))

(defn packages-search-html [context {{q :search} :query-params}]
  (let [pkgs (pg.repo/select context {:table "package_version"
                                      :where (when-not (str/blank? q)
                                               [:ilike :name [:pg/param (str "%" (str/replace q #"\s+" "%") "%")]])
                                      :order-by [:pg/desc :name :version]})]
    (h/hiccup-response
     [:div
      (when (empty? pkgs)
        [:div.text-gray-400 "No results"])
      (render-packages pkgs)])))

(def canonicals-stats-sql
  "
select resource_type, count(*) as count
from canonical
where package_id = ?
group by 1
order by 1
")

(defn canonicals-tabs [context pkg-id]
  (let [prefix ["packages" pkg-id]
        stats (pg/execute! context {:sql [canonicals-stats-sql pkg-id]})]
    (->> stats
         (mapv (fn [{rt :resource_type cnt :count}]
                 [(into prefix [rt]) [:span rt " (" cnt ")"]]))
         (into [[prefix "Package"]])
         (apply h/nav))))


(comment
  (pg/execute! context {:sql [canonicals-stats-sql "d2f5929e-d332-58a1-a3bc-a8d5896776b6"]})


  )

(defn package-html [context {{id :id} :route-params}]
  (let [pkg (pg.repo/read context {:table "package_version" :match {:id id}})]
    (h/hiccup-response
     [:div.px-4
      (h/breadcramp "packages" "packages" "#" (:name pkg))

      [:div.flex.space-x-4
       (canonicals-tabs context id)
       [:div.flex-1
        (h/h1 (:name pkg) "@" (:version pkg))
        [:p (:description pkg)]
        [:h2 "Deps"]
        (->> (:dependencies pkg)
             (mapv (fn [[n v]]
                     [:li.pl-4 (name n) "@" v]))
             (into [:ul]))

        [:br]
        (h/h2 "Package.json")
        (h/json-block (dissoc pkg :all_dependencies))

        ]]


      ])))

(defn valuesets-html [context {{id :id} :route-params}]
  (let [pkg       (pg.repo/read context {:table "package_version" :match {:id id}})
        valuesets (pg.repo/select context {:table "valueset" :select [:id :name :url :version :processing] :match {:package_id id}})
        ]
    (h/hiccup-response
     [:div.px-4
      (h/breadcramp "packages" "packages" (:id pkg) (:name pkg) "#" "valuesets")
      [:br]

      [:div.flex.space-x-4
       (canonicals-tabs context id)
       [:div.flex-1
        (h/h1 "ValueSets")
        (h/table [] (->> valuesets (sort-by :name))
                 (fn [c]
                   [
                    (if (:resolved (:processing c))
                      [:i.fa-regular.fa-square-check.text-green-500]
                      [:i.fa-regular.fa-square.text-red-500])
                    (h/link ["canonicals" "ValueSet" (:id c)] (str (:name c)))
                    (:version c)
                    (if (:static (:processing c)) "static" "dynamic")
                    (:expand (:processing c))]))]]])))


(defn codesystems-html [context {{id :id} :route-params}]
  (let [pkg       (pg.repo/read context {:table "package_version" :match {:id id}})
        valuesets (pg.repo/select context {:table "codesystem" :match {:package_id id}})]
    (h/hiccup-response
     [:div.px-4
      (h/breadcramp "packages" "packages" (:id pkg) (:name pkg) "#" "valuesets")
      [:br]
      [:div.flex.space-x-4
       (canonicals-tabs context id)
       [:div.flex-1
        (h/h1 "CodeSystems")
        (h/table [] (->> valuesets (sort-by :name))
                 (fn [c]
                   [(h/link ["canonicals" "CodeSystem" (:id c)] (str (:name c)))
                    (:version c)
                    (:content c)]))]]])))

(defn canonicals-html [context {{rt :resource_type id :id} :route-params}]
  (let [pkg       (pg.repo/read context {:table "package_version" :match {:id id}})
        cns (pg.repo/select context {:table "canonical" :match {:package_id id :resource_type rt}})]
    (h/hiccup-response
     [:div.px-4
      (h/breadcramp "packages" "packages" (:id pkg) (:name pkg) "#" "valuesets")
      [:br]
      [:div.flex.space-x-4
       (canonicals-tabs context id)
       [:div.flex-1
        (h/h1 rt)
        (h/table [] (->> cns (sort-by :name))
                 (fn [c]
                   [(h/link ["canonicals" rt (:id c)] (str (or (:name c) (:url c))))
                    (:version c)
                    (:content c)]))]]])))




(defn valueset-html [context {{id :id} :route-params}]
  (let [cn (pg.repo/read context {:table "valueset" :match {:id id}})
        expand-sql (far.tx/expand-query context cn)
        expansion (far.tx/expand context cn)
        ;;concepts (far.tx/valueset-concepts context cn)
        deps (far.package/canonical-deps context cn)]
    (h/hiccup-response
     [:div.px-4
      (h/breadcramp
       "packages" "packages"
       (:package_id cn) (:package_name cn)
       "ValueSet" "valuesets"
       "#" (:name cn))

      (h/h1 [:span (:resourceType cn)]
          [:span (:name cn)]
          [:span.text-sm.text-gray-400 (:url cn)])

      (h/yaml-block (:processing cn))

      (h/h2 "Compose")
      (h/yaml-block (:compose cn))

      (h/h2 "Deps")
      (->> deps
           (mapv (fn [d]
                   [:li.flex.space-x-4
                    [:span (:type d)]
                    (if-let [did (:dep_id d)]
                      [:a.text-sky-500 {:href (str "/canonicals/" (:resource_type d) "/" did)} (:url d) (when-let [v (:version d)] (str "|" v))]
                      [:span.text-red-500 (:url d) (when-let [v (:version d)] (str "|v"))])
                    #_[:span.text-sm.text-gray-400 (pr-str (dissoc d :url :version :dep_id :definition))]]))
           (into [:ul.my-3]))

      [:details.my-3
       [:summary (h/h2 "Expansion query")]
       (h/edn-block expand-sql)
       ]

      (h/h2 "Expansion")
      ;; (h/yaml-block expansion)
      (h/table [:system :code :display] expansion)

      [:details.my-3
       [:summary [:b "resource"]]
       (h/yaml-block cn)]

      ])))

(defn codesystem-html [context {{id :id} :route-params}]
  (let [cn (pg.repo/read context {:table "codesystem" :match {:id id}})
        concepts (far.tx/codesystem-concepts context cn)]
    (h/hiccup-response
     [:div.px-4
      (h/breadcramp
       "packages" "packages"
       (:package_id cn) (:package_name cn)
       "CodeSystem" "codesystems"
       "#" (:name cn))

      (h/h1 [:span (:resourceType cn)]
            [:span (:name cn)])

      [:p.text-sm.text-gray-400 (:url cn)]

      (h/table [:code :display] concepts
               (fn [c]
                 [(:code c)
                  (:display c)
                  (h/json-block (:resource c))]))

      (h/yaml-block (dissoc cn :concept))]

     )))

(defn canonical-html [context {{rt :resource_type id :id} :route-params}]
  (let [cn (pg.repo/read context {:table (str/lower-case rt) :match {:id id}})]
    (h/hiccup-response
     [:div.px-4
      (h/breadcramp
       "packages" "packages"
       (:package_id cn) (:package_name cn)
       rt rt
       "#" (:name cn))

      (h/h1 [:span (:resourceType cn)]
            [:span (:name cn)])

      [:p.text-sm.text-gray-400 (:url cn)]

      [:br]

      (h/yaml-block (dissoc cn :concept))]

     )))

(declare render-schema)
(defn render-slices [sl]
  (->> (:slices sl)
       (map (fn [[sl-name sl]]
              [:div
               [:div.flex.space-x-2 [:span.font-semibold sl-name] (:max sl) (:min sl)]
               [:div.pl-8 (render-schema (:schema sl))]]))
       (into [:div ]))

  )

(defn code [x]
  (cond
    (map? x)
    (-> (->> x
          (mapv (fn [[k v]] [:span [:span.font-medium (name k) ":"] (code v)]))
          (interpose ",")
          (into [:span [:span " { "]]))
        (conj [:span "}"]))
    (vector? x)
    (-> (->> x
             (mapv (fn [v] (code v)))
             (interpose " , ")
             (into [:span [:span " [ "]]))
        (conj [:span " ] "]))
    :else [:span.text-gray-600 (pr-str x)]))


(defn render-schema [schema]
  (let [required (into #{} (map keyword (:required schema)))]
    (->> 
     (concat 
      (->> (:extensions schema)
           (mapv (fn [[k el]]
                   [:div
                    (when-let [s (:short el)] [:span.text-gray-500.text-xs.font-mono "// " s])
                    [:div.flex.space-x-1.py-0.5.items-center
                     [:span.font-mono.font-semibold.text-gray-700
                      "+"(name k)
                      (when (contains? required k)
                        [:i.fa-sharp.fa-solid.fa-asterisk.text-red-600.text-xs.relative.-top-1] )]
                     [:span.text-sky-800.flex.space-x-2.items-center.font-mono
                      [:span.whitespace-nowrap
                       (str (when-let [tp (:type el)]
                              (str ":" tp))
                            " &lt; " (last (str/split (get el :url "") #"/")) " &gt;")
                       [:span.text-gray-500 "[" (get el :min 0) "," (get el :max "*")  "]"]]]]])))
      (->> (:elements schema)
           (sort-by #(:index (second %)))
           (map (fn [[k el]]
                  [:div
                   (when-let [s (:short el)] [:span.text-gray-500.text-xs.font-mono "// " s])
                   [:div.flex.space-x-1.py-0.5.items-center
                    [:span.font-mono.font-semibold.text-gray-700
                     (name k)
                     (when (contains? required k)
                       [:i.fa-sharp.fa-solid.fa-asterisk.text-red-600.text-xs.relative.-top-1] )]
                    [:span.text-sky-800.flex.space-x-2.items-center.font-mono
                     [:span.whitespace-nowrap
                      (when-let [tp (:type el)]
                        (str ":" tp))
                      (when-let [cr (:contentReference el)]
                        (str ":" (subs cr 1)))
                      (when (:array el)
                        [:span.text-gray-500 "[" (get el :min 0) "," (get el :max "*")  "]"])]
                     (when-let [b (:binding el)]
                       (list
                        [:span " &lt;" (case (:strength b)
                                          "required" "!"
                                          "extensible" "+"
                                          "example" "?"
                                          )
                         " "
                         (last (str/split (get b :valueSet "?") #"/")) " &gt; "]))
                     (when-let [r (:refers el)]
                       (list
                        [:span " &lt; " (->> r (mapv :resource) (str/join " | ")) " &gt;"]))
                     (when-let [p (:pattern el)]
                       (list
                        [:span.text-gray-700 " = " (code (:value p))]))
                     ]
                    (when-let [disc (get-in el [:slicing :discriminator])]
                      [:span  " ~= " (code disc)])]
                   (when-let [sl (:slicing el)]
                     [:div.pl-5 (render-slices sl)])
                   (when (:elements el)
                     [:div.pl-5 (render-schema el)])]))))
     (into [:div.text-sm]))))

(defn structuredef-html [context {{ id :id} :route-params}]
  (let [cn (pg.repo/read context {:table "structuredefinition" :match {:id id}})
        schema (fhir.schema.transpiler/translate cn)]
    (h/hiccup-response
     [:div.px-4
      (h/breadcramp
       "packages" "packages"
       (:package_id cn) (:package_name cn)
       "StructureDefinition" "StructureDefinition"
       "#" (:name cn))

      (h/h1 [:span (:resourceType cn)]
            [:span (:name cn)])

      [:p.text-sm.text-gray-400 (:url cn)]

      [:br]

      ;; (h/yaml-block schema)

      [:div.border.p-4.rounded.bg-gray-100 (render-schema schema)]

      [:br]

      (h/yaml-block schema)

      [:br]

      (h/yaml-block (dissoc cn :concept))]

     )))

(defn mount-routes [context]
  (http/register-endpoint context {:method :get :path "/packages" :fn #'packages-html})
  (http/register-endpoint context {:method :get :path "/packages/search" :fn #'packages-search-html})
  (http/register-endpoint context {:method :get :path "/packages/:id" :fn #'package-html})
  (http/register-endpoint context {:method :get :path "/canonicals/ValueSet/:id" :fn #'valueset-html})
  (http/register-endpoint context {:method :get :path "/canonicals/CodeSystem/:id" :fn #'codesystem-html})
  (http/register-endpoint context {:method :get :path "/packages/:id/ValueSet" :fn #'valuesets-html})
  (http/register-endpoint context {:method :get :path "/packages/:id/CodeSystem" :fn #'codesystems-html})
  (http/register-endpoint context {:method :get :path "/packages/:id/:resource_type" :fn #'canonicals-html})
  (http/register-endpoint context {:method :get :path "/canonicals/:resource_type/:id" :fn #'canonical-html})

  (http/register-endpoint context {:method :get :path "/canonicals/StructureDefinition/:id" :fn #'structuredef-html})

  )

(system/defstart
  [context _]
  (mount-routes context))

(comment
  (def context far/context)

  (http/register-endpoint context {:method :get :path "/packages/:id/ValueSet" :fn #'valuesets-html})

  (http/get-routes context)

  )
