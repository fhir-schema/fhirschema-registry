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
     [:div.px-4
      [:br]
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
    [:div.pt-4
     (->> stats
          (mapv (fn [{rt :resource_type cnt :count}]
                  [(into prefix [rt])
                   [:span rt]
                   [:span {:class "ml-auto w-9 min-w-max rounded-full bg-white px-2.5 py-0.5 text-center text-xs/5 font-medium whitespace-nowrap text-gray-600 ring-1 ring-gray-200 ring-inset", :aria-hidden "true"} 
                    (str cnt)]
                   ]))
          (into [[prefix "Package"]])
          (apply h/nav))]))


(comment
  (pg/execute! context {:sql [canonicals-stats-sql "d2f5929e-d332-58a1-a3bc-a8d5896776b6"]})


  )

(defn render-deps [deps]
  (if (empty? deps)
    [:p.text-gray-500 "No dependencies"]
    (->> deps
         (map (fn [[k d]]
                [:div
                 [:div.py-1 (h/link ["packages" (:id d)] [:span.space-x-2.flex [:i.fa-solid.fa-folder.text-orange-300] [:span (name k) "@" (:version d)]])]
                 (when-let [deps (:deps d)]
                   [:div.px-8  (render-deps deps)])])))))

(defn package-html [context {{id :id} :route-params}]
  (let [pkg (pg.repo/read context {:table "package_version" :match {:id id}})
        deps-tree (far.package/package-deps-tree context pkg)]
    (h/hiccup-response
     [:div.px-4

      [:div.flex.space-x-4
       (canonicals-tabs context id)
       [:div.flex-1
        [:br]
        (h/breadcramp "packages" "packages" "#" (:name pkg))
        (h/h1 (:name pkg) "@" (:version pkg))
        [:p.text-sm.text-gray-600 (:description pkg)]

        (h/tabbed-content
         ["Deps" [:div.px-8.py-4.bg-gray-100.border.rounded (render-deps deps-tree)]]
         ["Package.json" (h/json-block (dissoc pkg :all_dependencies))]
         ["Dependant" "TBD"]

         )

        ]]


      ])))

(defn valuesets-html [context {{id :id} :route-params}]
  (let [pkg       (pg.repo/read context {:table "package_version" :match {:id id}})
        valuesets (pg.repo/select context {:table "valueset" :select [:id :name :url :version :processing] :match {:package_id id}})]
    (h/hiccup-response
     [:div.px-4
      [:div.flex.space-x-4
       (canonicals-tabs context id)
       [:div.flex-1
        (h/breadcramp "packages" "packages" (:id pkg) (:name pkg) "#" "valuesets")
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
      [:div.flex.space-x-4
       (canonicals-tabs context id)
       [:div.flex-1
        (h/breadcramp "packages" "packages" (:id pkg) (:name pkg) "#" "valuesets")
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
      [:div.flex.space-x-4
       (canonicals-tabs context id)
       [:div.flex-1
        (h/breadcramp "packages" "packages" (:id pkg) (:name pkg) "#" "valuesets")
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
            [:span (:name cn)]
            [:span.text-sm.text-gray-500
             [:b "content: "] (:content cn)])

      [:p.text-sm.text-gray-400 (:url cn)]

      (h/tabbed-content
       ["Concepts"
        (h/table [:code :display] concepts
                 (fn [c] [(:code c) (:display c) (get-in c [:resource :definition]) (str/join ", " (mapv name (keys (dissoc (:resource c) :definition))))]))]
       ["Defnition" (h/yaml-block (dissoc cn :concept))])

      ]

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
               [:div.flex.space-x-2 [:span.font-semibold.text-gray-700 sl-name]  "[" (get sl :min 0) "," (get sl :max "*")"]"
                (when-let [m (:match sl)]
                  [:span.font-mono "~=" (h/code m)])]
               [:div.pl-8
                (render-schema (:schema sl))]]))
       (into [:div ])))



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
                            " &lt; " (last (str/split (or (get el :url) "") #"/")) " &gt;")
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
                                          "preferred" "!?"
                                          "example" "?"
                                          )
                         " "
                         (last (str/split (get b :valueSet "?") #"/")) " &gt; "]))
                     (when-let [r (:refers el)]
                       (list
                        [:span " &lt; " (->> r (mapv (fn [x] (last (str/split (or (:profile x) (:resource x)) #"/")))) (str/join " | ")) " &gt;"]))
                     (when-let [p (:pattern el)]
                       (list
                        [:span.font-mono " = " (h/code (:value p))]))
                     ]
                    (when-let [disc (get-in el [:slicing :discriminator])]
                      [:span.font-mono  " ~= " (h/code disc)])]
                   (when-let [sl (:slicing el)]
                     [:div.pl-5.font-mono (render-slices sl)])
                   (when (:elements el)
                     [:div.pl-5 (render-schema el)])]))))
     (into [:div.text-sm]))))

(defn sd-type [{d :derivation t :type k :kind}]
  (cond
    (and (= "constraint" d) (= k "resource")) "Profile"
    (and (= "constraint" d) (= t "Extension")(= k "complex-type")) "Extension"
    (and (= "specialization" d) (= k "resource")) "Resource"
    (and (= k "resource")) "Resource"
    (and (= k "complex-type")) "Type"
    (and (= k "primitive-type")) "Primitive"
    (and (= k "logical")) "Logical"
    :else (str d "/" t "/" k)))

(defn structuredef-nav [context pkg-id]
  (let [cns (->> (pg.repo/select context {:table "canonical" :match {:package_id pkg-id :resource_type "StructureDefinition"}})
                 (mapv (fn [c] (assoc c ::type (sd-type c))))
                 (group-by ::type))]
    [:div.border-r {:style "width: 20em; max-width: 20em; height: 100vh; overflow-y: auto;"}
     (->> cns
          (mapv (fn [[tp cns]]
                  [tp
                   (->> cns
                        (sort-by :name)
                        (mapv (fn [c]
                                [:a.block.text-sm.text-sky-700.py-1.hover:bg-blue-100.px-4
                                 {:href (h/href ["canonicals" "StructureDefinition" (:id c)])
                                  :hx-push-url "true"
                                  :hx-get (h/href ["canonicals" "StructureDefinition" (:id c)]) :hx-target "#content"}
                                 (last (str/split (or (:url c) (:name c) ) #"/"))]))
                        (into [:div]))]))
          (apply h/accordion)
          )]))

(defn sd-layout [context {{hxt "hx-target" :as hs} :headers} cn content]
  (println hs)
  (h/hiccup-response
   (if hxt
     content
     [:div.flex.space-x-4
      [:div.bg-gray-100 (structuredef-nav context (:package_id cn))]
      content])))

(defn render-differential [schema cn]
  [:div.border.p-4.rounded.bg-gray-100
   [:p.text-gray-500.font-mono.text-xs "// " (:description schema)]
   [:span.font-mono.text-sm.flex.space-x-2
    [:span.font-semibold.text-green-800
     (str/lower-case (sd-type cn))]
    [:span.text-red-800.font-semibold
     (str/replace (:name schema) #"(\s+|-)" "")]
    [:span.font-normal "extends"]
    [:span.text-sky-700 (:type schema)  "&lt;" (last (str/split (get schema :base "") #"/")) "&gt; "]]
   [:div.pl-4
    (render-schema schema)]])

(defn render-dependant [dependant]
  (h/table
   []
   (->> dependant (sort-by :name))
   (fn [c]
     [(str (:package_name c) "@" (:package_version c))
      (:type c)
      (h/link ["canonicals" (:resource_type c) (:definition_id c)] (last (str/split (:definition c) #"/")))])))

(defn structuredef-html [context {{ id :id} :route-params :as request}]
  (let [cn (pg.repo/read context {:table "structuredefinition" :match {:id id}})
        schema (fhir.schema.transpiler/translate cn)
        dependant (pg.repo/select context {:table "canonical_deps" :match {:dep_id id}})]
    (sd-layout
     context request cn
     [:div#content.px-4.flex-1.py-4
      (h/breadcramp
       "packages" "packages"
       (:package_id cn) (:package_name cn)
       "StructureDefinition" "StructureDefinition"
       "#" (:name cn))

      (h/h1 [:span (sd-type cn)] [:span (:name cn)])

      [:p.text-sm.text-gray-400 (:url cn)]

      (h/tabbed-content
       ["Differential" (render-differential schema cn)]
       ["Schema" (h/formats-block schema)]
       ["StructureDefinition" (h/formats-block cn)]
       [(str "Dependant (" (count dependant) ")") (render-dependant dependant)])])))

(defn structuredefs-html [context {{id :id} :route-params}]
  (let [pkg       (pg.repo/read context {:table "package_version" :match {:id id}})
        cns (pg.repo/select context {:table "canonical" :match {:package_id id :resource_type "StructureDefinition"}})]
    (h/hiccup-response
     [:div.px-4
      [:div.flex.space-x-4
       (canonicals-tabs context id)
       [:div.flex-1
        (h/breadcramp "packages" "packages" (:id pkg) (:name pkg) "#" "valuesets")
        (h/h1 "StructureDefinition")
        (h/table [] (->> cns (sort-by :name))
                 (fn [c]
                   [(h/link ["canonicals" "StructureDefinition" (:id c)] (str (or (:name c) (:url c))))
                    (:version c)
                    (:content c)]))]]])))

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

  (http/register-endpoint context {:method :get :path "/packages/:id/StructureDefinition" :fn #'structuredefs-html})
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
