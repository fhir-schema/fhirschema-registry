(ns far.ui
  (:require
   [system]
   [clojure.string :as str]
   [pg.repo]
   [http]
   [pg]
   [far.ui.helpers :as h]
   [far.ui.fhirschema :as ui-fs]
   [far.package]
   [far.tx]
   [far.schema]
   [cheshire.core]
   [fhir.schema.transpiler]))

(system/defmanifest {:description "UI for far"})

(defn hx-target [request]
  (get-in request [:headers "hx-target"]))

(defn elipse [txt & [cnt]]
  (when txt
    [:span.hover:bg-yellow-100 {:title txt}
     (subs txt 0 (min (count txt) (or cnt 70))) "..."]))

(defn render-packages [pkgs]
  (->> pkgs
       (map (fn [p]
              [:a.px-4.py-2.border-b.space-x-2.flex.items-baseline {:href (str "/packages/" (:id p))}
               [:i.fa-regular.fa-folder.text-gray-500]
               [:span (:name p)] "@" [:span (:version p)]
               [:span.text-sm.text-gray-400 (elipse (:description p))]]))
       (into [:div#search-results])))

(defn canonical-breadcrump [context cn]
  (let [rt (or (:resource_type cn) (:resourceType cn))]
    (h/breadcramp "packages" "packages" (:package_id cn) (:package_name cn) rt rt "#" (:name cn))))

(defn canonical-dependant-tab [context cn]
  (let [dependant (pg.repo/select context {:table "canonical_deps" :match {:dep_id (:id cn)}})]
    [(str "Dependant (" (count dependant) ")")
     (h/table
      [:package :resource] dependant
      (fn [x]
        [(str (:package_name x) "@" (:package_version x))
         (h/link ["canonicals" (:definition_resource_type x) (:definition_id x)] (:definition x))]))]))

(defn canonical-deps-tab [context cn]
  (let [ deps (->> (pg.repo/select context {:table "canonical_deps" :match {:definition_id (:id cn)}})
                   (sort-by (fn [x] [(:type x) (:url x)])))]
    [(str "Deps (" (count deps) ")")
     (h/table [] deps
              (fn [d]
                [(str (:dep_package_id d))
                 (:type d)
                 [:a.text-sky-500 {:href (str "/canonicals/" (:resource_type d) "/" (:dep_id d))} (:url d) (when-let [v (:version d)] (str "|" v))]]))]))

(defn packages-html [context request]
  (let [pkgs (pg.repo/select context {:table "package_version" :order-by [:pg/desc :name :version]})]
    (h/hiccup-response request 
     [:div.px-4
      [:br]
      (h/search-input {:path ["packages" "search"]})
      [:br]
      (render-packages pkgs)])))

(defn packages-search-html [context {{q :search} :query-params :as request}]
  (let [pkgs (pg.repo/select context {:table "package_version"
                                      :where (when-not (str/blank? q)
                                               [:ilike :name [:pg/param (str "%" (str/replace q #"\s+" "%") "%")]])
                                      :order-by [:pg/desc :name :version]})]
    (h/hiccup-response request 
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
  (let [pkg (pg.repo/read context {:table "package_version" :match {:id pkg-id}})
        prefix ["packages" pkg-id]
        stats (pg/execute! context {:sql [canonicals-stats-sql pkg-id]})]
    [:div.text-xs.px-2 {:style "width: 20em; max-width: 20em;"}
     [:a.px-2.py-2.bg-sky-100.rounded-md.border.my-2.block  {:href (h/href ["packages" pkg-id])}
      (str (:name pkg) "@" (:version pkg))]
     [:div.py-2
      (->> stats
           (mapv (fn [{rt :resource_type cnt :count}]
                   [(into prefix [rt])
                    [:span.text-xs rt]
                    [:span {:class "ml-auto w-9 min-w-max rounded-full bg-white px-1.5 py-0.5 text-center text-xs font-medium whitespace-nowrap text-gray-500 ring-1 ring-gray-200 ring-inset", :aria-hidden "true"} 
                     (str cnt)]
                    ]))
           (apply h/nav))]]))

(defn render-deps [deps]
  (if (empty? deps)
    [:p.text-gray-500 "No dependencies"]
    (->> deps
         (map (fn [[k d]]
                [:div
                 [:div.py-1 (h/link ["packages" (:id d)] [:span.space-x-2.flex [:i.fa-solid.fa-folder.text-orange-300] [:span (name k) "@" (:version d)]])]
                 (when-let [deps (:deps d)]
                   [:div.px-8  (render-deps deps)])])))))

(defn package-html [context {{id :id} :route-params :as request}]
  (let [pkg (pg.repo/read context {:table "package_version" :match {:id id}})
        deps-tree (far.package/package-deps-tree context pkg)]
    (h/layout
     context request
     {:topnav (fn [] (canonicals-tabs context id))
      :navigation nil
      :content (fn []
                 [:div.p-4
                  (h/breadcramp "packages" "packages" "#" (:name pkg))
                  (h/h1 (:name pkg) "@" (:version pkg))
                  [:p.text-sm.text-gray-600 (:description pkg)]

                  (h/tabbed-content
                   ["Deps" [:div.px-8.py-4.bg-gray-100.border.rounded (render-deps deps-tree)]]
                   ["Package.json" (h/json-block (dissoc pkg :all_dependencies))]
                   ["Dependant" "TBD"])])})))



(defn sd-nav [context pkg-id]
  (let [cns (->> (pg.repo/select context {:table "canonical" :match {:package_id pkg-id :resource_type "StructureDefinition"}})
                 (mapv (fn [c] (assoc c ::type (ui-fs/sd-type c))))
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
          (apply h/accordion))]))

(defn cn-nav [context pkg-id rt]
  (let [cns (->> (pg.repo/select context {:table "canonical" :match {:package_id pkg-id :resource_type rt}})
                 (sort-by (fn [c] (last (str/split (or (:url c) (:name c) ) #"/")))))]
    [:div.text-xs.p-2 {:style "width: 20em; max-width: 20em; height: 100vh; overflow-y: auto;"}
     #_[:div.px-4.py-2.border-b.font-semibold.text-gray-500 rt]
     [:div [:input.bg-gray-100.my-1.px-2.py-1.border.block.w-full {:placeholder "Search..." :onkeyup "filterByClass('cn-item', event)"} ]]
     [:div.px-2
      (->> cns
           (map (fn [c]
                  (h/nav-link
                   {:class "cn-item"
                    :href (h/href ["canonicals" (:resource_type c) (:id c)])
                    :hx-push-url "true"
                    :hx-get (h/href ["canonicals" (:resource_type c) (:id c)]) :hx-target "#content"}
                   (last (str/split (or (:url c) (:name c) ) #"/"))))))]]))

(defn canonical-other-versions-tab [context cn]
  (let [other-versions (pg.repo/select context {:table "canonical" :match {:url (:url cn) :resource_type (:resourceType cn)}})]
    [(str "Other versions (" (count other-versions) ")")
     (h/table [:pacakge :canonical :version] other-versions (fn [c]
                                  [(str (:package_name c) "@" (:package_version c))
                                   (h/link ["canonicals" "CodeSystem" (:id c)] (:url c))
                                   (:version c)]
                                  ))]))

(defmulti render-canonical (fn [_context cn] (:resourceType cn)))


(defmethod render-canonical "ValueSet"
  [context cn]
  (let [expand-sql (far.tx/expand-query context cn)
        expansion (far.tx/expand context cn)]
    (h/tabbed-content
     ["Compose"   (h/formats-block (:compose cn))]
     ["Valueset"  (h/formats-block cn)]
     (canonical-deps-tab context cn)
     (canonical-dependant-tab context cn)
     (canonical-other-versions-tab context cn)
     ["Expansion"  (h/table [:system :code :display] expansion)]
     ["Expansion SQL"  (h/edn-block expand-sql)])))


(defmethod render-canonical "CodeSystem"
  [context cn]
  (let [concepts (->> (far.tx/codesystem-concepts context cn)
                      (sort-by (fn [c] (or (get-in c [:resource :is-a]) [(:code c)]))))
        other-versions (pg.repo/select context {:table "canonical" :match {:url (:url cn) :resource_type "CodeSystem"}})]
    (h/tabbed-content
     ["Concepts"
      (h/table [:code :display :defnition :keys] concepts
               (fn [c] [(:code c) (:display c)
                       (elipse (get-in c [:resource :definition]) 40)
                       (str/join ", " (mapv name (keys (dissoc (:resource c) :definition))))]))]
     ["Defnition" (h/yaml-block cn)]
     (canonical-deps-tab context cn)
     (canonical-dependant-tab context cn)
     (canonical-other-versions-tab context cn))))

(defmethod render-canonical "StructureDefinition"
  [context cn]
  (let [schema (fhir.schema.transpiler/translate cn)
        schema* (far.schema/enrich context schema)]
    (h/tabbed-content
     ["Differential" (ui-fs/render-differential schema* cn)]
     ["Schema" (h/formats-block schema)]
     ["Schema*" (h/formats-block schema*)]
     ["StructureDefinition" (h/formats-block cn)]
     (canonical-deps-tab context cn)
     (canonical-dependant-tab context cn)
     (canonical-other-versions-tab context cn))))

(defmethod render-canonical :default
  [context cn]
  (h/tabbed-content
   ["Resource" (h/formats-block cn)]
   (canonical-deps-tab context cn)
   (canonical-dependant-tab context cn)
   (canonical-other-versions-tab context cn)))

(defn cn-content [context cn]
  [:div.px-4
   (h/h1 [:span (:resourceType cn)] [:span (:name cn)])

   [:p.text-sm.text-gray-400 (:url cn)]
   (render-canonical context cn)])

(defn canonical-html [context {{rt :resource_type id :id} :route-params :as request}]
  (let [cn (pg.repo/read context {:table (str/lower-case rt) :match {:id id}})]
    (h/layout
     context request
     {:topnav (fn [] (canonicals-tabs context (:package_id cn)))
      :navigation #(cn-nav context (:package_id cn) (:resourceType cn))
      :content    #(cn-content context cn)})))

(defmulti render-canonicals-stats (fn [context pkg-id rt] rt))

(defmethod render-canonicals-stats
  :default
  [context pkg-id rt]
  (let [cnt (pg/execute! context {:dsql {:select {:count [:pg/sql "count(*)"]}
                                         :from (keyword (str/lower-case (name rt)))
                                         :where [:= :package_id [:pg/param pkg-id]]}})]
    [:div.p-4
     [:div.text-xl.text-bold (:count (first cnt))]]))

(defn canonicals-html [context {{rt :resource_type id :id} :route-params :as request}]
  (h/layout
   context request
   {:topnav (fn [] (canonicals-tabs context id))
    :navigation #(cn-nav context id rt)
    :content    (render-canonicals-stats context id rt)}))




;;TODO prefix search
(defn canonicals-lookup [context request]
  (h/hiccup-response request
                     (if (hx-target request)
                       (let [q (get-in request [:query-params :search])
                             cns (pg.repo/select
                                  context {:table "canonical"
                                           :where (when-not (str/blank? q)
                                                    [:ilike :url [:pg/param (str "%" (str/replace q #"\s" "%") "%")]])
                                           :order-by :url
                                           :limit 100})]
                         (h/table [] cns
                                  (fn [r]
                                    [(:resource_type r)
                                     (h/link ["packages" (:package_id r)] (str (:package_name r) "@" (:package_version r)))

                                     (h/link ["canonicals" (:resource_type r) (:id r)] (:url r))])))
                       [:div.px-4
                        [:br]
                        (h/search-input {:path ["canonicals"]})
                        [:br]
                        [:div#search-results]])))


(defn mount-routes [context]
  (http/register-endpoint context {:method :get :path "/packages" :fn #'packages-html})
  (http/register-endpoint context {:method :get :path "/packages/search" :fn #'packages-search-html})
  (http/register-endpoint context {:method :get :path "/packages/:id" :fn #'package-html})

  (http/register-endpoint context {:method :get :path "/packages/:id/:resource_type" :fn #'canonicals-html})
  (http/register-endpoint context {:method :get :path "/canonicals/:resource_type/:id" :fn #'canonical-html})
  (http/register-endpoint context {:method :get :path "/canonicals" :fn #'canonicals-lookup})

  )


(system/defstart
  [context _]
  (mount-routes context))

;; TODO: introduce layouts and middlewares

(comment
  (def context far/context)
  (mount-routes context)

  (http/register-endpoint context {:method :get :path "/packages/:id/ValueSet" :fn #'valuesets-html})

  (http/get-routes context)

  (pg.repo/read context {:table "structuredefinition" :match {:id "3204c076-36c3-5f21-9508-244e319170f1"}})

  (->> (pg.repo/select context {:table "canonical_deps" :match {:definition_id "3204c076-36c3-5f21-9508-244e319170f1"}})
       (group-by :url))

  )
