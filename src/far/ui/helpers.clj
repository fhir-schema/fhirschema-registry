(ns far.ui.helpers
  (:require
   [system]
   [hiccup.core]
   [clj-yaml.core]
   [cheshire.core]
   [clojure.pprint]
   [clojure.string :as str]))

(system/defmanifest {:description "UI for far"})

(def open-tab-fn "
var  tabs = {};
function open_tab(id, cmp_id) {
  var cur = tabs[cmp_id];
  console.log('open-tab', id,cur)
  if( cur == id) { return; }
  var new_tab = document.getElementById(id);
  new_tab.style.display = 'block';
  var new_tab_node = document.getElementById('tab-' + id);
  console.log('?', 'tab-' + id, new_tab_node);
  new_tab_node.classList.add('border-sky-500');
  new_tab_node.classList.remove('border-transparent');
  var old_tab = document.getElementById(cur);
  old_tab.style.display = 'none';
  var old_tab_node = document.getElementById('tab-' + cur )
  old_tab_node.classList.remove('border-sky-500')
  old_tab_node.classList.add('border-transparent');
  tabs[cmp_id] = id;
}
"

  )

(defn top-nav []
  [:nav {:class "bg-gray-800"}
   [:div {:class "px-2 sm:px-4 lg:px-4"}
    [:div {:class "relative flex h-12 items-center justify-between"}
     [:div {:class "flex flex-1 items-center justify-center sm:items-stretch sm:justify-start"}
      [:div {:class "flex shrink-0 items-center"}
       [:img {:class "h-8 w-auto", :src "https://tailwindui.com/plus/img/logos/mark.svg?color=indigo&shade=500", :alt "Your Company"}]]
      [:div {:class "hidden sm:ml-6 sm:block"}
       [:div {:class "flex space-x-4"}
        [:a {:href "/packages",   :class "rounded-md px-3 py-2 text-sm font-medium text-gray-300 hover:bg-gray-700 hover:text-white", :aria-current "page"} "Packages"]
        [:a {:href "/canonicals", :class "rounded-md px-3 py-2 text-sm font-medium text-gray-300 hover:bg-gray-700 hover:text-white"} "Canonicals"]]]]]]])


(defn hx-target [request]
  (get-in request [:headers "hx-target"]))

(defn hiccup-response [request body]
  {:status 200
   :headers {"content-type" "text/html"}
   :body (hiccup.core/html
          (if (hx-target request)
            body
            [:html
             [:head
              [:script {:src "https://cdn.tailwindcss.com"}]
              [:script {:src "https://unpkg.com/htmx.org@2.0.3" :integrity "sha384-0895/pl2MU10Hqc6jd4RvrthNlDiE9U1tWmX7WRESftEDRosgxNsQG/Ze9YMRzHq" :crossorigin "anonymous"}]
              [:script {:src "https://kit.fontawesome.com/d9939909b1.js" :crossorigin "anonymous"}]
              [:link {:rel "stylesheet", :href "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/default.min.css"}]
              [:script {:src "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"}]
              [:script {:src "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/clojure.min.js"}]
              [:script open-tab-fn]]

             [:body {:hx-boost "true"}
              body
              [:script "hljs.highlightAll();"]]]))})

(defn layout [context request fragments-map]
  (if-let [trg (hx-target request)]
    (if-let [trg-fn (get fragments-map (keyword trg))]
      {:status 200 :body (hiccup.core/html (trg-fn))}
      {:status 500 :body (str "Error: no fragment for " trg)})
    (hiccup-response
     request
     [:div (top-nav)
      [:div.flex
       (when-let [tnav (:topnav fragments-map)]
         [:div#topnav.bg-gray-200.border-r (if (fn? tnav) (tnav) tnav)])
       (when-let [nav (:navigation fragments-map)]
         [:div#nav.bg-gray-100.border-r (if (fn? nav) (nav) nav)])
       (when-let [cnt (:content fragments-map)]
         [:div#content.flex-1 (if (fn? cnt) (cnt) cnt)])]])))

(defn table [columns rows & [row-fn]]
  (let [row-fn (or row-fn (fn [x] (->> columns (mapv (fn [k] (get x k))))))]
    (->> rows
         (mapv (fn [c] (->> (row-fn c)
                           (mapv (fn [x] [:td.p-2.border x]))
                           (into [:tr]))))
         (into [:tbody])
         (conj [:table.text-sm.mt-2
                (when (seq columns)
                                 [:thead
                                  (->> columns (mapv (fn [c] [:th.border.px-2.py-1.bg-gray-100.font-semibold (name c)]))
                                       (into [:tr])
                                       )]
                                 )]))))


(def bc-home [:svg {:class "size-5 shrink-0", :viewBox "0 0 20 20", :fill "currentColor", :aria-hidden "true", :data-slot "icon"} [:path {:fill-rule "evenodd", :d "M9.293 2.293a1 1 0 0 1 1.414 0l7 7A1 1 0 0 1 17 11h-1v6a1 1 0 0 1-1 1h-2a1 1 0 0 1-1-1v-3a1 1 0 0 0-1-1H9a1 1 0 0 0-1 1v3a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1v-6H3a1 1 0 0 1-.707-1.707l7-7Z", :clip-rule "evenodd"}]])

(def bc-delim
  [:svg {:class "size-5 shrink-0 text-gray-400", :viewBox "0 0 20 20", :fill "currentColor", :aria-hidden "true", :data-slot "icon"}
   [:path {:fill-rule "evenodd", :d "M8.22 5.22a.75.75 0 0 1 1.06 0l4.25 4.25a.75.75 0 0 1 0 1.06l-4.25 4.25a.75.75 0 0 1-1.06-1.06L11.94 10 8.22 6.28a.75.75 0 0 1 0-1.06Z", :clip-rule "evenodd"}]])

(defn bc-li [& xs]
  [:li {:class "flex"}
   (into [:div {:class "flex items-center"}] xs)])

(defn bc-container [& xs]
  [:nav
   {:class "flex", :aria-label "Breadcrumb"}
   (into [:ol {:role "list", :class "flex space-x-4"}] xs)])

(defn href [path & [params]]
  (str "/" (str/join "/" path)
       (when params
         (str "?" (str/join "&" (mapv (fn [[k v]] (str (name k) "=" v)) params))))))

(defn bc-link [path & xs]
  (into [:a {:href (if (= (last path) "#") "#" (str "/" (str/join "/" path)))
             :class "ml-4 text-sm font-medium text-gray-500 hover:text-gray-700"}] xs))

(defn breadcramp [& pairs]
  (->>
   (partition 2 pairs)
   (reduce (fn [acc [item nm]]
             (-> acc
                 (update :items conj [(conj (:path acc) item) nm])
                 (update :path conj item)))
           {:path [] :items []})
   :items
   (map-indexed
    (fn [i [url nm]]
      (if (= i 0)
        (bc-li bc-home  (bc-link url nm))
        (bc-li bc-delim (bc-link url nm)))))
   (bc-container)))

(defn yaml-block [data]
  [:pre.p-2.text-xs.bg-gray-100
   [:code {:class "language-yaml"} (clj-yaml.core/generate-string data)]])

(defn json-block [data]
  [:pre.p-2.text-xs.bg-gray-100
   [:code {:class "language-json"}
    (cheshire.core/generate-string data {:pretty true})]])

(defn edn-block [data]
  [:pre.p-2.text-xs.bg-gray-100
   [:code {:class "language-clojure"}
    (with-out-str (clojure.pprint/pprint data))
    ]])


(defn link [path title]
  [:a.text-sky-600 {:href (str "/" (str/join "/" path))} title])

(defn h1 [& body]
  (into [:h1.text-xl.my-3.flex.items-baseline.space-x-4] body))

(defn h2 [& body]
  (into [:h1.text-lg.my-3.flex.items-baseline.space-x-4] body))

(defn search-input [{p :path ph :placeholder}]
  [:div.flex.space-x-4.items-center
   [:i.fa-duotone.fa-regular.fa-magnifying-glass.text-gray-500]
   [:input
    {:class "col-start-1 row-start-1 block w-full rounded-md bg-white py-1.5 pl-10 pr-3 text-base text-gray-900 outline outline-1 -outline-offset-1 outline-gray-300 placeholder:text-gray-400 focus:outline focus:outline-2 focus:-outline-offset-2 focus:outline-indigo-600 sm:pl-9 sm:text-sm/6"
     :type "search", :name "search",
     :placeholder (or ph "Search..")
     :hx-get (str "/" (str/join "/" p))
     :hx-trigger "input changed delay:500ms, search",
     :hx-target "#search-results",
     :hx-indicator ".htmx-indicator"}]])

(defn tabs [& tabs]
  [:div
   [:div
    {:class "grid grid-cols-1 sm:hidden"}
    [:select {:aria-label "Select a tab", :class "col-start-1 row-start-1 w-full appearance-none rounded-md bg-white py-2 pl-3 pr-8 text-base text-gray-900 outline outline-1 -outline-offset-1 outline-gray-300 focus:outline focus:outline-2 focus:-outline-offset-2 focus:outline-indigo-600"}
     [:option "My Account"]
     [:option "Company"]
     [:option {:selected ""} "Team Members"]
     [:option "Billing"]]
    [:svg {:class "pointer-events-none col-start-1 row-start-1 mr-2 size-5 self-center justify-self-end fill-gray-500", :viewBox "0 0 16 16", :fill "currentColor", :aria-hidden "true", :data-slot "icon"}
     [:path {:fill-rule "evenodd", :d "M4.22 6.22a.75.75 0 0 1 1.06 0L8 8.94l2.72-2.72a.75.75 0 1 1 1.06 1.06l-3.25 3.25a.75.75 0 0 1-1.06 0L4.22 7.28a.75.75 0 0 1 0-1.06Z", :clip-rule "evenodd"}]]]
   [:div {:class "hidden sm:block"}
    [:div {:class "border-b border-gray-200"}
     (->> tabs
          (mapv (fn [[path & items]]
                  (into [:a {:href (href path) :class "whitespace-nowrap border-b-2 border-transparent px-1 py-4 text-sm font-medium text-gray-500 hover:border-gray-300 hover:text-gray-700"}]
                        items)))
          (into [:nav {:class "-mb-px flex space-x-8", :aria-label "Tabs"}]))]]])


(defn nav [& tabs]
  [:nav {:class "text-sm flex flex-col px-2", :aria-label "Sidebar"}
   (->> tabs
        (mapv (fn [[path & items]]
                (into
                 [:li
                  (into [:a {:href (href path) :class "text-xs whitespace-nowrap group flex gap-x-2 rounded-md py-1 px-2 text-sm/6 font-semibold text-gray-700 hover:bg-gray-50 hover:text-indigo-600"}]
                        items)])))
        (into [:ul {:role "list", :class "-mx-2 space-y-1"}]))])


(defn get-id [x]
  (str "v" (str/replace (hash x) #"-" "_")))

(defn tabbed-content [& tabs]
  (let [cmp-id (get-id tabs)]
    [:div.container
     [:script (str "tabs['" cmp-id "']='"  (get-id (first tabs)) "'; ")]
     (->> tabs
          (map-indexed
           (fn [idx [t & _ :as tb]]
             (let [tid (get-id tb)]
               [:a {:id (str "tab-" tid)
                    :href "#" #_(str "#" tid)
                    :onClick (str "open_tab('" tid "', '" cmp-id "')")
                    :class (cond-> "whitespace-nowrap border-b-2 px-1 pb-1 pt-2 text-sm font-medium text-gray-500 hover:border-gray-300 hover:text-gray-700"
                             (= 0 idx) (str " border-sky-500")
                             (< 0 idx) (str " border-transparent"))}
                t])))
          (into [:nav {:class "-mb-px flex space-x-6", :aria-label "Tabs"}]))
     (->> tabs
          (map-indexed
           (fn [i [t & body :as tb]]
             (into [:div {:id (get-id tb) :style (if (= i 0) "" "display: none;")}] body)))
          (into [:div.content]))]))


(defn formats-block [cn]
  [:div.bg-gray-100.px-2.border
   (tabbed-content
    ["yaml"  (yaml-block cn)]
    ["json"    (json-block cn)]
    ["edn"    (edn-block cn)])])

(defn accordion [& tabs]
  (->> tabs
       (map
        (fn [[t & body]]
          (into [:details [:summary.text-gray-600.border-b.py-2.px-4.hover:bg-gray-100 t]] body)))
       (into [:div.content.mt-2])))

;; (tabbed-content
;;  ["Title1" "Body2"]
;;  ["Title1" "Body2"])

(defn code [x]
  (cond
    (map? x)
    (-> (->> x
             (mapv (fn [[k v]] [:span [:span.font-medium.text-black (name k) ":"] (code v)]))
             (interpose ",")
             (into [:span [:span.text-gray-400 "{"]]))
        (conj [:span.text-gray-400 "}"]))
    (vector? x)
    (-> (->> x
             (mapv (fn [v] (code v)))
             (interpose ",")
             (into [:span [:span.text-gray-400 "["]]))
        (conj [:span.text-gray-400 "]"]))
    :else [:span.text-gray-600 (pr-str x)]))
