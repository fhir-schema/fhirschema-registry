(ns far.ui.helpers
  (:require
   [system]
   [hiccup.core]
   [clj-yaml.core]
   [cheshire.core]
   [clojure.pprint]
   [clojure.string :as str]))

(system/defmanifest {:description "UI for far"})

(defn hiccup-response [body]
  {:status 200
   :headers {"content-type" "text/html"}
   :body (hiccup.core/html
          [:html
           [:head
            [:script {:src "https://cdn.tailwindcss.com"}]
            [:script {:src "https://unpkg.com/htmx.org@2.0.3" :integrity "sha384-0895/pl2MU10Hqc6jd4RvrthNlDiE9U1tWmX7WRESftEDRosgxNsQG/Ze9YMRzHq" :crossorigin "anonymous"}]
            [:script {:src "https://kit.fontawesome.com/d9939909b1.js" :crossorigin "anonymous"}]
            [:link {:rel "stylesheet", :href "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/default.min.css"}]
            [:script {:src "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"}]
            [:script {:src "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/clojure.min.js"}]]

           [:body {:hx-boost "true"}
            [:div.px-8.py-4
             body
             [:script "hljs.highlightAll();"]]]])})

(defn table [columns rows & [row-fn]]
  (let [row-fn (or row-fn (fn [x] (->> columns (mapv (fn [k] (get x k))))))]
    (->> rows
         (mapv (fn [c] (->> (row-fn c)
                           (mapv (fn [x] [:td.p-2.border x]))
                           (into [:tr]))))
         (into [:tbody])
         (conj [:table]))))


(def bc-home [:svg {:class "size-5 shrink-0", :viewBox "0 0 20 20", :fill "currentColor", :aria-hidden "true", :data-slot "icon"} [:path {:fill-rule "evenodd", :d "M9.293 2.293a1 1 0 0 1 1.414 0l7 7A1 1 0 0 1 17 11h-1v6a1 1 0 0 1-1 1h-2a1 1 0 0 1-1-1v-3a1 1 0 0 0-1-1H9a1 1 0 0 0-1 1v3a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1v-6H3a1 1 0 0 1-.707-1.707l7-7Z", :clip-rule "evenodd"}]])

(def bc-delim [:svg {:class "h-full w-6 shrink-0 text-gray-200", :viewBox "0 0 24 44", :preserveaspectratio "none", :fill "currentColor", :aria-hidden "true"} [:path {:d "M.293 0l22 22-22 22h1.414l22-22-22-22H.293z"}]])

(defn bc-li [& xs]
  [:li {:class "flex"}
   (into [:div {:class "flex items-center"}] xs)])

(defn bc-container [& xs]
  [:nav
   {:class "flex", :aria-label "Breadcrumb"}
   (into [:ol {:role "list", :class "flex space-x-4 rounded-md bg-white px-6 shadow"}] xs)])

(defn href [path]
  (str "/" (str/join "/" path)))

(defn bc-link [path & xs]
  (into [:a {:href (href path) :class "ml-4 text-sm font-medium text-gray-500 hover:text-gray-700"}] xs))

(defn bc-link [path & xs]
  (into [:a {:href (if (= (last path) "#") "#" (str "/" (str/join "/" path))) :class "ml-4 text-sm font-medium text-gray-500 hover:text-gray-700"}] xs))

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
  [:pre.p-4.text-sm.bg-gray-100
   [:code {:class "language-yaml"} (clj-yaml.core/generate-string data)]])

(defn json-block [data]
  [:pre.p-4.text-sm.bg-gray-100
   [:code {:class "language-json"}
    (cheshire.core/generate-string data {:pretty true})]])

(defn edn-block [data]
  [:pre.p-4.text-sm.bg-gray-100
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
  [:nav {:class "flex flex-col px-8", :aria-label "Sidebar"}
   (->> tabs
        (mapv (fn [[path & items]]
                (into
                 [:li
                  (into [:a {:href (href path) :class "whitespace-nowrap group flex gap-x-3 rounded-md py-1 px-4 text-sm/6 font-semibold text-gray-700 hover:bg-gray-50 hover:text-indigo-600"}]
                        items)])))
        (into [:ul {:role "list", :class "-mx-2 space-y-1"}]))])
