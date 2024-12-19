(ns far.ui.fhirschema
  (:require [clojure.string :as str]
            [far.ui.helpers :as h]))


(defn url-to-name [url]
  (last (str/split (or url "") #"/")))

(defn generic [s]
  [:span.flex.items-baseline.inline
   [:span.text-gray-400 "&lt;"] s [:span.text-gray-400 "&gt;"]])

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
                        [:i.fa-sharp.fa-solid.fa-asterisk.text-red-600.text-xs.relative.-top-1] )
                      [:span.text-gray-400 ":"]]
                     [:span.text-sky-800.flex.space-x-1.items-center.font-mono
                      [:span.whitespace-nowrap.flex.space-x-1
                       (when-let [tp (:type el)]
                         (if-let [tpr (:type_ref el)]
                           [:a.px-2.rounded.hover:bg-blue-100
                            {:href (h/href ["canonicals" (:resourceType tpr) (:id tpr)])} tp]
                           [:span.text-red-500 tp]))
                       (generic
                        (let [extnm (url-to-name (:url el))]
                          (if-let [extr (:extension_ref el)]
                            [:a.px-2.rounded.hover:bg-blue-100
                             {:href (h/href ["canonicals" (:resourceType extr) (:id extr)])} extnm]
                            [:span.text-red-500 extnm])))
                       [:span.text-gray-500 "[" (get el :min 0) "," (get el :max "*")  "]"]]]]])))
      (let [elements (:elements schema)
            choices-idx (->> elements (reduce (fn [acc [k v]] (if-let [chs (:choices v)] (assoc acc k chs) acc)) {}))
            choices (map keyword (apply concat (vals choices-idx)))
            elements (->> choices-idx (reduce (fn [els [k chs]]
                                                (->> chs
                                                     (reduce
                                                      (fn [els ch]
                                                        (let [chk (keyword ch)]
                                                          (-> els
                                                              (dissoc chk)
                                                              (assoc-in [k :elements chk] (get els chk)))))
                                                      els)))
                                              elements))]
        (println choices)
        (->>
         (apply dissoc elements choices)
         (sort-by #(:index (second %)))
         (map (fn [[k el]]
                [:div
                 (when-let [s (:short el)] [:span.text-gray-500.text-xs.font-mono "// " s])
                 [:div.flex.space-x-1.py-0.5.items-center
                  [:span.font-mono.font-semibold.text-gray-700
                   (name k)
                   (when (contains? required k)
                     [:i.fa-sharp.fa-solid.fa-asterisk.text-red-600.text-xs.relative.-top-1] )
                   [:span.text-gray-400 ":"]]
                  [:span.text-sky-800.flex.space-x-1.items-center.font-mono
                   [:span.whitespace-nowrap
                    (when-let [tp (:type el)]
                      (if-let [tpr (:type_ref el)]
                        [:a.px-2.rounded.hover:bg-blue-100
                         {:href (h/href ["canonicals" (:resourceType tpr) (:id tpr)])} tp]
                        [:span.text-red-500 tp]))
                    (when-let [cr (:contentReference el)]
                      (str ":" (subs cr 1)))
                    (when (:array el)
                      [:span.text-gray-500 "[" (get el :min 0) "," (get el :max "*")  "]"])]
                   (when-let [b (:binding el)]
                     (list
                      (generic
                       [:span.flex.space-x-1.items-baseline.inline
                        [:span (case (:strength b) "required" "!" "extensible" "+" "preferred" "!?" "example" "?")]
                        (let [vsu (url-to-name (:valueSet b))]
                          (if-let [vsr (:valueSet_ref b)]
                            [:a.px-2.rounded.hover:bg-blue-100
                             {:href (h/href ["canonicals" (:resourceType vsr) (:id vsr)])} vsu]
                            [:span.text-red-500 vsu]))])))
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
                   [:div.pl-5 (render-schema el)])])))))
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


(defn render-differential [schema cn]
  [:div.border.p-4.rounded.bg-gray-100
   [:p.text-gray-500.font-mono.text-xs "// " (:description schema)]
   [:span.font-mono.text-sm.flex.space-x-2
    [:span.font-semibold.text-green-800
     (str/lower-case (sd-type cn))]
    [:span.text-red-800.font-semibold
     (str/replace (:name schema) #"(\s+|-)" "")]
    [:span.font-normal "extends"]
    [:span.text-sky-700.flex.space-x-2
     [:span (:type schema)]
     (generic (let [nm (url-to-name (get schema :base))]
                (if-let [br (:base_ref schema)]
                  [:a.px-2.rounded.hover:bg-blue-100 {:href (h/href ["canonicals" (:resourceType br) (:id br)])} nm]
                  nm)))]]
   [:div.pl-4
    (render-schema schema)]])
