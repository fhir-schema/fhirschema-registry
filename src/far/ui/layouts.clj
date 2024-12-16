(ns far.ui.layouts)

(defn render [context state html]
  (cond
    (fn? (first html))
    (render context state (apply (first html) context state (rest html)))

    (keyword (first html))
    (->> (rest html)
         (mapv (fn [e]
                 (if (vector? e)
                   (render context state e)
                   e)))
         (into [(first html)]))

    :else html))


(defn query [context opts]
  [{:id "u1"} {:id "u2"}]
  )

(defn find-user [context st]
  (assoc (:user st) :name (str "name-" (get-in st [:user :id]))))

;; produce action
(defn select-user [state u]
  (assoc state :user u))

(defn left-navigation [context state]
  (let [users (query context {})]
    [:ul
     (for [u users]
       {:onClick [#'select-user u]})]))

(defn resource-view [context state]
  (let [user (find-user context state)]
    [:resource [:b (:name user)]]))

(defn top-nav [context state]
  [::top-nav])

(defn layout [context state body]
  [:layout
   [top-nav]
   body])

(defn page [context state]
  [layout
   [:div
    [left-navigation ]
    [resource-view ]]])

(def ctx {})
(def state {:user {:id "u1"}})

(render ctx state (page ctx state))

(def state2 (select-user state {:id "u2"}))
state2

(render ctx state2 (page ctx state))

;; page state st1=>st2
;; (render st1) -> (render st2) -> diff - to apply
;; if no st1 - just render
