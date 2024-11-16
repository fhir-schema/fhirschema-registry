(ns rpc)

(defmulti proc  (fn [ztx req]  (keyword (:method req))))
(defmulti op    (fn [ztx req] (keyword (:op req))))

(defmethod op :default
  [ztx req]
  {:status 404
   :body {:message (str (:op req) " is not found")}})

(defmethod proc :default
  [ztx req]
  {:status 404
   :body {:message (str (:method req) " is not found")}})
