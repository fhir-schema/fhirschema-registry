(ns http.core
  (:require
   [cheshire.core]
   [clojure.string :as str]
   [cognitect.transit :as transit]
   [hiccup.page]
   [org.httpkit.server :as server]
   [ring.middleware.head]
   [ring.middleware.params]
   [ring.middleware.cookies]
   [ring.util.codec :as codec]
   [ring.util.io]
   [ring.util.response]
   [cheshire.core]
   [rpc])
  (:import [java.io BufferedWriter OutputStreamWriter  ByteArrayInputStream ByteArrayOutputStream]))

(defn handle-static [{meth :request-method uri :uri :as req}]
  (let [opts {:root "public"
              :index-files? true
              :allow-symlinks? true}
        path (subs (codec/url-decode (:uri req)) 8)]
    (-> (ring.util.response/resource-response path opts)
        (ring.middleware.head/head-response req))))

(defn parse-body [b]
  (when b
    (cond (string? b) (cheshire.core/parse-string b keyword)
          (instance? java.io.InputStream b) (cheshire.core/parse-stream b keyword)
          :else b)))

(defn render-index [ztx req]
  {:body "ok"})

(defn resolve-operation [meth uri]
  (let [parts  (rest (str/split uri #"/"))
        cnt (count parts)]
    (cond
      (= 1 cnt) {:op (str (name meth) "-" (str/lower-case (first parts)))}
      (= 2 cnt) (let [id (second parts)]
                  (if (str/starts-with? id "$")
                    {:op (str (name meth) "-" (str/lower-case (first parts)) "-" (subs id 1))}
                    {:op (str (name meth) "-" (str/lower-case (first parts)) "-inst")
                     :params {:id id}}))
      (= 3 cnt) (let [id (second parts)]
                  {:op (str (name meth) "-" (str/lower-case (first parts)) "-" (nth parts 2))
                   :params {:id id}}))))

(comment
  (resolve-operation :get "/Package")
  (resolve-operation :get "/Package/id")
  (resolve-operation :get "/Package/$op")
  (resolve-operation :get "/Patient/pt-1/history")
  )

(defn parse-params [params encoding]
  (let [params (when params (reduce (fn [acc [k v]] (assoc acc (keyword k) v)) {} (codec/form-decode params encoding)))]
    (if (map? params) params {})))

(parse-params "a=2%20;" "UTF-8")

(defn dispatch [ztx {meth :request-method uri :uri :as req}]
  (cond
    (and (contains? #{:get :head} meth) (str/starts-with? (or uri "") "/static/"))
    (handle-static req)

    (and (= "/" uri) (= :get meth))
    (render-index ztx req)

    (and (= "/" uri) (= :post meth))
    (rpc/proc ztx (parse-body (:body req)) req)

    :else
    (let [query-params (parse-params (:query-string req) "UTF-8")]
      (if-let [op (resolve-operation meth uri)]
        (update (rpc/op ztx (assoc (merge req op) :query-params query-params))
                :body (fn [x] (when x (cheshire.core/generate-string x))))
        {:status 404}))))

(defn start [ztx config]
  (let [port (or (:port config) 7777)]
    (println :http port)
    (swap! ztx assoc :http (server/run-server (fn [req] (#'dispatch ztx req)) {:port port}))))

(defn stop [ztx]
  (when-let [stop (:http @ztx)]
    (stop)
    (swap! ztx dissoc :http)))


(comment
  (require '[org.httpkit.client :as http])

  (defmethod rpc/op :get-package
    [ztx req]
    {:status 200
     :body {:message "ok"}})

  (def ztx (atom {}))

  (start ztx {:port 7776})
  (stop ztx)

  (slurp (:body @(http/get "http://localhost:7776")))

  (slurp (:body @(http/get "http://localhost:7776/Package")))
  (slurp (:body @(http/get "http://localhost:7776/Package")))








  )
