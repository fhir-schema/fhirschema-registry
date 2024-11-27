(ns svs.http
  (:require
   [system]
   [cheshire.core]
   [clojure.string :as str]
   [hiccup.page]
   [org.httpkit.server :as server]
   [ring.middleware.head]
   [ring.middleware.params]
   [ring.middleware.cookies]
   [ring.util.codec :as codec]
   [ring.util.io]
   [ring.util.response]
   [cognitect.transit :as transit]
   [cheshire.core]
   [svs.logger :as log]
   [cheshire.generate :as json-gen]
   [svs.http.routing]
   [svs.http.formats]
   [org.httpkit.client :as http])
  (:import [java.io BufferedWriter OutputStreamWriter ByteArrayInputStream ByteArrayOutputStream]
           [java.nio.charset StandardCharsets]
           [java.util.zip GZIPOutputStream]))



(set! *warn-on-reflection* true)

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

(defn render-index [ctx req]
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

(defn response-body [ctx body]
  (cheshire.core/generate-string body))

(defn register-middleware [ctx mw-fn]
  (system/update-system-state ctx [:middlewares] (fn [mws] (conj (or mws []) mw-fn))))

(defn clear-middlewares [ctx]
  (system/update-system-state ctx [:middlewares] (fn [mws] [])))

(defn apply-middlewares [ctx req]
  (->> (system/get-system-state ctx [:middlewares])
       (reduce (fn [ctx mw] (mw ctx req)) ctx)))

;; example work with context
(defn ctx-remote-addr [ctx]
  (system/ctx-get ctx ::remote-addr))

(defn parse-route [uri]
  (->> (str/split (str/replace uri #"(^/|/$)" "") #"/")
       (mapv (fn [i]
               (if (str/starts-with? i ":")
                 [(keyword (subs i 1))]
                 i)))))


(defn register-endpoint [ctx meth url f & [opts]]
  (let [route (parse-route url)
        path (into route [meth])]
    (system/update-system-state
     ctx [:endpoints]
     (fn [x] (assoc-in x path (merge {:fn f} opts))))))


(defn clear-endpoints [ctx]
  (system/clear-system-state ctx [:endpoints]))

(defn unregister-endpoint [ctx meth url]
  (let [route (parse-route url)]
    (system/clear-system-state ctx (into [:endpoints] (conj route meth)))))

(defn resolve-endpoint [ctx meth url]
  (let [routes (system/get-system-state ctx [:endpoints])]
    (svs.http.routing/match [meth url] routes)))

(defn on-request-hooks [ctx params]
  (doseq [on-request-hook (system/get-hooks ctx ::on-request)]
    (when-let [f (:fn on-request-hook)]
      (f ctx params))))

(defn format-response [ctx resp]
  (if-not (and (:body resp) (or (map? (:body resp)) (vector? (:body resp))))
    resp
    (-> resp
     (update :body (fn [x] (if (or (vector? x) (map? x)) (cheshire.core/generate-string x) x)))
     (assoc-in [:headers "content-type"] "application/json"))))

(defn dispatch [system {meth :request-method uri :uri :as req}]
  (let [ctx (system/new-context system {::uri uri ::method meth ::remote-addr (:remote-addr req)})
        ctx (apply-middlewares ctx req)]
    (cond
      (and (contains? #{:get :head} meth) (str/starts-with? (or uri "") "/static/"))
      (handle-static req)

      (and (= "/" uri) (= :get meth))
      (render-index ctx req)

      :else
      (let [query-params (parse-params (:query-string req) "UTF-8")]
        (if-let [{{f :fn :as op} :match params :params} (resolve-endpoint ctx meth uri)]
          (do
            (log/info ctx meth uri {:route-params params})
            (on-request-hooks ctx {:uri uri :method meth :query-params query-params})
            (->>
             (f ctx (assoc (merge req op) :query-params query-params :route-params params))
             (format-response ctx)))
          (do
            (log/info ctx meth (str uri " not found" {:http.status 404}))
            {:status 404}))))))

(defn stream [req cb]
  (server/with-channel req chan
    (server/on-close chan (fn [_status] (println "Close channel")))
    (future
      (try
        (server/send! chan {:headers {"content-type" "application/x-ndjson" "content-encoding" "gzip"}} false)
        (let [array (ByteArrayOutputStream.)
              gzip (GZIPOutputStream. array true)
              wrtr (BufferedWriter. (OutputStreamWriter. gzip StandardCharsets/UTF_8))
              wr (fn [^String res]
                   (.write wrtr res)
                   (.write wrtr "\n")
                   (.flush wrtr)
                   (.flush array)
                   (server/send! chan (.toByteArray array) false)
                   (.reset array))]
          (cb wr)
          (.finish gzip)
          (.flush array)
          (server/send! chan (.toByteArray array) false))
        (catch Exception e (println :error e))
        (finally
          (server/close chan))))))


(defn request [ctx {path :path}]
  (let [url (str "http://localhost:" (system/get-system-state ctx [:port]) path)
        resp @(http/get url)]
    (log/info ctx ::get url)
    (update resp :body (fn [x] (if (string? x) x (if (nil? x) nil (slurp x)))))))

(defn get-open-api [ctx req]
  {:status 200
   :body (system/get-system-state ctx [:endpoints])})

(defn start [system config]
  (system/start-service
   system
   (let [port (or (:port config) 7777)]
     (log/info system ::start "start http server" {:port port})
     {:server (server/run-server (fn [req] (#'dispatch system req)) {:port port}) :port port}))
  (system/manifest-hook system ::on-request {:desc "This hook is called on request and passed method, uri and params"})
  (register-endpoint system :get "/api" #'get-open-api))

(defn stop [system]
  (system/stop-service
   system
   (when-let [stop (system/get-system-state system [:server])]
     (stop))))

(system/defmanifest
  {:description "http server module"
   :deps [#'log/manifest]
   :config
   {:port
    {:type "integer"
     :default 8080
     :validator pos-int?}}})

(comment

  (defn get-test
    [ctx req]
    {:status 200
     :body (response-body ctx {:message (java.util.Date.) :route-params (:route-params req)})})

  (defn logging-mw [ctx req]
    (println :HTTP (:uri req))
    ctx)

  (def system (system/start-system {:services ["svs.http"] :svs.http {:port 7776}}))

  (request system {:path "/api"})

  (stop system)

  (register-middleware system #'logging-mw)

  (unregister-endpoint system :get "/test")
  (clear-endpoints system)

  (register-endpoint system :get "/test" #'get-test)

  (register-endpoint system :get "/Patient/:id" #'get-test)

  (resolve-endpoint system :get "/test")
  (resolve-endpoint system :get "/Patient/pt-1")

  (system/get-system-state system [:endpoints])

  (clear-middlewares system)

  (time (request system {:path "/test"}))
  (request system {:path "/Patient/pt-1"})


  (parse-route "/Patient/:id")

  (defn on-request-hook [ctx params] (println :HOOK params))

  (system/register-hook system ::on-request #'on-request-hook)

  (request system {:path "/api"})


  )
