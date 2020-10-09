(ns osrs-tileman-server.core
  (:require [clojure.data.json :as json]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [mount.core :as mount]
            [org.httpkit.server :as server]
            [osrs-tileman-server.event-persistence :as persistence]
            [ring.middleware.defaults :refer :all]
            [ring.util.http-response :refer :all])
  (:import (java.io File))
  (:gen-class))

(defn default-content-type-middleware [next-handler]
  (fn [request]
    (let [response (next-handler request)]
      (header response "Content-Type" (get-in response [:headers "Content-Type"] "test/plain")))))

(defn basic-auth-middleware [next-handler]
  (fn [{:keys [headers] :as request}]
    (if-let [server-secret (get-in (mount/args) [:properties :secret])]
      (let [request-auth-header (get headers "authorization")
            [_ type token] (and request-auth-header (re-matches #"([^ ]+) (.+)" request-auth-header))]
        (if (and (= type "Basic") (= token server-secret))
          (next-handler request)
          (unauthorized "Error: Failed to provide the correct secret with the request.")))
      (next-handler request))))

(defn handle-post-events-request [{:keys [body] :as request}]
  (let [body-str (slurp body)
        events (try (json/read-str body-str)
                    (catch Exception e e))]
    (cond (instance? Exception events)
          (bad-request "Error: Unable to interpret the request body as valid json.")

          (not (and (vector? events) (every? map? events)))
          (bad-request "Error: Request body should be an array of maps.")

          :else
          (do (persistence/write-events events)
              (ok)))))

(defn handle-get-events-request [after]
  (try (let [{:keys [new-marker events-json]} (persistence/read-events-since-marker after)]
         (-> (str "{\"new-marker\":" new-marker ",\"events\":" events-json "}")
             ok
             (content-type "application/json")))
       (catch IllegalArgumentException e
         (if (= (.getMessage e) persistence/MARKER_OUTSIDE_VALID_RANGE_MESSAGE)
           (bad-request persistence/MARKER_OUTSIDE_VALID_RANGE_MESSAGE)
           (throw e)))))

(defroutes app-routes
  (GET "/status" []
    (ok "osrs-tileman-server service is running."))

  (basic-auth-middleware
    (context "/event-log" []
      (POST "/" request (handle-post-events-request request))
      (GET "/" [] (handle-get-events-request 0))
      (GET "/after/:after" [after] (handle-get-events-request (Long/parseLong after)))))

  (route/not-found "Error: Resource not found."))

(mount/defstate ^{:on-reload :noop} web-server
  :start (server/run-server (-> #'app-routes
                                default-content-type-middleware
                                (wrap-defaults api-defaults))
                            {:port (get-in (mount/args) [:properties :port] 3000)}) ;default to port 3000 if not specified
  :stop (web-server :timeout 100)) ;give 100ms for received requests to complete before shutting down

(defn -main [& args]
  (let [properties-map (json/read-str (slurp "properties.json") :key-fn keyword)]
    (println "Starting server...")
    (mount/start-with-args {:event-file (File. "current.events")
                            :tail-file (File. "current.tail")
                            :properties properties-map})
    (println (str "Server started on port " (get properties-map :port 3000)))))