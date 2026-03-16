(ns franz.components.http-server
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [compojure.core :refer [routes]]
            [franz.controllers.middleware :as middleware]
            [ring.adapter.jetty :as jetty]))

(defrecord HttpServer [config ops api server]
  component/Lifecycle
  (start [this]
    (if server
      this
      (let [port       (get-in config [:config :http :port])
            ops-handler (:handler-fn ops)
            api-handler (:handler-fn api)
            app        (-> (routes ops-handler api-handler)
                           middleware/wrap-not-found)
            srv        (jetty/run-jetty app {:port port :join? false})]
        (log/info "HTTP server started" {:port port})
        (assoc this :server srv))))
  (stop [this]
    (when server
      (.stop server)
      (log/info "HTTP server stopped"))
    (assoc this :server nil)))

(defn new-http-server []
  (map->HttpServer {}))
