(ns franz.components.http-server
  (:require [com.stuartsierra.component :as component]
            [ring.adapter.jetty :as jetty]))

(defrecord HttpServer [port handler server]
  component/Lifecycle
  (start [this]
    (if server
      this
      (let [handler-fn (:handler-fn handler)
            srv        (jetty/run-jetty handler-fn {:port port :join? false})]
        (println (str "HTTP server started on port " port))
        (assoc this :server srv))))
  (stop [this]
    (when server
      (.stop server)
      (println "HTTP server stopped"))
    (assoc this :server nil)))

(defn new-http-server [port]
  (map->HttpServer {:port port}))
