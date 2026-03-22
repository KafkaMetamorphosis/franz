(ns franz.components.api
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [franz.controllers.router :as router]
            [franz.controllers.middleware :as middleware]))

(defrecord Api [database config handler-fn]
  component/Lifecycle
  (start [this]
    (let [components {:db     (:datasource database)
                      :config (:config config)}
          handler    (-> (router/routes components)
                        middleware/wrap-not-found
                        middleware/wrap-json-body
                        middleware/wrap-json-response
                        middleware/wrap-exception-handler
                        middleware/wrap-request-logging)]
      (log/info "API handler ready")
      (assoc this :handler-fn handler)))
  (stop [this]
    (log/info "API handler stopped")
    (assoc this :handler-fn nil)))

(defn new-api []
  (map->Api {}))
