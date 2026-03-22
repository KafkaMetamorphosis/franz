(ns franz.components.api
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [franz.controllers.router :as router]
            [franz.controllers.middleware :as middleware]))

(defrecord Api [database handler-fn]
  component/Lifecycle
  (start [this]
    (let [db      (:datasource database)
          handler (-> (router/routes db)
                      middleware/wrap-json-body
                      middleware/wrap-json-response
                      middleware/wrap-exception-handler)]
      (log/info "API handler ready")
      (assoc this :handler-fn handler)))
  (stop [this]
    (log/info "API handler stopped")
    (assoc this :handler-fn nil)))

(defn new-api []
  (map->Api {}))
