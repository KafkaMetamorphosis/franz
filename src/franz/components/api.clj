(ns franz.components.api
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
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
                        wrap-keyword-params
                        wrap-params
                        middleware/wrap-exception-handler
                        middleware/wrap-request-logging)]
      (log/info "API handler ready")
      (assoc this :handler-fn handler)))
  (stop [this]
    (log/info "API handler stopped")
    (assoc this :handler-fn nil)))

(defn new-api []
  (map->Api {}))
