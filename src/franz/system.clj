(ns franz.system
  (:require [com.stuartsierra.component :as component]
            [franz.clients.kafka :as kafka]
            [franz.clients.postgresql :as postgresql]
            [franz.components.http-server :as http-server]
            [franz.ops.routes :as ops-routes]
            [ring.middleware.json :refer [wrap-json-response]]))

(defrecord HealthChecker []
  component/Lifecycle
  (start [this] this)
  (stop [this] this))

(defn new-health-checker []
  (map->HealthChecker {}))

(defrecord Handler [health handler-fn]
  component/Lifecycle
  (start [this]
    (let [app (-> (ops-routes/ops-routes health)
                  wrap-json-response)]
      (assoc this :handler-fn app)))
  (stop [this]
    (assoc this :handler-fn nil)))

(defn new-handler []
  (map->Handler {}))

(defn new-system [config]
  (component/system-map
    :health (new-health-checker)
    :handler (component/using (new-handler) [:health])
    :http-server (component/using (http-server/new-http-server (get-in config [:http :port])) [:handler])

    ;; ADR-001 stubs -- no-op until implemented
    :database (postgresql/new-database (get config :database))

    :kafka-producer (kafka/new-kafka-producer (get config :kafka))

    :kafka-consumer (kafka/new-kafka-consumer (get config :kafka))))
