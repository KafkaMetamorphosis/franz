(ns franz.clients.postgresql
  "PostgreSQL database client component.
   Clients connect to external dependencies (ADR-001)."
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]))

(defrecord Database [config datasource]
  component/Lifecycle
  (start [this]
    (log/info "Starting Database component")
    (let [ds (jdbc/get-datasource config)]
      (log/info "Database component started")
      (assoc this :datasource ds)))
  (stop [this]
    (log/info "Stopping Database component")
    (when (instance? java.io.Closeable datasource)
      (.close ^java.io.Closeable datasource)
      (log/info "Datasource closed"))
    (assoc this :datasource nil)))

(defn new-database [config]
  (map->Database {:config config}))
