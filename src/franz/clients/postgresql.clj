(ns franz.clients.postgresql
  "PostgreSQL database client component.
   Clients connect to external dependencies (ADR-001)."
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]))


(defrecord Database [config datasource]
  component/Lifecycle
  (start [this]
    (let [db-cfg (get-in config [:config :database])
          ds     (jdbc/get-datasource db-cfg)]
      (log/info "Database started")
      (assoc this :datasource ds)))
  (stop [this]
    (when (instance? java.io.Closeable datasource)
      (.close ^java.io.Closeable datasource))
    (log/info "Database stopped")
    (assoc this :datasource nil)))


(defn new-database []
  (map->Database {}))
