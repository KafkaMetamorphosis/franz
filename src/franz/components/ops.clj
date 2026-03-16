(ns franz.components.ops
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [franz.ops.routes :as ops-routes]
            [ring.middleware.json :refer [wrap-json-response]]))

(defrecord OpsComponent [database handler-fn]
  component/Lifecycle
  (start [this]
    (let [ds  (:datasource database)
          app (-> (ops-routes/ops-routes ds)
                  wrap-json-response)]
      (log/info "Ops handler ready")
      (assoc this :handler-fn app)))
  (stop [this]
    (log/info "Ops handler stopped")
    (assoc this :handler-fn nil)))

(defn new-ops-component []
  (map->OpsComponent {}))