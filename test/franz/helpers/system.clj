(ns franz.helpers.system
  (:require [com.stuartsierra.component :as component]
            [next.jdbc :as jdbc]
            [franz.system :as system]))

(defn build-test-system []
  (component/start
    (system/new-system :test)))

(defn clean-db! [system]
  (let [ds (get-in system [:database :datasource])]
    (jdbc/execute! ds ["TRUNCATE topic_configurations, clusters, topic_definitions CASCADE"])))
