(ns franz.helpers.system
  (:require [com.stuartsierra.component :as component]
            [franz.config.core :as config]
            [franz.system :as system]))

(defn build-test-system []
  (let [config (config/load-config)]
    (component/start
      (system/new-system (assoc config :http {:port 0})))))
