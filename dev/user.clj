(ns user
  (:require [com.stuartsierra.component :as component]
            [franz.config.core :as config]
            [franz.system :as system]))

(def sys nil)

(defn init []
  (alter-var-root #'sys (constantly (system/new-system (config/load-config)))))

(defn start []
  (alter-var-root #'sys component/start))

(defn stop []
  (alter-var-root #'sys (fn [s] (when s (component/stop s)))))

(defn reset []
  (stop)
  (init)
  (start))
