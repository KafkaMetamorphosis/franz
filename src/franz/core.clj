(ns franz.core
  (:gen-class)
  (:require [com.stuartsierra.component :as component]
            [franz.config.core :as config]
            [franz.system :as system]))

(defn -main [& _args]
  (let [cfg     (config/load-config)
        sys     (system/new-system cfg)
        started (component/start sys)]
    (.addShutdownHook
      (Runtime/getRuntime)
      (Thread. ^Runnable (fn [] (component/stop started))))
    (println "franz is running. Press Ctrl+C to stop.")
    @(promise)))
