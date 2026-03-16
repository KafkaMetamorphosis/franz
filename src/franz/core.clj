(ns franz.core
  (:gen-class)
  (:require [com.stuartsierra.component :as component]
            [franz.system :as system]))


(defn -main [& _args]
  (let [started (component/start (system/new-system))]
    (.addShutdownHook
      (Runtime/getRuntime)
      (Thread. ^Runnable (fn [] (component/stop started))))
    (println "franz is running. Press Ctrl+C to stop.")
    @(promise)))
