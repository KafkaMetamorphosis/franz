(ns franz.core
  (:gen-class)
  (:require [com.stuartsierra.component :as component]
            [franz.banner :as banner]
            [franz.system :as system]))


(defn -main [& _args]
  (banner/print!)
  (let [started (component/start (system/new-system))]
    (.addShutdownHook
      (Runtime/getRuntime)
      (Thread. ^Runnable (fn [] (component/stop started))))
    @(promise)))
