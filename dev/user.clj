(ns user
  (:require [com.stuartsierra.component :as component]
            [franz.dev.seed :as seed]
            [franz.system :as system]))


(def sys nil)


(defn init []
  (alter-var-root #'sys (constantly (system/new-system))))


(defn start []
  (alter-var-root #'sys component/start))


(defn stop []
  (alter-var-root #'sys (fn [s] (when s (component/stop s)))))


(defn reset []
  (stop)
  (init)
  (start))


(defn seed []
  (seed/seed! (get-in sys [:database :datasource])))
