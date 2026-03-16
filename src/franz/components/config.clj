(ns franz.components.config
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]))


(defrecord ConfigComponent [profile config]
  component/Lifecycle
  (start [this]
    (let [cfg (aero/read-config (io/resource "config.edn") {:profile profile})]
      (log/info "Config loaded" {:profile profile})
      (assoc this :config cfg)))
  (stop [this]
    (log/info "Config stopped")
    this))


(defn new-config-component
  ([]
   (new-config-component :default))
  ([profile]
   (map->ConfigComponent {:profile profile :config nil})))
