(ns franz.config.core
  (:require [aero.core :as aero]
            [clojure.java.io :as io]))

(defn load-config
  ([]
   (load-config :default))
  ([profile]
   (aero/read-config (io/resource "config.edn") {:profile profile})))
