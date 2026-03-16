(ns franz.system
  (:require [com.stuartsierra.component :as component]
            [franz.clients.postgresql :as postgresql]
            [franz.components.api :as api]
            [franz.components.config :as config]
            [franz.components.http-server :as http-server]
            [franz.components.ops :as c-ops]))

(defn new-system
  ([]
   (new-system :default))
  ([profile]
   (component/system-map
     :config      (config/new-config-component profile)
     :ops         (component/using (c-ops/new-ops-component) [:database])
     :database    (component/using (postgresql/new-database) [:config])
     :api         (component/using (api/new-api) [:database])
     :http-server (component/using (http-server/new-http-server) [:config :ops :api]))))
