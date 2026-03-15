(ns franz.helpers.http
  (:require [clj-http.client :as http]
            [state-flow.api :as flow]))

(defn- server-port [system]
  (-> system :http-server :server .getConnectors first .getLocalPort))

(defn request [method path]
  (flow/flow (str (name method) " " path)
    [system (flow/get-state)]
    [:let [port (server-port system)
           url  (str "http://localhost:" port path)
           resp (http/request {:method           method
                               :url              url
                               :as               :json
                               :throw-exceptions false
                               :coerce           :always})]]
    (flow/return resp)))

(defn GET [path]
  (request :get path))
