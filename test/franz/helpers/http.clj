(ns franz.helpers.http
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [state-flow.api :as flow]))

(defn- server-port [system]
  (-> system :http-server :server .getConnectors first .getLocalPort))

(defn request
  ([method path]
   (request method path nil))
  ([method path body]
   (flow/flow (str (name method) " " path)
     [system (flow/get-state)]
     [:let [port (server-port system)
            url  (str "http://localhost:" port path)
            opts (cond-> {:method           method
                          :url              url
                          :as               :json
                          :throw-exceptions false
                          :coerce           :always}
                   body (assoc :body         (json/generate-string body)
                               :content-type :json))
            resp (http/request opts)]]
     (flow/return resp))))

(defn GET [path]
  (request :get path))

(defn POST [path body]
  (request :post path body))

(defn PUT [path body]
  (request :put path body))

(defn DELETE [path]
  (request :delete path))
