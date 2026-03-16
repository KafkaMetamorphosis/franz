(ns franz.controllers.middleware
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]))

(defn wrap-json-body [handler]
  (fn [request]
    (let [body (when-let [body-stream (:body request)]
                 (when (and body-stream
                            (some-> (get-in request [:headers "content-type"])
                                    (.contains "application/json")))
                   (json/parse-string (slurp body-stream) true)))]
      (handler (cond-> request
                 body (assoc :body body))))))

(defn wrap-json-response [handler]
  (fn [request]
    (let [response (handler request)]
      (when response
        (-> response
            (update :body json/generate-string)
            (assoc-in [:headers "Content-Type"] "application/json"))))))

(defn wrap-exception-handler [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception e
        (log/error e "Unhandled exception")
        {:status  500
         :headers {"Content-Type" "application/json"}
         :body    (json/generate-string {:error "internal server error"})}))))

(defn wrap-not-found [handler]
  (fn [request]
    (or (handler request)
        {:status 404
         :body   {:error "not found"}})))
