(ns franz.controllers.middleware
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log])
  (:import [org.slf4j MDC]
           [java.util UUID]))

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

(defn wrap-request-logging [handler]
  (fn [request]
    (let [method     (-> request :request-method name .toUpperCase)
          path       (:uri request)
          request-id (or (get-in request [:headers "x-request-id"])
                         (str (UUID/randomUUID)))
          start-time (System/currentTimeMillis)]
      (try
        (MDC/put "request-id" request-id)
        (MDC/put "method" method)
        (MDC/put "path" path)
        (log/info (str "HTTP " method " " path))
        (let [response (handler request)
              duration (- (System/currentTimeMillis) start-time)]
          (log/info (str "HTTP " method " " path " -> " (or (:status response) "nil") " (" duration "ms)"))
          response)
        (finally
          (MDC/clear))))))
