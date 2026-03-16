(ns franz.ops.handlers
  (:require [franz.ops.health :as health]
            [ring.util.response :as response]))

(defn health-handler [ds _request]
  (let [result (health/run-checks ds)
        status (if (= :healthy (:status result)) 200 503)]
    (-> (response/response result)
        (response/status status))))

(defn liveness-handler [_request]
  (response/response {:status "alive"}))

(defn readiness-handler [_request]
  (if (health/ready?)
    (response/response {:status "ready"})
    (-> (response/response {:status "not-ready"})
        (response/status 503))))
