(ns franz.ops.handlers
  (:require [franz.ops.health :as health]
            [ring.util.response :as response]))

(defn health-handler [health-checker _request]
  (let [result (health/run-checks health-checker)
        status (if (= :healthy (:status result)) 200 503)]
    (-> (response/response result)
        (response/status status))))

(defn liveness-handler [_request]
  (response/response {:status "alive"}))

(defn readiness-handler [health-checker _request]
  (if (health/ready? health-checker)
    (response/response {:status "ready"})
    (-> (response/response {:status "not-ready"})
        (response/status 503))))
