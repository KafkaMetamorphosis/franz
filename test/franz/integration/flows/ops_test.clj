(ns franz.integration.flows.ops-test
  (:require [state-flow.api :refer [defflow flow match?]]
            [franz.helpers.system :as test-system]
            [franz.helpers.http :as http]))

(defflow health-endpoint-returns-healthy
  {:init test-system/build-test-system}
  (flow "GET /ops/health returns 200 with healthy status"
    (match? {:status 200
             :body   {:status "healthy"
                      :checks {:self "healthy"}}}
            (http/GET "/ops/health"))))

(defflow liveness-endpoint-returns-alive
  {:init test-system/build-test-system}
  (flow "GET /ops/liveness returns 200"
    (match? {:status 200
             :body   {:status "alive"}}
            (http/GET "/ops/liveness"))))

(defflow readiness-endpoint-returns-ready
  {:init test-system/build-test-system}
  (flow "GET /ops/readiness returns 200 when system is ready"
    (match? {:status 200
             :body   {:status "ready"}}
            (http/GET "/ops/readiness"))))
