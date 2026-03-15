(ns franz.ops.routes
  (:require [compojure.core :refer [GET context]]
            [franz.ops.handlers :as handlers]))

(defn ops-routes
  "Returns compojure routes for ops endpoints.
   health-checker is the HealthChecker component."
  [health-checker]
  (context "/ops" []
    (GET "/health" request
      (handlers/health-handler health-checker request))
    (GET "/liveness" request
      (handlers/liveness-handler request))
    (GET "/readiness" request
      (handlers/readiness-handler health-checker request))))
