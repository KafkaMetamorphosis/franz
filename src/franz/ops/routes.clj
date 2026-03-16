(ns franz.ops.routes
  (:require [compojure.core :refer [GET context]]
            [franz.ops.handlers :as handlers]))

(defn ops-routes [ds]
  (context "/ops" []
    (GET "/health"    request (handlers/health-handler ds request))
    (GET "/liveness"  request (handlers/liveness-handler request))
    (GET "/readiness" request (handlers/readiness-handler request))))
