(ns franz.controllers.router
  (:require [compojure.core :refer [GET POST PUT DELETE context]]
            [compojure.core :as compojure]
            [franz.controllers.clusters :as clusters]))

(defn routes [ds]
  (context "/api/v0" []
    (compojure/routes
      (GET    "/clusters"               request (clusters/list-clusters ds request))
      (POST   "/clusters"               request (clusters/create-cluster ds request))
      (GET    "/clusters/:cluster-name" request (clusters/get-cluster ds request))
      (PUT    "/clusters/:cluster-name" request (clusters/update-cluster ds request))
      (DELETE "/clusters/:cluster-name" request (clusters/delete-cluster ds request)))))
