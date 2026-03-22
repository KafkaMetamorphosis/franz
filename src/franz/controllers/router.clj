(ns franz.controllers.router
  (:require [compojure.core :refer [GET POST PUT DELETE context]]
            [compojure.core :as compojure]
            [franz.controllers.clusters :as clusters]
            [franz.controllers.topic-configurations :as topic-configurations]))

(defn routes [db]
  (context "/api/v0" []
    (compojure/routes
      (GET    "/clusters"               request (clusters/list-clusters db request))
      (POST   "/clusters"               request (clusters/create-cluster db request))
      (GET    "/clusters/:cluster-name" request (clusters/get-cluster db request))
      (PUT    "/clusters/:cluster-name" request (clusters/update-cluster db request))
      (DELETE "/clusters/:cluster-name" request (clusters/delete-cluster db request))

      (GET    "/topic_configuration"                         request (topic-configurations/list-topic-configurations db request))
      (POST   "/topic_configuration"                         request (topic-configurations/create-topic-configuration db request))
      (GET    "/topic_configuration/:topic-configuration-id" request (topic-configurations/get-topic-configuration db request))
      (PUT    "/topic_configuration/:topic-configuration-id" request (topic-configurations/update-topic-configuration db request))
      (DELETE "/topic_configuration/:topic-configuration-id" request (topic-configurations/delete-topic-configuration db request)))))
