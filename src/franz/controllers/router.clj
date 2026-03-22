(ns franz.controllers.router
  (:require
   [compojure.core :refer [GET POST PUT DELETE]]
   [compojure.core :as compojure]
   [franz.ops.handlers :as ops-handlers]
   [franz.controllers.clusters :as clusters]
   [franz.controllers.topic-configurations :as topic-configurations]))

(defmacro compojure-route [method path handler]
  (let [request-sym (gensym "request")]
    `(case ~method
       "GET"    (GET    ~path ~request-sym (~handler ~request-sym))
       "POST"   (POST   ~path ~request-sym (~handler ~request-sym))
       "PUT"    (PUT    ~path ~request-sym (~handler ~request-sym))
       "DELETE" (DELETE ~path ~request-sym (~handler ~request-sym)))))

(def route-manifest
  [["GET"    "/ops/health"      ops-handlers/health-handler]
   ["GET"    "/ops/liveness"    ops-handlers/liveness-handler]
   ["GET"    "/ops/readiness"   ops-handlers/readiness-handler]
   ["GET"    "/ops/config/dump" ops-handlers/config-dump-handler]
   ["GET"    "/api/v0/clusters"                          clusters/list-clusters]
   ["POST"   "/api/v0/clusters"                          clusters/create-cluster]
   ["GET"    "/api/v0/clusters/:cluster-name"            clusters/get-cluster]
   ["PUT"    "/api/v0/clusters/:cluster-name"            clusters/update-cluster]
   ["DELETE" "/api/v0/clusters/:cluster-name"            clusters/delete-cluster]
   ["GET"    "/api/v0/topic_configurations"                         topic-configurations/list-topic-configurations]
   ["POST"   "/api/v0/topic_configurations"                         topic-configurations/create-topic-configuration]
   ["GET"    "/api/v0/topic_configurations/:topic-configuration-id" topic-configurations/get-topic-configuration]
   ["PUT"    "/api/v0/topic_configurations/:topic-configuration-id" topic-configurations/update-topic-configuration]
   ["DELETE" "/api/v0/topic_configurations/:topic-configuration-id" topic-configurations/delete-topic-configuration]])

(defn routes [components]
  (apply compojure/routes
    (map (fn [[method path handler]]
           (compojure-route method path (partial handler components)))
         route-manifest)))
