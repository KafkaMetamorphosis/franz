(ns franz.integration.flows.topic-configurations-test
  (:require [state-flow.api :refer [defflow flow match?]]
            [franz.helpers.system :as test-system]
            [franz.helpers.http :as http]))

(defn- init-system []
  (let [system (test-system/build-test-system)]
    (test-system/clean-db! system)
    system))

(def ^:private valid-topic-config
  {:name               "my-topic"
   :partitions         3
   :replication-factor 2
   :retention-ms       86400000
   :configs            {:cleanup.policy "compact"}
   :labels             {:env "test"}})

(defflow create-topic-configuration-test
  {:init init-system}
  (flow "POST /api/v0/topic_configuration creates a new topic configuration"
    (match? {:status 201
             :body   {:name               "my-topic"
                      :partitions         3
                      :replication-factor 2
                      :retention-ms       86400000
                      :configs            {:cleanup.policy "compact"}
                      :labels             {:env "test"}}}
            (http/POST "/api/v0/topic_configuration" valid-topic-config))))

(defflow create-conflict-test
  {:init init-system}
  (flow "POST /api/v0/topic_configuration returns 409 on duplicate name"
    (http/POST "/api/v0/topic_configuration" valid-topic-config)
    (match? {:status 409}
            (http/POST "/api/v0/topic_configuration" valid-topic-config))))

(defflow create-validation-test
  {:init init-system}
  (flow "POST /api/v0/topic_configuration returns 422 on invalid input"
    (match? {:status 422}
            (http/POST "/api/v0/topic_configuration"
              {:name "" :partitions 0 :replication-factor 0 :retention-ms 0}))))

(defflow get-topic-configuration-test
  {:init init-system}
  (flow "GET /api/v0/topic_configuration/:id returns a topic configuration"
    [create-resp (http/POST "/api/v0/topic_configuration" valid-topic-config)]
    [:let [id (get-in create-resp [:body :id])]]
    (match? {:status 200
             :body   {:name "my-topic"}}
            (http/GET (str "/api/v0/topic_configuration/" id)))))

(defflow get-not-found-test
  {:init init-system}
  (flow "GET /api/v0/topic_configuration/:id returns 404"
    (match? {:status 404}
            (http/GET "/api/v0/topic_configuration/00000000-0000-0000-0000-000000000000"))))

(defflow list-topic-configurations-test
  {:init init-system}
  (flow "GET /api/v0/topic_configuration lists topic configurations with pagination"
    (http/POST "/api/v0/topic_configuration"
      (assoc valid-topic-config :name "topic-1"))
    (http/POST "/api/v0/topic_configuration"
      (assoc valid-topic-config :name "topic-2"))
    (match? {:status 200
             :body   {:page  1
                      :size  20
                      :total 2}}
            (http/GET "/api/v0/topic_configuration?page=1&size=20"))))

(defflow update-topic-configuration-test
  {:init init-system}
  (flow "PUT /api/v0/topic_configuration/:id updates a topic configuration"
    [create-resp (http/POST "/api/v0/topic_configuration" valid-topic-config)]
    [:let [id (get-in create-resp [:body :id])]]
    (match? {:status 200
             :body   {:partitions 6}}
            (http/PUT (str "/api/v0/topic_configuration/" id)
              {:partitions 6}))))

(defflow update-partition-decrease-test
  {:init init-system}
  (flow "PUT /api/v0/topic_configuration/:id returns 422 when decreasing partitions"
    [create-resp (http/POST "/api/v0/topic_configuration" valid-topic-config)]
    [:let [id (get-in create-resp [:body :id])]]
    (match? {:status 422}
            (http/PUT (str "/api/v0/topic_configuration/" id)
              {:partitions 1}))))

(defflow update-not-found-test
  {:init init-system}
  (flow "PUT /api/v0/topic_configuration/:id returns 404"
    (match? {:status 404}
            (http/PUT "/api/v0/topic_configuration/00000000-0000-0000-0000-000000000000"
              {:partitions 6}))))

(defflow delete-topic-configuration-test
  {:init init-system}
  (flow "DELETE /api/v0/topic_configuration/:id deletes a topic configuration"
    [create-resp (http/POST "/api/v0/topic_configuration" valid-topic-config)]
    [:let [id (get-in create-resp [:body :id])]]
    (match? {:status 204}
            (http/DELETE (str "/api/v0/topic_configuration/" id)))))

(defflow delete-not-found-test
  {:init init-system}
  (flow "DELETE /api/v0/topic_configuration/:id returns 404"
    (match? {:status 404}
            (http/DELETE "/api/v0/topic_configuration/00000000-0000-0000-0000-000000000000"))))
