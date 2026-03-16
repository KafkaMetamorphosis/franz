(ns franz.integration.flows.clusters-test
  (:require [state-flow.api :refer [defflow flow match?]]
            [franz.helpers.system :as test-system]
            [franz.helpers.http :as http]))

(defn- init-system []
  (let [system (test-system/build-test-system)]
    (test-system/clean-db! system)
    system))

(defflow create-cluster-test
  {:init init-system}
  (flow "POST /api/v0/clusters creates a new cluster"
    (match? {:status 201
             :body   {:name          "test-cluster"
                      :bootstrap-url "broker:9092"
                      :labels        {:env "test"}}}
            (http/POST "/api/v0/clusters"
              {:name          "test-cluster"
               :bootstrap-url "broker:9092"
               :labels        {:env "test"}}))))

(defflow create-cluster-conflict-test
  {:init init-system}
  (flow "POST /api/v0/clusters returns 409 on duplicate name"
    (http/POST "/api/v0/clusters"
      {:name "dup-cluster" :bootstrap-url "broker:9092"})
    (match? {:status 409}
            (http/POST "/api/v0/clusters"
              {:name "dup-cluster" :bootstrap-url "broker:9092"}))))

(defflow create-cluster-validation-test
  {:init init-system}
  (flow "POST /api/v0/clusters returns 422 on invalid input"
    (match? {:status 422}
            (http/POST "/api/v0/clusters"
              {:name "" :bootstrap-url "broker:9092"}))))

(defflow get-cluster-test
  {:init init-system}
  (flow "GET /api/v0/clusters/:name returns a cluster"
    (http/POST "/api/v0/clusters"
      {:name "get-test" :bootstrap-url "broker:9092"})
    (match? {:status 200
             :body   {:name "get-test"}}
            (http/GET "/api/v0/clusters/get-test"))))

(defflow get-cluster-not-found-test
  {:init init-system}
  (flow "GET /api/v0/clusters/:name returns 404"
    (match? {:status 404}
            (http/GET "/api/v0/clusters/nonexistent"))))

(defflow list-clusters-test
  {:init init-system}
  (flow "GET /api/v0/clusters lists clusters with pagination"
    (http/POST "/api/v0/clusters"
      {:name "list-1" :bootstrap-url "broker:9092"})
    (http/POST "/api/v0/clusters"
      {:name "list-2" :bootstrap-url "broker:9092"})
    (match? {:status 200
             :body   {:page  1
                      :size  20
                      :total 2}}
            (http/GET "/api/v0/clusters?page=1&size=20"))))

(defflow update-cluster-test
  {:init init-system}
  (flow "PUT /api/v0/clusters/:name updates a cluster"
    (http/POST "/api/v0/clusters"
      {:name "update-test" :bootstrap-url "broker:9092"})
    (match? {:status 200
             :body   {:name          "update-test"
                      :bootstrap-url "new-broker:9092"}}
            (http/PUT "/api/v0/clusters/update-test"
              {:bootstrap-url "new-broker:9092"}))))

(defflow delete-cluster-test
  {:init init-system}
  (flow "DELETE /api/v0/clusters/:name deletes a cluster"
    (http/POST "/api/v0/clusters"
      {:name "delete-test" :bootstrap-url "broker:9092"})
    (match? {:status 204}
            (http/DELETE "/api/v0/clusters/delete-test"))))

