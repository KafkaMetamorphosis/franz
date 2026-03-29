(ns franz.integration.flows.topic-definitions-test
  (:require [state-flow.api :refer [defflow flow match?]]
            [franz.helpers.system :as test-system]
            [franz.helpers.http :as http]))

(defn- init-system []
  (let [system (test-system/build-test-system)]
    (test-system/clean-db! system)
    system))

(def ^:private valid-topic-config
  {:name               "default-config"
   :partitions         3
   :replication-factor 2
   :retention-ms       86400000})

(defn- create-topic-config []
  (http/POST "/api/v0/topic_configurations" valid-topic-config))

;; 1. create successfully
(defflow create-topic-definition-test
  {:init init-system}
  (flow "POST /api/v0/topic_definitions creates a new topic definition"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (match? {:status 201
             :body   {:topic-name             "my-topic"
                      :topic-configuration-id config-id
                      :status                 "Active"
                      :expansion-status       "PendingExpansion"
                      :labels                 {:env "test"}}}
            (http/POST "/api/v0/topic_definitions"
              {:topic-name             "my-topic"
               :topic-configuration-id config-id
               :labels                 {:env "test"}}))))

;; 2. create conflict
(defflow create-topic-definition-conflict-test
  {:init init-system}
  (flow "POST /api/v0/topic_definitions returns 409 on duplicate topic-name"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "dup-topic" :topic-configuration-id config-id})
    (match? {:status 409}
            (http/POST "/api/v0/topic_definitions"
              {:topic-name "dup-topic" :topic-configuration-id config-id}))))

;; 3. create blank name
(defflow create-topic-definition-blank-name-test
  {:init init-system}
  (flow "POST /api/v0/topic_definitions returns 422 on blank name"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (match? {:status 422}
            (http/POST "/api/v0/topic_definitions"
              {:topic-name "" :topic-configuration-id config-id}))))

;; 4. create with non-existent topic-configuration-id
(defflow create-topic-definition-invalid-config-test
  {:init init-system}
  (flow "POST /api/v0/topic_definitions returns 422 when topic-configuration-id does not exist"
    (match? {:status 422}
            (http/POST "/api/v0/topic_definitions"
              {:topic-name             "orphan-topic"
               :topic-configuration-id "00000000-0000-0000-0000-000000000000"}))))

;; 5. get by name
(defflow get-topic-definition-test
  {:init init-system}
  (flow "GET /api/v0/topic_definitions/:name returns a topic definition"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "get-test" :topic-configuration-id config-id})
    (match? {:status 200
             :body   {:topic-name "get-test"
                      :status     "Active"}}
            (http/GET "/api/v0/topic_definitions/get-test"))))

;; 6. get not found
(defflow get-topic-definition-not-found-test
  {:init init-system}
  (flow "GET /api/v0/topic_definitions/:name returns 404"
    (match? {:status 404}
            (http/GET "/api/v0/topic_definitions/nonexistent"))))

;; 7. list two
(defflow list-topic-definitions-test
  {:init init-system}
  (flow "GET /api/v0/topic_definitions lists topic definitions with pagination"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "list-1" :topic-configuration-id config-id})
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "list-2" :topic-configuration-id config-id})
    (match? {:status 200
             :body   {:page  1
                      :size  20
                      :total 2}}
            (http/GET "/api/v0/topic_definitions?page=1&size=20"))))

;; 8. list excludes deleted by default
(defflow list-topic-definitions-excludes-deleted-test
  {:init init-system}
  (flow "GET /api/v0/topic_definitions excludes deleted by default"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "to-delete" :topic-configuration-id config-id})
    (http/DELETE "/api/v0/topic_definitions/to-delete")
    (match? {:status 200
             :body   {:total 0}}
            (http/GET "/api/v0/topic_definitions"))))

;; 9. list filter by status=Paused
(defflow list-topic-definitions-filter-by-status-test
  {:init init-system}
  (flow "GET /api/v0/topic_definitions?status=Paused filters by status"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "active-topic" :topic-configuration-id config-id})
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "paused-topic" :topic-configuration-id config-id})
    (http/PUT "/api/v0/topic_definitions/paused-topic"
      {:status "Paused"})
    (match? {:status 200
             :body   {:total 1}}
            (http/GET "/api/v0/topic_definitions?status=Paused"))))

;; 10. list with invalid status value
(defflow list-topic-definitions-invalid-status-test
  {:init init-system}
  (flow "GET /api/v0/topic_definitions?status=Invalid returns 422"
    (match? {:status 422}
            (http/GET "/api/v0/topic_definitions?status=Invalid"))))

;; 11. update labels
(defflow update-topic-definition-labels-test
  {:init init-system}
  (flow "PUT /api/v0/topic_definitions/:name updates labels"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "update-labels" :topic-configuration-id config-id})
    (match? {:status 200
             :body   {:labels {:env "prod"}}}
            (http/PUT "/api/v0/topic_definitions/update-labels"
              {:labels {:env "prod"}}))))

;; 12. update topic-configuration-id
(defflow update-topic-definition-config-test
  {:init init-system}
  (flow "PUT /api/v0/topic_definitions/:name updates topic-configuration-id"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    [config-resp-2 (http/POST "/api/v0/topic_configurations"
                     (assoc valid-topic-config :name "other-config"))]
    [:let [config-id-2 (get-in config-resp-2 [:body :id])]]
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "update-config" :topic-configuration-id config-id})
    (match? {:status 200
             :body   {:topic-configuration-id config-id-2}}
            (http/PUT "/api/v0/topic_definitions/update-config"
              {:topic-configuration-id config-id-2}))))

;; 13. pause (Active -> Paused)
(defflow pause-topic-definition-test
  {:init init-system}
  (flow "PUT /api/v0/topic_definitions/:name transitions Active to Paused"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "pause-me" :topic-configuration-id config-id})
    (match? {:status 200
             :body   {:status "Paused"}}
            (http/PUT "/api/v0/topic_definitions/pause-me"
              {:status "Paused"}))))

;; 14. resume (Paused -> Active)
(defflow resume-topic-definition-test
  {:init init-system}
  (flow "PUT /api/v0/topic_definitions/:name transitions Paused to Active"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "resume-me" :topic-configuration-id config-id})
    (http/PUT "/api/v0/topic_definitions/resume-me"
      {:status "Paused"})
    (match? {:status 200
             :body   {:status "Active"}}
            (http/PUT "/api/v0/topic_definitions/resume-me"
              {:status "Active"}))))

;; 15. invalid transition (Active -> Error)
(defflow invalid-transition-test
  {:init init-system}
  (flow "PUT /api/v0/topic_definitions/:name returns 422 on Active->Error"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "no-error" :topic-configuration-id config-id})
    (match? {:status 422}
            (http/PUT "/api/v0/topic_definitions/no-error"
              {:status "Error"}))))

;; 16. PUT status=Deleted -> 422
(defflow put-deleted-status-test
  {:init init-system}
  (flow "PUT /api/v0/topic_definitions/:name returns 422 for status=Deleted"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "no-delete-via-put" :topic-configuration-id config-id})
    (match? {:status 422}
            (http/PUT "/api/v0/topic_definitions/no-delete-via-put"
              {:status "Deleted"}))))

;; 17. update not found
(defflow update-topic-definition-not-found-test
  {:init init-system}
  (flow "PUT /api/v0/topic_definitions/:name returns 404"
    (match? {:status 404}
            (http/PUT "/api/v0/topic_definitions/nonexistent"
              {:labels {:env "prod"}}))))

;; 18. delete -> subsequent GET -> 404
(defflow delete-topic-definition-test
  {:init init-system}
  (flow "DELETE /api/v0/topic_definitions/:name soft deletes"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "delete-me" :topic-configuration-id config-id})
    (match? {:status 204}
            (http/DELETE "/api/v0/topic_definitions/delete-me"))
    (match? {:status 404}
            (http/GET "/api/v0/topic_definitions/delete-me"))))

;; 19. delete not found
(defflow delete-topic-definition-not-found-test
  {:init init-system}
  (flow "DELETE /api/v0/topic_definitions/:name returns 404"
    (match? {:status 404}
            (http/DELETE "/api/v0/topic_definitions/nonexistent"))))

;; 20. create after delete with same name -> 409
(defflow create-after-delete-conflict-test
  {:init init-system}
  (flow "POST /api/v0/topic_definitions returns 409 for deleted topic-name"
    [config-resp (create-topic-config)]
    [:let [config-id (get-in config-resp [:body :id])]]
    (http/POST "/api/v0/topic_definitions"
      {:topic-name "reuse-name" :topic-configuration-id config-id})
    (http/DELETE "/api/v0/topic_definitions/reuse-name")
    (match? {:status 409}
            (http/POST "/api/v0/topic_definitions"
              {:topic-name "reuse-name" :topic-configuration-id config-id}))))
