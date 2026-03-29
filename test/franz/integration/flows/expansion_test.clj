(ns franz.integration.flows.expansion-test
  (:require [state-flow.api :refer [defflow flow match?]]
            [state-flow.api :as flow]
            [franz.helpers.system :as test-system]
            [franz.helpers.http :as http]
            [franz.helpers.db :as test-db]))

(defn- init-system []
  (let [system (test-system/build-test-system)]
    (test-system/clean-db! system)
    system))

(def ^:private base-topic-config
  {:name               "default-config"
   :partitions         3
   :replication-factor 2
   :retention-ms       86400000})

(defn- datasource-from-system [system]
  (get-in system [:database :datasource]))

(defn- query-claims-for-definition [definition-id]
  (flow/flow "query claims for definition"
    [system (flow/get-state)]
    [:let [datasource (datasource-from-system system)
           claims (test-db/find-claims-for-definition datasource definition-id)]]
    (flow/return claims)))

(defn- query-revisions-for-claim [claim-id]
  (flow/flow "query revisions for claim"
    [system (flow/get-state)]
    [:let [datasource (datasource-from-system system)
           revisions (test-db/find-revisions-for-claim datasource claim-id)]]
    (flow/return revisions)))

(defn- count-claims-for-definition [definition-id]
  (flow/flow "count claims for definition"
    [system (flow/get-state)]
    [:let [datasource (datasource-from-system system)
           claim-count (test-db/count-claims datasource definition-id)]]
    (flow/return claim-count)))

;; 1. Create cluster + definition -> claims and PendingReconciliation revisions created
(defflow create-cluster-and-definition-creates-claims-test
  {:init init-system}
  (flow "creating a cluster then a topic definition produces claims and revisions"
    [config-response (http/POST "/api/v0/topic_configurations" base-topic-config)]
    [:let [config-id (get-in config-response [:body :id])]]
    [_cluster-response (http/POST "/api/v0/clusters"
                         {:name                           "cluster-1"
                          :bootstrap-url                  "broker:9092"
                          :default-topic-configuration-id config-id})]
    [definition-response (http/POST "/api/v0/topic_definitions"
                           {:topic-name             "my-topic"
                            :topic-configuration-id config-id})]
    [:let [definition-id (get-in definition-response [:body :id])]]
    [claims (query-claims-for-definition definition-id)]
    [:let [_ (assert (= 1 (count claims)) (str "expected 1 claim, got " (count claims)))
           the-claim (first claims)]]
    (match? "Pending" (:status the-claim))
    [revisions (query-revisions-for-claim (:id the-claim))]
    (match? 1 (count revisions))
    (match? "PendingReconciliation" (:status (first revisions)))))

;; 2. Tainted (no-creation) cluster, no toleration -> no claim
(defflow tainted-no-creation-cluster-no-toleration-test
  {:init init-system}
  (flow "tainted no-creation cluster without toleration gets no claim"
    [config-response (http/POST "/api/v0/topic_configurations" base-topic-config)]
    [:let [config-id (get-in config-response [:body :id])]]
    [_cluster-response (http/POST "/api/v0/clusters"
                         {:name                           "tainted-cluster"
                          :bootstrap-url                  "broker:9092"
                          :default-topic-configuration-id config-id
                          :labels                         {:franz.taint "unstable:no-creation"}})]
    [definition-response (http/POST "/api/v0/topic_definitions"
                           {:topic-name             "no-tol-topic"
                            :topic-configuration-id config-id})]
    [:let [definition-id (get-in definition-response [:body :id])]]
    [claim-count (count-claims-for-definition definition-id)]
    (match? 0 claim-count)))

;; 3. Tainted (no-creation) cluster, toleration present -> claim created
(defflow tainted-no-creation-cluster-with-toleration-test
  {:init init-system}
  (flow "tainted no-creation cluster with matching toleration gets a claim"
    [config-response (http/POST "/api/v0/topic_configurations" base-topic-config)]
    [:let [config-id (get-in config-response [:body :id])]]
    [_cluster-response (http/POST "/api/v0/clusters"
                         {:name                           "tainted-cluster-tol"
                          :bootstrap-url                  "broker:9092"
                          :default-topic-configuration-id config-id
                          :labels                         {:franz.taint "unstable:no-creation"}})]
    [definition-response (http/POST "/api/v0/topic_definitions"
                           {:topic-name             "tol-topic"
                            :topic-configuration-id config-id
                            :labels                 {:franz.taint/toleration "unstable:no-creation"}})]
    [:let [definition-id (get-in definition-response [:body :id])]]
    [claim-count (count-claims-for-definition definition-id)]
    (match? 1 claim-count)))

;; 4. Definition with selector, only 1 of 2 clusters matches -> claim on matching cluster only
(defflow selector-filters-clusters-test
  {:init init-system}
  (flow "affinity selector filters to matching clusters only"
    [config-response (http/POST "/api/v0/topic_configurations" base-topic-config)]
    [:let [config-id (get-in config-response [:body :id])]]
    (http/POST "/api/v0/clusters"
      {:name                           "prod-cluster"
       :bootstrap-url                  "broker:9092"
       :default-topic-configuration-id config-id
       :labels                         {:env "production"}})
    (http/POST "/api/v0/clusters"
      {:name                           "staging-cluster"
       :bootstrap-url                  "broker:9092"
       :default-topic-configuration-id config-id
       :labels                         {:env "staging"}})
    [definition-response (http/POST "/api/v0/topic_definitions"
                           {:topic-name             "prod-only-topic"
                            :topic-configuration-id config-id
                            :labels                 {:franz.affinity/selector "env=production"}})]
    [:let [definition-id (get-in definition-response [:body :id])]]
    [claim-count (count-claims-for-definition definition-id)]
    (match? 1 claim-count)))

;; 5. Definition with selector + shard-size=1, 3 matching clusters -> only 1 claim (highest weight)
(defflow shard-size-limits-claims-test
  {:init init-system}
  (flow "shard-size limits number of claims to highest weight clusters"
    [config-response (http/POST "/api/v0/topic_configurations" base-topic-config)]
    [:let [config-id (get-in config-response [:body :id])]]
    (http/POST "/api/v0/clusters"
      {:name                           "weight-10"
       :bootstrap-url                  "broker:9092"
       :default-topic-configuration-id config-id
       :labels                         {:env "production" :franz.affinity/weight "10"}})
    (http/POST "/api/v0/clusters"
      {:name                           "weight-5"
       :bootstrap-url                  "broker:9092"
       :default-topic-configuration-id config-id
       :labels                         {:env "production" :franz.affinity/weight "5"}})
    (http/POST "/api/v0/clusters"
      {:name                           "weight-1"
       :bootstrap-url                  "broker:9092"
       :default-topic-configuration-id config-id
       :labels                         {:env "production" :franz.affinity/weight "1"}})
    [definition-response (http/POST "/api/v0/topic_definitions"
                           {:topic-name             "shard-topic"
                            :topic-configuration-id config-id
                            :labels                 {:franz.affinity/selector   "env=production"
                                                     :franz.affinity/shard-size "1"}})]
    [:let [definition-id (get-in definition-response [:body :id])]]
    [claim-count (count-claims-for-definition definition-id)]
    (match? 1 claim-count)))

;; 6. Definition with no selector -> all non-tainted clusters get claims (shard-size ignored)
(defflow no-selector-ignores-shard-size-test
  {:init init-system}
  (flow "no selector means all non-tainted clusters get claims, shard-size ignored"
    [config-response (http/POST "/api/v0/topic_configurations" base-topic-config)]
    [:let [config-id (get-in config-response [:body :id])]]
    (http/POST "/api/v0/clusters"
      {:name                           "cluster-a"
       :bootstrap-url                  "broker:9092"
       :default-topic-configuration-id config-id})
    (http/POST "/api/v0/clusters"
      {:name                           "cluster-b"
       :bootstrap-url                  "broker:9092"
       :default-topic-configuration-id config-id})
    [definition-response (http/POST "/api/v0/topic_definitions"
                           {:topic-name             "all-clusters-topic"
                            :topic-configuration-id config-id
                            :labels                 {:franz.affinity/shard-size "1"}})]
    [:let [definition-id (get-in definition-response [:body :id])]]
    [claim-count (count-claims-for-definition definition-id)]
    (match? 2 claim-count)))

;; 7. Delete definition -> PendingDelete revisions created for existing claims
(defflow delete-definition-creates-pending-delete-revisions-test
  {:init init-system}
  (flow "deleting a topic definition creates PendingDelete revisions"
    [config-response (http/POST "/api/v0/topic_configurations" base-topic-config)]
    [:let [config-id (get-in config-response [:body :id])]]
    (http/POST "/api/v0/clusters"
      {:name                           "del-cluster"
       :bootstrap-url                  "broker:9092"
       :default-topic-configuration-id config-id})
    [definition-response (http/POST "/api/v0/topic_definitions"
                           {:topic-name             "delete-me-topic"
                            :topic-configuration-id config-id})]
    [:let [definition-id (get-in definition-response [:body :id])]]
    ;; Verify claim exists first
    [claims-before (query-claims-for-definition definition-id)]
    [:let [_ (assert (= 1 (count claims-before)))]]
    ;; Delete the definition
    (http/DELETE "/api/v0/topic_definitions/delete-me-topic")
    ;; Check that PendingDelete revisions were created
    [claims-after (query-claims-for-definition definition-id)]
    [:let [deleted-claim (first claims-after)]]
    (match? "Deleted" (:status deleted-claim))
    [revisions (query-revisions-for-claim (:id deleted-claim))]
    [:let [latest-revision (last (sort-by :created-at revisions))]]
    (match? "PendingDelete" (:status latest-revision))))

;; 8. Manual trigger endpoint -> expands PendingExpansion definitions
(defflow manual-trigger-expands-pending-definitions-test
  {:init init-system}
  (flow "POST /api/v0/topic_definitions_expansion expands pending definitions"
    [config-response (http/POST "/api/v0/topic_configurations" base-topic-config)]
    [:let [config-id (get-in config-response [:body :id])]]
    ;; Create a definition first (no clusters yet, so no claims created but expansion runs with 0 clusters)
    [definition-response (http/POST "/api/v0/topic_definitions"
                           {:topic-name             "trigger-topic"
                            :topic-configuration-id config-id})]
    [:let [definition-id (get-in definition-response [:body :id])]]
    ;; Now add a cluster (marks definitions PendingExpansion)
    (http/POST "/api/v0/clusters"
      {:name                           "trigger-cluster"
       :bootstrap-url                  "broker:9092"
       :default-topic-configuration-id config-id})
    ;; Trigger expansion
    [trigger-response (http/POST "/api/v0/topic_definitions_expansion" {})]
    (match? {:status 200
             :body   {:expanded-count 1}}
            trigger-response)
    ;; Verify claim was created
    [claim-count (count-claims-for-definition definition-id)]
    (match? 1 claim-count)))

;; 9. Cluster create -> definitions marked PendingExpansion, manual trigger then creates claims
(defflow cluster-create-marks-pending-expansion-test
  {:init init-system}
  (flow "creating a cluster marks existing definitions as PendingExpansion"
    [config-response (http/POST "/api/v0/topic_configurations" base-topic-config)]
    [:let [config-id (get-in config-response [:body :id])]]
    ;; Create definition with no clusters (expansion runs but finds nothing)
    [definition-response (http/POST "/api/v0/topic_definitions"
                           {:topic-name             "pending-topic"
                            :topic-configuration-id config-id})]
    [:let [definition-id (get-in definition-response [:body :id])]]
    ;; Verify definition was expanded (no clusters = Expanded status)
    [get-response (http/GET "/api/v0/topic_definitions/pending-topic")]
    (match? {:body {:expansion-status "Expanded"}} get-response)
    ;; Now create a cluster -> marks definitions PendingExpansion
    (http/POST "/api/v0/clusters"
      {:name                           "new-cluster"
       :bootstrap-url                  "broker:9092"
       :default-topic-configuration-id config-id})
    ;; Check definition is now PendingExpansion
    [get-response-after (http/GET "/api/v0/topic_definitions/pending-topic")]
    (match? {:body {:expansion-status "PendingExpansion"}} get-response-after)
    ;; Trigger expansion to create claims
    (http/POST "/api/v0/topic_definitions_expansion" {})
    [claim-count (count-claims-for-definition definition-id)]
    (match? 1 claim-count)))
