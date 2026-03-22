(ns franz.unit.models.adapters-test
  (:require [clojure.test :refer :all]
            [franz.models.adapters :as adapters])
  (:import [java.sql Timestamp]
           [java.time Instant]
           [java.util UUID]))

(def ^:private now (Instant/now))
(def ^:private ts (Timestamp/from now))
(def ^:private id (UUID/randomUUID))

(def ^:private topic-config-id (UUID/randomUUID))

(deftest db-row->cluster-test
  (testing "converts a DB row to a cluster domain model"
    (let [row {:clusters/id                             id
               :clusters/name                           "prod-cluster"
               :clusters/bootstrap_url                  "broker:9092"
               :clusters/default_topic_configuration_id topic-config-id
               :clusters/labels                         "{\"env\": \"prod\"}"
               :clusters/created_at                     ts
               :clusters/updated_at                     ts}
          result (adapters/db-row->cluster row)]
      (is (= id (:id result)))
      (is (= "prod-cluster" (:name result)))
      (is (= "broker:9092" (:bootstrap-url result)))
      (is (= topic-config-id (:default-topic-configuration-id result)))
      (is (= {:env "prod"} (:labels result)))
      (is (instance? Instant (:created-at result)))
      (is (instance? Instant (:updated-at result)))))

  (testing "returns nil for nil input"
    (is (nil? (adapters/db-row->cluster nil)))))

(deftest cluster->db-row-test
  (testing "converts a cluster to a DB row"
    (let [cluster {:name                           "prod"
                   :bootstrap-url                  "broker:9092"
                   :default-topic-configuration-id topic-config-id
                   :labels                         {"env" "prod"}}
          row     (adapters/cluster->db-row cluster)]
      (is (= "prod" (:name row)))
      (is (= "broker:9092" (:bootstrap_url row)))
      (is (= topic-config-id (:default_topic_configuration_id row)))
      (is (string? (:labels row)))))

  (testing "converts a cluster without optional fields"
    (let [cluster {:name                           "prod"
                   :bootstrap-url                  "broker:9092"
                   :default-topic-configuration-id topic-config-id
                   :labels                         {}}
          row     (adapters/cluster->db-row cluster)]
      (is (= "prod" (:name row)))
      (is (= topic-config-id (:default_topic_configuration_id row))))))

(def ^:private tc-id (UUID/randomUUID))
(def ^:private tc-ts (Timestamp/from now))

(deftest db-row->cluster-with-topic-configuration-test
  (testing "converts a joined DB row to a cluster with embedded topic configuration"
    (let [row {:clusters/id                             id
               :clusters/name                           "prod-cluster"
               :clusters/bootstrap_url                  "broker:9092"
               :clusters/default_topic_configuration_id tc-id
               :clusters/labels                         "{\"env\": \"prod\"}"
               :clusters/created_at                     ts
               :clusters/updated_at                     ts
               :topic_configurations/tc_id                 tc-id
               :topic_configurations/tc_name               "default-config"
               :topic_configurations/tc_partitions         3
               :topic_configurations/tc_replication_factor 2
               :topic_configurations/tc_retention_ms       86400000
               :topic_configurations/tc_configs            "{\"cleanup.policy\": \"compact\"}"
               :topic_configurations/tc_labels             "{\"tier\": \"gold\"}"
               :topic_configurations/tc_created_at         tc-ts
               :topic_configurations/tc_updated_at         tc-ts}
          result (adapters/db-row->cluster-with-topic-configuration row)]
      (is (= id (:id result)))
      (is (= "prod-cluster" (:name result)))
      (is (= "broker:9092" (:bootstrap-url result)))
      (is (= {:env "prod"} (:labels result)))
      (is (instance? Instant (:created-at result)))
      (is (instance? Instant (:updated-at result)))

      (testing "embeds the topic configuration"
        (let [tc (:default-topic-configuration result)]
          (is (= tc-id (:id tc)))
          (is (= "default-config" (:name tc)))
          (is (= 3 (:partitions tc)))
          (is (= 2 (:replication-factor tc)))
          (is (= 86400000 (:retention-ms tc)))
          (is (= {:cleanup.policy "compact"} (:configs tc)))
          (is (= {:tier "gold"} (:labels tc)))
          (is (instance? Instant (:created-at tc)))
          (is (instance? Instant (:updated-at tc)))))))

  (testing "returns nil for nil input"
    (is (nil? (adapters/db-row->cluster-with-topic-configuration nil)))))

(deftest db-row->topic-configuration-test
  (testing "converts a DB row to a topic configuration domain model"
    (let [row {:topic_configurations/id                 id
               :topic_configurations/name               "my-topic"
               :topic_configurations/partitions         3
               :topic_configurations/replication_factor 2
               :topic_configurations/retention_ms       86400000
               :topic_configurations/configs            "{\"cleanup.policy\": \"compact\"}"
               :topic_configurations/labels             "{\"env\": \"prod\"}"
               :topic_configurations/created_at         ts
               :topic_configurations/updated_at         ts}
          result (adapters/db-row->topic-configuration row)]
      (is (= id (:id result)))
      (is (= "my-topic" (:name result)))
      (is (= 3 (:partitions result)))
      (is (= 2 (:replication-factor result)))
      (is (= 86400000 (:retention-ms result)))
      (is (= {:cleanup.policy "compact"} (:configs result)))
      (is (= {:env "prod"} (:labels result)))
      (is (instance? Instant (:created-at result)))
      (is (instance? Instant (:updated-at result)))))

  (testing "returns nil for nil input"
    (is (nil? (adapters/db-row->topic-configuration nil)))))

(deftest topic-configuration->db-row-test
  (testing "converts a topic configuration to a DB row"
    (let [topic-config {:name "my-topic"
                        :partitions 3
                        :replication-factor 2
                        :retention-ms 86400000
                        :configs {"cleanup.policy" "compact"}
                        :labels {"env" "prod"}}
          row          (adapters/topic-configuration->db-row topic-config)]
      (is (= "my-topic" (:name row)))
      (is (= 3 (:partitions row)))
      (is (= 2 (:replication_factor row)))
      (is (= 86400000 (:retention_ms row)))
      (is (string? (:configs row)))
      (is (string? (:labels row))))))
