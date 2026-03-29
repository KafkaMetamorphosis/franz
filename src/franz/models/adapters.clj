(ns franz.models.adapters
  (:require [cheshire.core :as json])
  (:import [java.sql Timestamp]
           [java.time Instant]
           [org.postgresql.util PGobject]))

(defn- timestamp->instant [^Timestamp ts]
  (when ts
    (.toInstant ts)))

(defn- instant->timestamp [^Instant inst]
  (when inst
    (Timestamp/from inst)))

(defn- parse-jsonb [value]
  (cond
    (instance? PGobject value) (json/parse-string (.getValue value) true)
    (string? value)            (json/parse-string value true)
    :else                      (or value {})))

(defn- serialize-jsonb [value]
  (json/generate-string (or value {})))

(defn db-row->cluster [row]
  (when row
    {:id                             (:clusters/id row)
     :name                           (:clusters/name row)
     :bootstrap-url                  (:clusters/bootstrap_url row)
     :default-topic-configuration-id (:clusters/default_topic_configuration_id row)
     :labels                         (parse-jsonb (:clusters/labels row))
     :created-at                     (timestamp->instant (:clusters/created_at row))
     :updated-at                     (timestamp->instant (:clusters/updated_at row))}))

(defn cluster->db-row [cluster]
  {:name                           (:name cluster)
   :bootstrap_url                  (:bootstrap-url cluster)
   :default_topic_configuration_id (:default-topic-configuration-id cluster)
   :labels                         (serialize-jsonb (:labels cluster))})

(defn db-row->cluster-with-topic-configuration [row]
  (when row
    {:id                          (:clusters/id row)
     :name                        (:clusters/name row)
     :bootstrap-url               (:clusters/bootstrap_url row)
     :labels                      (parse-jsonb (:clusters/labels row))
     :created-at                  (timestamp->instant (:clusters/created_at row))
     :updated-at                  (timestamp->instant (:clusters/updated_at row))
     :default-topic-configuration {:id                 (:topic_configurations/tc_id row)
                                   :name               (:topic_configurations/tc_name row)
                                   :partitions         (:topic_configurations/tc_partitions row)
                                   :replication-factor (:topic_configurations/tc_replication_factor row)
                                   :retention-ms       (:topic_configurations/tc_retention_ms row)
                                   :configs            (parse-jsonb (:topic_configurations/tc_configs row))
                                   :labels             (parse-jsonb (:topic_configurations/tc_labels row))
                                   :created-at         (timestamp->instant (:topic_configurations/tc_created_at row))
                                   :updated-at         (timestamp->instant (:topic_configurations/tc_updated_at row))}}))

(defn db-row->topic-configuration [row]
  (when row
    {:id                 (:topic_configurations/id row)
     :name               (:topic_configurations/name row)
     :partitions         (:topic_configurations/partitions row)
     :replication-factor (:topic_configurations/replication_factor row)
     :retention-ms       (:topic_configurations/retention_ms row)
     :configs            (parse-jsonb (:topic_configurations/configs row))
     :labels             (parse-jsonb (:topic_configurations/labels row))
     :created-at         (timestamp->instant (:topic_configurations/created_at row))
     :updated-at         (timestamp->instant (:topic_configurations/updated_at row))}))

(defn topic-configuration->db-row [topic-config]
  {:name               (:name topic-config)
   :partitions         (:partitions topic-config)
   :replication_factor (:replication-factor topic-config)
   :retention_ms       (:retention-ms topic-config)
   :configs            (serialize-jsonb (:configs topic-config))
   :labels             (serialize-jsonb (:labels topic-config))})

(defn db-row->topic-definition [row]
  (when row
    {:id                     (:topic_definitions/id row)
     :topic-name             (:topic_definitions/topic_name row)
     :topic-configuration-id (:topic_definitions/topic_configuration_id row)
     :status                 (:topic_definitions/status row)
     :expansion-status       (:topic_definitions/expansion_status row)
     :labels                 (parse-jsonb (:topic_definitions/labels row))
     :created-at             (timestamp->instant (:topic_definitions/created_at row))
     :updated-at             (timestamp->instant (:topic_definitions/updated_at row))}))

(defn db-row->topic-claim [row]
  (when row
    {:id                              (:topic_claims/id row)
     :topic-definition-id             (:topic_claims/topic_definition_id row)
     :cluster-id                      (:topic_claims/cluster_id row)
     :topic-configuration-override-id (:topic_claims/topic_configuration_override_id row)
     :status                          (:topic_claims/status row)
     :labels                          (parse-jsonb (:topic_claims/labels row))
     :error                           (:topic_claims/error row)
     :created-at                      (timestamp->instant (:topic_claims/created_at row))
     :updated-at                      (timestamp->instant (:topic_claims/updated_at row))}))

(defn db-row->topic-revision [row]
  (when row
    {:id                       (:topic_revisions/id row)
     :topic-claim-id           (:topic_revisions/topic_claim_id row)
     :kafka-cluster-id         (:topic_revisions/kafka_cluster_id row)
     :topic-configuration      (parse-jsonb (:topic_revisions/topic_configuration row))
     :status                   (:topic_revisions/status row)
     :last-topic-configuration (parse-jsonb (:topic_revisions/last_topic_configuration row))
     :error                    (:topic_revisions/error row)
     :attempts                 (:topic_revisions/attempts row)
     :retry-of-revision-id     (:topic_revisions/retry_of_revision_id row)
     :created-at               (timestamp->instant (:topic_revisions/created_at row))
     :updated-at               (timestamp->instant (:topic_revisions/updated_at row))}))

