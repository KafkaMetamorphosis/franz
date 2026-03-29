(ns franz.db.topic-revisions
  (:require [next.jdbc :as jdbc]
            [cheshire.core :as json]
            [franz.models.adapters :as adapters]))

(defn insert-topic-revision! [db {:keys [topic-claim-id kafka-cluster-id topic-configuration status]}]
  (->> (jdbc/execute-one! db
         ["INSERT INTO topic_revisions (topic_claim_id, kafka_cluster_id, topic_configuration, status) VALUES (?::uuid, ?::uuid, ?::jsonb, ?) RETURNING *"
          topic-claim-id
          kafka-cluster-id
          (json/generate-string (or topic-configuration {}))
          (or status "PendingReconciliation")])
       adapters/db-row->topic-revision))

(defn find-latest-revision-by-claim-id [db claim-id]
  (->> (jdbc/execute-one! db
         ["SELECT * FROM topic_revisions WHERE topic_claim_id = ?::uuid ORDER BY created_at DESC LIMIT 1"
          claim-id])
       adapters/db-row->topic-revision))
