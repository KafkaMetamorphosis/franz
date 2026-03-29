(ns franz.db.topic-claims
  (:require [next.jdbc :as jdbc]
            [cheshire.core :as json]
            [franz.models.adapters :as adapters]))

(defn insert-topic-claim! [db {:keys [topic-definition-id cluster-id status labels]}]
  (->> (jdbc/execute-one! db
         ["INSERT INTO topic_claims (topic_definition_id, cluster_id, status, labels) VALUES (?::uuid, ?::uuid, ?, ?::jsonb) RETURNING *"
          topic-definition-id
          cluster-id
          (or status "Pending")
          (json/generate-string (or labels {}))])
       adapters/db-row->topic-claim))

(defn upsert-topic-claim! [db {:keys [topic-definition-id cluster-id status labels]}]
  (->> (jdbc/execute-one! db
         ["INSERT INTO topic_claims (topic_definition_id, cluster_id, status, labels) VALUES (?::uuid, ?::uuid, ?, ?::jsonb) ON CONFLICT (topic_definition_id, cluster_id) DO UPDATE SET status = EXCLUDED.status, labels = EXCLUDED.labels, updated_at = now() RETURNING *"
          topic-definition-id
          cluster-id
          (or status "Pending")
          (json/generate-string (or labels {}))])
       adapters/db-row->topic-claim))

(defn find-claims-by-topic-definition-id [db topic-definition-id]
  (->> (jdbc/execute! db
         ["SELECT * FROM topic_claims WHERE topic_definition_id = ?::uuid" topic-definition-id])
       (mapv adapters/db-row->topic-claim)))

(defn find-active-claims-by-topic-definition-id [db topic-definition-id]
  (->> (jdbc/execute! db
         ["SELECT * FROM topic_claims WHERE topic_definition_id = ?::uuid AND status != 'Deleted'" topic-definition-id])
       (mapv adapters/db-row->topic-claim)))

(defn update-claim-status! [db claim-id new-status]
  (->> (jdbc/execute-one! db
         ["UPDATE topic_claims SET status = ?, updated_at = now() WHERE id = ?::uuid RETURNING *"
          new-status claim-id])
       adapters/db-row->topic-claim))
