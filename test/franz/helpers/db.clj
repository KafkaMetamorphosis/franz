(ns franz.helpers.db
  (:require [next.jdbc :as jdbc]
            [franz.models.adapters :as adapters]))

(defn find-claims-for-definition [db topic-definition-id]
  (->> (jdbc/execute! db
         ["SELECT * FROM topic_claims WHERE topic_definition_id = ?::uuid" topic-definition-id])
       (mapv adapters/db-row->topic-claim)))

(defn find-revisions-for-claim [db claim-id]
  (->> (jdbc/execute! db
         ["SELECT * FROM topic_revisions WHERE topic_claim_id = ?::uuid" claim-id])
       (mapv adapters/db-row->topic-revision)))

(defn count-claims [db topic-definition-id]
  (:count (jdbc/execute-one! db
            ["SELECT count(*) FROM topic_claims WHERE topic_definition_id = ?::uuid AND status != 'Deleted'"
             topic-definition-id])))
