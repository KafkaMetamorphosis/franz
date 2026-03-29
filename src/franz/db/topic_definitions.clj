(ns franz.db.topic-definitions
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [cheshire.core :as json]
            [franz.models.adapters :as adapters]
            [franz.logic.pagination :as pagination]))

(defn insert-topic-definition! [db topic-definition]
  (->> (jdbc/execute-one! db
         ["INSERT INTO topic_definitions (topic_name, topic_configuration_id, labels) VALUES (?, ?::uuid, ?::jsonb) RETURNING *"
          (:topic-name topic-definition)
          (:topic-configuration-id topic-definition)
          (json/generate-string (or (:labels topic-definition) {}))])
       adapters/db-row->topic-definition))

(defn find-topic-definition-by-name [db topic-name]
  (->> (jdbc/execute-one! db
         ["SELECT * FROM topic_definitions WHERE topic_name = ? AND status != 'Deleted'" topic-name])
       adapters/db-row->topic-definition))

(defn find-topic-definition-by-name-any-status [db topic-name]
  (->> (jdbc/execute-one! db
         ["SELECT * FROM topic_definitions WHERE topic_name = ?" topic-name])
       adapters/db-row->topic-definition))

(defn list-topic-definitions [db params]
  (let [{:keys [size] :as pagination-params} (pagination/parse-params params)
        offset    (pagination/->offset pagination-params)
        status    (:status params)
        where-sql (if status
                    "WHERE status = ?"
                    "WHERE status != 'Deleted'")
        count-params (if status [status] [])
        total     (:count (jdbc/execute-one! db
                    (into [(str "SELECT count(*) FROM topic_definitions " where-sql)]
                          count-params)))
        rows      (jdbc/execute! db
                    (into [(str "SELECT * FROM topic_definitions " where-sql
                                " ORDER BY created_at DESC LIMIT ? OFFSET ?")]
                          (concat count-params [size offset])))]
    (pagination/page-response (mapv adapters/db-row->topic-definition rows)
                              pagination-params total)))

(defn update-topic-definition! [db topic-name changes]
  (let [fragments (cond-> []
                    (contains? changes :topic-configuration-id)
                    (conj {:sql "topic_configuration_id = ?::uuid" :param (:topic-configuration-id changes)})

                    (contains? changes :labels)
                    (conj {:sql "labels = ?::jsonb" :param (json/generate-string (or (:labels changes) {}))})

                    (contains? changes :status)
                    (conj {:sql "status = ?" :param (:status changes)}))
        fragments (conj fragments {:sql "updated_at = now()" :param nil})
        set-sql   (str/join ", " (map :sql fragments))
        params    (->> fragments (keep :param) vec)
        params    (conj params topic-name)
        query     (str "UPDATE topic_definitions SET " set-sql " WHERE topic_name = ? AND status != 'Deleted' RETURNING *")]
    (->> (jdbc/execute-one! db (into [query] params))
         adapters/db-row->topic-definition)))

(defn soft-delete-topic-definition! [db topic-name]
  (->> (jdbc/execute-one! db
         ["UPDATE topic_definitions SET status = 'Deleted', updated_at = now() WHERE topic_name = ? AND status != 'Deleted' RETURNING *"
          topic-name])
       adapters/db-row->topic-definition))
