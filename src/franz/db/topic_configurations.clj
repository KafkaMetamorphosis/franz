(ns franz.db.topic-configurations
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [cheshire.core :as json]
            [franz.models.adapters :as adapters]
            [franz.logic.pagination :as pagination]))

(defn insert-topic-configuration! [db topic-config]
  (->> (jdbc/execute-one! db
         ["INSERT INTO topic_configurations (name, partitions, replication_factor, retention_ms, configs, labels) VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb) RETURNING *"
          (:name topic-config)
          (:partitions topic-config)
          (:replication-factor topic-config)
          (:retention-ms topic-config)
          (json/generate-string (or (:configs topic-config) {}))
          (json/generate-string (or (:labels topic-config) {}))])
       adapters/db-row->topic-configuration))

(defn find-topic-configuration-by-id [db id]
  (->> (jdbc/execute-one! db
         ["SELECT * FROM topic_configurations WHERE id = ?" (java.util.UUID/fromString id)])
       adapters/db-row->topic-configuration))

(defn find-topic-configuration-by-name [db name]
  (->> (jdbc/execute-one! db
         ["SELECT * FROM topic_configurations WHERE name = ?" name])
       adapters/db-row->topic-configuration))

(defn list-topic-configurations [db params]
  (let [{:keys [size] :as pagination-params} (pagination/parse-params params)
        offset (pagination/->offset pagination-params)
        total  (:count (jdbc/execute-one! db ["SELECT count(*) FROM topic_configurations"]))
        rows   (jdbc/execute! db
                 ["SELECT * FROM topic_configurations ORDER BY created_at DESC LIMIT ? OFFSET ?" size offset])]
    (pagination/page-response (mapv adapters/db-row->topic-configuration rows) pagination-params total)))

(defn update-topic-configuration! [db id changes]
  (let [set-fragments (cond-> []
                       (contains? changes :partitions)
                       (conj {:sql "partitions = ?" :param (:partitions changes)})

                       (contains? changes :replication-factor)
                       (conj {:sql "replication_factor = ?" :param (:replication-factor changes)})

                       (contains? changes :retention-ms)
                       (conj {:sql "retention_ms = ?" :param (:retention-ms changes)})

                       (contains? changes :configs)
                       (conj {:sql "configs = ?::jsonb" :param (json/generate-string (or (:configs changes) {}))})

                       (contains? changes :labels)
                       (conj {:sql "labels = ?::jsonb" :param (json/generate-string (or (:labels changes) {}))}))
        all-fragments  (conj set-fragments {:sql "updated_at = now()" :param nil})
        set-clause     (str/join ", " (map :sql all-fragments))
        query-params   (conj (->> all-fragments (keep :param) vec) (java.util.UUID/fromString id))
        query          (str "UPDATE topic_configurations SET " set-clause " WHERE id = ? RETURNING *")]
    (->> (jdbc/execute-one! db (into [query] query-params))
         adapters/db-row->topic-configuration)))

(defn delete-topic-configuration! [db id]
  (let [result (jdbc/execute-one! db
                 ["DELETE FROM topic_configurations WHERE id = ?" (java.util.UUID/fromString id)])]
    (pos? (or (:next.jdbc/update-count result) 0))))
