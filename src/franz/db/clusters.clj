(ns franz.db.clusters
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [cheshire.core :as json]
            [franz.models.adapters :as adapters]
            [franz.logic.pagination :as pagination]))

(def ^:private select-cluster-with-topic-configuration
  (str "SELECT clusters.*, "
       "topic_configurations.id AS tc_id, "
       "topic_configurations.name AS tc_name, "
       "topic_configurations.partitions AS tc_partitions, "
       "topic_configurations.replication_factor AS tc_replication_factor, "
       "topic_configurations.retention_ms AS tc_retention_ms, "
       "topic_configurations.configs AS tc_configs, "
       "topic_configurations.labels AS tc_labels, "
       "topic_configurations.created_at AS tc_created_at, "
       "topic_configurations.updated_at AS tc_updated_at "
       "FROM clusters "
       "JOIN topic_configurations ON clusters.default_topic_configuration_id = topic_configurations.id"))

(defn insert-cluster! [db cluster]
  (let [inserted (jdbc/execute-one! db
                   ["INSERT INTO clusters (name, bootstrap_url, default_topic_configuration_id, labels) VALUES (?, ?, ?::uuid, ?::jsonb) RETURNING *"
                    (:name cluster)
                    (:bootstrap-url cluster)
                    (:default-topic-configuration-id cluster)
                    (json/generate-string (or (:labels cluster) {}))])]
    (->> (jdbc/execute-one! db
           [(str select-cluster-with-topic-configuration " WHERE clusters.name = ?")
            (:clusters/name inserted)])
         adapters/db-row->cluster-with-topic-configuration)))

(defn find-cluster-by-name [db name]
  (->> (jdbc/execute-one! db
         [(str select-cluster-with-topic-configuration " WHERE clusters.name = ?") name])
       adapters/db-row->cluster-with-topic-configuration))

(defn list-clusters [db params]
  (let [{:keys [size] :as pagination-params} (pagination/parse-params params)
        offset (pagination/->offset pagination-params)
        total  (:count (jdbc/execute-one! db ["SELECT count(*) FROM clusters"]))
        rows   (jdbc/execute! db
                 [(str select-cluster-with-topic-configuration
                       " ORDER BY clusters.created_at DESC LIMIT ? OFFSET ?")
                  size offset])]
    (pagination/page-response (mapv adapters/db-row->cluster-with-topic-configuration rows)
                              pagination-params total)))

(defn update-cluster! [db cluster-name changes]
  (let [fragments (cond-> []
                    (contains? changes :bootstrap-url)
                    (conj {:sql "bootstrap_url = ?" :param (:bootstrap-url changes)})

                    (contains? changes :default-topic-configuration-id)
                    (conj {:sql "default_topic_configuration_id = ?::uuid" :param (:default-topic-configuration-id changes)})

                    (contains? changes :labels)
                    (conj {:sql "labels = ?::jsonb" :param (json/generate-string (or (:labels changes) {}))}))
        fragments (conj fragments {:sql "updated_at = now()" :param nil})
        set-sql   (str/join ", " (map :sql fragments))
        params    (->> fragments (keep :param) vec)
        params    (conj params cluster-name)
        query     (str "UPDATE clusters SET " set-sql " WHERE name = ? RETURNING *")]
    (jdbc/execute-one! db (into [query] params))
    (->> (jdbc/execute-one! db
           [(str select-cluster-with-topic-configuration " WHERE clusters.name = ?")
            cluster-name])
         adapters/db-row->cluster-with-topic-configuration)))

(defn delete-cluster! [db cluster-name]
  (let [result (jdbc/execute-one! db
                 ["DELETE FROM clusters WHERE name = ?" cluster-name])]
    (pos? (or (:next.jdbc/update-count result) 0))))

(defn list-all-clusters [db]
  (->> (jdbc/execute! db ["SELECT * FROM clusters"])
       (mapv adapters/db-row->cluster)))
