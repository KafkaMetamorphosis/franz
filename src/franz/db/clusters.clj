(ns franz.db.clusters
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [cheshire.core :as json]
            [franz.models.adapters :as adapters]
            [franz.logic.pagination :as pagination]))

(defn insert-cluster! [db cluster]
  (->> (jdbc/execute-one! db
         ["INSERT INTO clusters (name, bootstrap_url, labels) VALUES (?, ?, ?::jsonb) RETURNING *"
          (:name cluster)
          (:bootstrap-url cluster)
          (json/generate-string (or (:labels cluster) {}))])
       adapters/db-row->cluster))

(defn find-cluster-by-name [db name]
  (->> (jdbc/execute-one! db
         ["SELECT * FROM clusters WHERE name = ?" name])
       adapters/db-row->cluster))

(defn list-clusters [db params]
  (let [{:keys [size] :as pagination-params} (pagination/parse-params params)
        offset (pagination/->offset pagination-params)
        total  (:count (jdbc/execute-one! db ["SELECT count(*) FROM clusters"]))
        rows   (jdbc/execute! db
                 ["SELECT * FROM clusters ORDER BY created_at DESC LIMIT ? OFFSET ?" size offset])]
    (pagination/page-response (mapv adapters/db-row->cluster rows) pagination-params total)))

(defn update-cluster! [db cluster-name changes]
  (let [fragments (cond-> []
                    (contains? changes :bootstrap-url)
                    (conj {:sql "bootstrap_url = ?" :param (:bootstrap-url changes)})

                    (contains? changes :labels)
                    (conj {:sql "labels = ?::jsonb" :param (json/generate-string (or (:labels changes) {}))}))
        fragments (conj fragments {:sql "updated_at = now()" :param nil})
        set-sql   (str/join ", " (map :sql fragments))
        params    (->> fragments (keep :param) vec)
        params    (conj params cluster-name)
        query     (str "UPDATE clusters SET " set-sql " WHERE name = ? RETURNING *")]
    (->> (jdbc/execute-one! db (into [query] params))
         adapters/db-row->cluster)))

(defn delete-cluster! [db cluster-name]
  (let [result (jdbc/execute-one! db
                 ["DELETE FROM clusters WHERE name = ?" cluster-name])]
    (pos? (or (:next.jdbc/update-count result) 0))))
