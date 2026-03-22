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
    {:id            (:clusters/id row)
     :name          (:clusters/name row)
     :bootstrap-url (:clusters/bootstrap_url row)
     :labels        (parse-jsonb (:clusters/labels row))
     :created-at    (timestamp->instant (:clusters/created_at row))
     :updated-at    (timestamp->instant (:clusters/updated_at row))}))

(defn cluster->db-row [cluster]
  {:name          (:name cluster)
   :bootstrap_url (:bootstrap-url cluster)
   :labels        (serialize-jsonb (:labels cluster))})

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

