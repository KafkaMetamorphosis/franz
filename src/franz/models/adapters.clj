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

(defn- parse-jsonb [v]
  (cond
    (instance? PGobject v) (json/parse-string (.getValue v) true)
    (string? v)            (json/parse-string v true)
    :else                  (or v {})))

(defn- serialize-jsonb [m]
  (json/generate-string (or m {})))

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

