(ns franz.models.topic-configuration
  (:require [schema.core :as s])
  (:import [java.time Instant]
           [java.util UUID]))

(s/defschema TopicConfiguration
  {:id                 UUID
   :name               s/Str
   :partitions         s/Int
   :replication-factor s/Int
   :retention-ms       s/Int
   :configs            {s/Str s/Str}
   :labels             {s/Str s/Str}
   :created-at         Instant
   :updated-at         Instant})
