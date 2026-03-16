(ns franz.models.cluster
  (:require [schema.core :as s])
  (:import [java.time Instant]
           [java.util UUID]))

(s/defschema Cluster
  {:id            UUID
   :name          s/Str
   :bootstrap-url s/Str
   :labels        {s/Str s/Str}
   :created-at    Instant
   :updated-at    Instant})
