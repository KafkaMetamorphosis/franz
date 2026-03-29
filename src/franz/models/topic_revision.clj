(ns franz.models.topic-revision
  (:require [schema.core :as s])
  (:import [java.time Instant]
           [java.util UUID]))

(s/defschema TopicRevisionStatus
  (s/enum "PendingReconciliation" "PendingDelete" "Created" "Updated" "Deleted" "Error"))

(s/defschema TopicRevision
  {:id                                      UUID
   :topic-claim-id                          UUID
   :kafka-cluster-id                        UUID
   :topic-configuration                     {s/Str s/Any}
   :status                                  TopicRevisionStatus
   (s/optional-key :last-topic-configuration) (s/maybe {s/Str s/Any})
   (s/optional-key :error)                  (s/maybe s/Str)
   :attempts                                s/Int
   (s/optional-key :retry-of-revision-id)   (s/maybe UUID)
   :created-at                              Instant
   :updated-at                              Instant})
