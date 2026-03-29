(ns franz.models.topic-claim
  (:require [schema.core :as s])
  (:import [java.time Instant]
           [java.util UUID]))

(s/defschema TopicClaimStatus
  (s/enum "Pending" "Ready" "Paused" "Deleted" "Error"))

(s/defschema TopicClaim
  {:id                                            UUID
   :topic-definition-id                           UUID
   :cluster-id                                    UUID
   (s/optional-key :topic-configuration-override-id) (s/maybe UUID)
   :status                                        TopicClaimStatus
   :labels                                        {s/Str s/Str}
   (s/optional-key :error)                        (s/maybe s/Str)
   :created-at                                    Instant
   :updated-at                                    Instant})
