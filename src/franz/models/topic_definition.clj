(ns franz.models.topic-definition
  (:require [schema.core :as s])
  (:import [java.time Instant]
           [java.util UUID]))

(s/defschema TopicDefinitionStatus
  (s/enum "Active" "Paused" "Error" "Deleted"))

(s/defschema ExpansionStatus
  (s/enum "Expanded" "PendingExpansion"))

(s/defschema TopicDefinition
  {:id                     UUID
   :topic-name             s/Str
   :topic-configuration-id UUID
   :status                 TopicDefinitionStatus
   :expansion-status       ExpansionStatus
   :labels                 {s/Str s/Str}
   :created-at             Instant
   :updated-at             Instant})
