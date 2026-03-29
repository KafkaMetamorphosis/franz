(ns franz.wire.in
  (:require [schema.core :as s]
            [schema.coerce :as coerce]
            [schema.utils :as su])
  (:import [java.util UUID]))

(s/defschema CreateClusterRequest
  {:name                                               s/Str
   :bootstrap-url                                      s/Str
   :default-topic-configuration-id                      UUID
   (s/optional-key :labels)                            {s/Keyword s/Str}})

(s/defschema UpdateClusterRequest
  {(s/optional-key :bootstrap-url)                  s/Str
   (s/optional-key :default-topic-configuration-id) UUID
   (s/optional-key :labels)                         {s/Keyword s/Str}})

(s/defschema CreateTopicConfigurationRequest
  {:name                       s/Str
   :partitions                 s/Int
   :replication-factor         s/Int
   :retention-ms               s/Int
   (s/optional-key :configs)   {s/Keyword s/Str}
   (s/optional-key :labels)    {s/Keyword s/Str}})

(s/defschema UpdateTopicConfigurationRequest
  {(s/optional-key :partitions)         s/Int
   (s/optional-key :replication-factor) s/Int
   (s/optional-key :retention-ms)       s/Int
   (s/optional-key :configs)            {s/Keyword s/Str}
   (s/optional-key :labels)             {s/Keyword s/Str}})

(s/defschema CreateTopicDefinitionRequest
  {:topic-name                       s/Str
   :topic-configuration-id           UUID
   (s/optional-key :labels)          {s/Keyword s/Str}})

(s/defschema UpdateTopicDefinitionRequest
  {(s/optional-key :topic-configuration-id) UUID
   (s/optional-key :labels)                 {s/Keyword s/Str}
   (s/optional-key :status)                 s/Str})

(s/defschema PaginationParams
  {(s/optional-key :page) s/Int
   (s/optional-key :size) s/Int})

;; --- Coercion ---

(defn- uuid-matcher [schema]
  (when (= schema UUID)
    (fn [value]
      (if (string? value)
        (try (UUID/fromString value) (catch Exception _ value))
        value))))

(def ^:private request-coercion-matcher
  (coerce/first-matcher [uuid-matcher coerce/json-coercion-matcher]))

(defn- coerce-request [schema body]
  (let [coercer (coerce/coercer schema request-coercion-matcher)
        result  (coercer (or body {}))]
    (if (su/error? result)
      {:valid? false :errors [(pr-str (su/error-val result))]}
      {:valid? true :value result})))

(defn coerce-create-cluster-request [body]
  (coerce-request CreateClusterRequest body))

(defn coerce-update-cluster-request [body]
  (coerce-request UpdateClusterRequest body))

(defn coerce-create-topic-configuration-request [body]
  (coerce-request CreateTopicConfigurationRequest body))

(defn coerce-update-topic-configuration-request [body]
  (coerce-request UpdateTopicConfigurationRequest body))

(defn coerce-create-topic-definition-request [body]
  (coerce-request CreateTopicDefinitionRequest body))

(defn coerce-update-topic-definition-request [body]
  (coerce-request UpdateTopicDefinitionRequest body))
