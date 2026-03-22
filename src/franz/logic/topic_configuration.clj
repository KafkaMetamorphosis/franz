(ns franz.logic.topic-configuration
  (:require [clojure.string :as str]))

(defn validate-create [{:keys [name partitions replication-factor retention-ms]}]
  (let [errors (cond-> []
                 (str/blank? name)
                 (conj "name must not be empty")

                 (and (not (str/blank? name))
                      (not= name (str/trim name)))
                 (conj "name must not have leading or trailing whitespace")

                 (or (nil? partitions) (<= partitions 0))
                 (conj "partitions must be greater than 0")

                 (or (nil? replication-factor) (<= replication-factor 0))
                 (conj "replication-factor must be greater than 0")

                 (or (nil? retention-ms) (<= retention-ms 0))
                 (conj "retention-ms must be greater than 0"))]
    (if (seq errors)
      {:valid? false :errors errors}
      {:valid? true})))

(defn validate-update [changes existing]
  (let [errors (cond-> []
                 (and (contains? changes :partitions)
                      (<= (:partitions changes) 0))
                 (conj "partitions must be greater than 0")

                 (and (contains? changes :partitions)
                      (> (:partitions changes) 0)
                      (< (:partitions changes) (:partitions existing)))
                 (conj "partitions can only be increased")

                 (and (contains? changes :replication-factor)
                      (<= (:replication-factor changes) 0))
                 (conj "replication-factor must be greater than 0")

                 (and (contains? changes :retention-ms)
                      (<= (:retention-ms changes) 0))
                 (conj "retention-ms must be greater than 0"))]
    (if (seq errors)
      {:valid? false :errors errors}
      {:valid? true})))
