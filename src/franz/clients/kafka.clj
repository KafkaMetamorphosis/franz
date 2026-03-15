(ns franz.clients.kafka
  "Kafka producer and consumer client components.
   Clients connect to external dependencies (ADR-001)."
  (:require [com.stuartsierra.component :as component]))

(defrecord KafkaProducer [config]
  component/Lifecycle
  (start [this] (println "KafkaProducer stub started") this)
  (stop  [this] (println "KafkaProducer stub stopped") this))

(defn new-kafka-producer [config]
  (map->KafkaProducer {:config config}))

(defrecord KafkaConsumer [config]
  component/Lifecycle
  (start [this] (println "KafkaConsumer stub started") this)
  (stop  [this] (println "KafkaConsumer stub stopped") this))

(defn new-kafka-consumer [config]
  (map->KafkaConsumer {:config config}))
