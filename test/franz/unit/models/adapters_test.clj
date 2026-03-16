(ns franz.unit.models.adapters-test
  (:require [clojure.test :refer :all]
            [franz.models.adapters :as adapters])
  (:import [java.sql Timestamp]
           [java.time Instant]
           [java.util UUID]))

(def ^:private now (Instant/now))
(def ^:private ts (Timestamp/from now))
(def ^:private id (UUID/randomUUID))

(deftest db-row->cluster-test
  (testing "converts a DB row to a cluster domain model"
    (let [row {:clusters/id            id
               :clusters/name          "prod-cluster"
               :clusters/bootstrap_url "broker:9092"
               :clusters/labels        "{\"env\": \"prod\"}"
               :clusters/created_at    ts
               :clusters/updated_at    ts}
          result (adapters/db-row->cluster row)]
      (is (= id (:id result)))
      (is (= "prod-cluster" (:name result)))
      (is (= "broker:9092" (:bootstrap-url result)))
      (is (= {:env "prod"} (:labels result)))
      (is (instance? Instant (:created-at result)))
      (is (instance? Instant (:updated-at result)))))

  (testing "returns nil for nil input"
    (is (nil? (adapters/db-row->cluster nil)))))

(deftest cluster->db-row-test
  (testing "converts a cluster to a DB row"
    (let [cluster {:name "prod" :bootstrap-url "broker:9092" :labels {"env" "prod"}}
          row     (adapters/cluster->db-row cluster)]
      (is (= "prod" (:name row)))
      (is (= "broker:9092" (:bootstrap_url row)))
      (is (string? (:labels row))))))
