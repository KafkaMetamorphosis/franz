(ns franz.unit.logic.topic-configuration-test
  (:require [clojure.test :refer :all]
            [franz.logic.topic-configuration :as logic]))

(deftest validate-create-test
  (testing "valid full input"
    (is (= {:valid? true}
           (logic/validate-create {:name "my-topic"
                                   :partitions 3
                                   :replication-factor 2
                                   :retention-ms 86400000}))))

  (testing "blank name"
    (let [result (logic/validate-create {:name ""
                                         :partitions 3
                                         :replication-factor 2
                                         :retention-ms 86400000})]
      (is (false? (:valid? result)))
      (is (some #(= "name must not be empty" %) (:errors result)))))

  (testing "name with whitespace"
    (let [result (logic/validate-create {:name " my-topic "
                                         :partitions 3
                                         :replication-factor 2
                                         :retention-ms 86400000})]
      (is (false? (:valid? result)))
      (is (some #(= "name must not have leading or trailing whitespace" %) (:errors result)))))

  (testing "partitions = 0"
    (let [result (logic/validate-create {:name "my-topic"
                                         :partitions 0
                                         :replication-factor 2
                                         :retention-ms 86400000})]
      (is (false? (:valid? result)))
      (is (some #(= "partitions must be greater than 0" %) (:errors result)))))

  (testing "partitions = -1"
    (let [result (logic/validate-create {:name "my-topic"
                                         :partitions -1
                                         :replication-factor 2
                                         :retention-ms 86400000})]
      (is (false? (:valid? result)))
      (is (some #(= "partitions must be greater than 0" %) (:errors result)))))

  (testing "replication-factor = 0"
    (let [result (logic/validate-create {:name "my-topic"
                                         :partitions 3
                                         :replication-factor 0
                                         :retention-ms 86400000})]
      (is (false? (:valid? result)))
      (is (some #(= "replication-factor must be greater than 0" %) (:errors result)))))

  (testing "retention-ms = 0"
    (let [result (logic/validate-create {:name "my-topic"
                                         :partitions 3
                                         :replication-factor 2
                                         :retention-ms 0})]
      (is (false? (:valid? result)))
      (is (some #(= "retention-ms must be greater than 0" %) (:errors result)))))

  (testing "multiple invalid fields"
    (let [result (logic/validate-create {:name ""
                                         :partitions 0
                                         :replication-factor 0
                                         :retention-ms 0})]
      (is (false? (:valid? result)))
      (is (= 4 (count (:errors result)))))))

(deftest validate-update-test
  (testing "valid update increasing partitions"
    (is (= {:valid? true}
           (logic/validate-update {:partitions 6}
                                  {:partitions 3 :replication-factor 2 :retention-ms 86400000}))))

  (testing "empty map is valid"
    (is (= {:valid? true}
           (logic/validate-update {} {:partitions 3 :replication-factor 2 :retention-ms 86400000}))))

  (testing "partitions decreased below existing"
    (let [result (logic/validate-update {:partitions 1}
                                        {:partitions 3 :replication-factor 2 :retention-ms 86400000})]
      (is (false? (:valid? result)))
      (is (some #(= "partitions can only be increased" %) (:errors result)))))

  (testing "partitions equal to existing is valid"
    (is (= {:valid? true}
           (logic/validate-update {:partitions 3}
                                  {:partitions 3 :replication-factor 2 :retention-ms 86400000}))))

  (testing "replication-factor = 0"
    (let [result (logic/validate-update {:replication-factor 0}
                                        {:partitions 3 :replication-factor 2 :retention-ms 86400000})]
      (is (false? (:valid? result)))
      (is (some #(= "replication-factor must be greater than 0" %) (:errors result)))))

  (testing "retention-ms = 0"
    (let [result (logic/validate-update {:retention-ms 0}
                                        {:partitions 3 :replication-factor 2 :retention-ms 86400000})]
      (is (false? (:valid? result)))
      (is (some #(= "retention-ms must be greater than 0" %) (:errors result))))))
