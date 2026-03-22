(ns franz.unit.logic.cluster-test
  (:require [clojure.test :refer :all]
            [franz.logic.cluster :as logic]))

(deftest validate-create-test
  (testing "valid cluster"
    (is (= {:valid? true}
           (logic/validate-create {:name "my-cluster" :bootstrap-url "broker:9092"}))))

  (testing "empty name"
    (let [result (logic/validate-create {:name "" :bootstrap-url "broker:9092"})]
      (is (false? (:valid? result)))
      (is (some #(= "name must not be empty" %) (:errors result)))))

  (testing "nil name"
    (let [result (logic/validate-create {:name nil :bootstrap-url "broker:9092"})]
      (is (false? (:valid? result)))))

  (testing "name with leading whitespace"
    (let [result (logic/validate-create {:name " my-cluster" :bootstrap-url "broker:9092"})]
      (is (false? (:valid? result)))
      (is (some #(= "name must not have leading or trailing whitespace" %) (:errors result)))))

  (testing "name with trailing whitespace"
    (let [result (logic/validate-create {:name "my-cluster " :bootstrap-url "broker:9092"})]
      (is (false? (:valid? result)))))

  (testing "empty bootstrap-url"
    (let [result (logic/validate-create {:name "my-cluster" :bootstrap-url ""})]
      (is (false? (:valid? result)))
      (is (some #(= "bootstrap-url must not be empty" %) (:errors result)))))

  (testing "multiple errors"
    (let [result (logic/validate-create {:name "" :bootstrap-url ""})]
      (is (false? (:valid? result)))
      (is (= 2 (count (:errors result)))))))

(deftest validate-update-test
  (testing "valid update with bootstrap-url"
    (is (= {:valid? true}
           (logic/validate-update {:bootstrap-url "new-broker:9092"}))))

  (testing "empty update is valid"
    (is (= {:valid? true}
           (logic/validate-update {}))))

  (testing "empty bootstrap-url in update"
    (let [result (logic/validate-update {:bootstrap-url ""})]
      (is (false? (:valid? result))))))
