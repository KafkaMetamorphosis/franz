(ns franz.unit.logic.topic-definition-test
  (:require [clojure.test :refer :all]
            [franz.logic.topic-definition :as logic]))

(deftest validate-create-test
  (testing "valid topic-name"
    (is (= {:valid? true}
           (logic/validate-create {:topic-name "my-topic"}))))

  (testing "blank topic-name"
    (let [result (logic/validate-create {:topic-name ""})]
      (is (false? (:valid? result)))
      (is (some #(= "topic-name must not be empty" %) (:errors result)))))

  (testing "nil topic-name"
    (let [result (logic/validate-create {:topic-name nil})]
      (is (false? (:valid? result)))))

  (testing "topic-name with leading whitespace"
    (let [result (logic/validate-create {:topic-name " my-topic"})]
      (is (false? (:valid? result)))
      (is (some #(= "topic-name must not have leading or trailing whitespace" %) (:errors result)))))

  (testing "topic-name with trailing whitespace"
    (let [result (logic/validate-create {:topic-name "my-topic "})]
      (is (false? (:valid? result)))
      (is (some #(= "topic-name must not have leading or trailing whitespace" %) (:errors result))))))

(deftest valid-api-transition-test
  (testing "Active -> Paused is allowed"
    (is (true? (:allowed? (logic/valid-api-transition? "Active" "Paused")))))

  (testing "Paused -> Active is allowed"
    (is (true? (:allowed? (logic/valid-api-transition? "Paused" "Active")))))

  (testing "Error -> Paused is allowed"
    (is (true? (:allowed? (logic/valid-api-transition? "Error" "Paused")))))

  (testing "Active -> Error is not allowed"
    (let [result (logic/valid-api-transition? "Active" "Error")]
      (is (false? (:allowed? result)))
      (is (= "status cannot be set to Error via the API" (:reason result)))))

  (testing "Active -> Deleted is not allowed via PUT"
    (let [result (logic/valid-api-transition? "Active" "Deleted")]
      (is (false? (:allowed? result)))
      (is (= "status cannot be set to Deleted via PUT; use DELETE endpoint" (:reason result)))))

  (testing "Paused -> Error is not allowed"
    (let [result (logic/valid-api-transition? "Paused" "Error")]
      (is (false? (:allowed? result)))
      (is (= "status cannot be set to Error via the API" (:reason result)))))

  (testing "Paused -> Deleted is not allowed"
    (let [result (logic/valid-api-transition? "Paused" "Deleted")]
      (is (false? (:allowed? result)))
      (is (= "status cannot be set to Deleted via PUT; use DELETE endpoint" (:reason result)))))

  (testing "Active -> Active is not allowed (same state)"
    (let [result (logic/valid-api-transition? "Active" "Active")]
      (is (false? (:allowed? result)))))

  (testing "Deleted -> Active is not allowed"
    (let [result (logic/valid-api-transition? "Deleted" "Active")]
      (is (false? (:allowed? result)))))

  (testing "Error -> Active is not allowed"
    (let [result (logic/valid-api-transition? "Error" "Active")]
      (is (false? (:allowed? result))))))

(deftest validate-update-test
  (testing "valid update without status change"
    (is (= {:valid? true}
           (logic/validate-update {:labels {"env" "prod"}}
                                  {:status "Active"}))))

  (testing "empty update is valid"
    (is (= {:valid? true}
           (logic/validate-update {} {:status "Active"}))))

  (testing "valid status transition Active -> Paused"
    (is (= {:valid? true}
           (logic/validate-update {:status "Paused"}
                                  {:status "Active"}))))

  (testing "invalid status transition Active -> Error"
    (let [result (logic/validate-update {:status "Error"}
                                        {:status "Active"})]
      (is (false? (:valid? result)))
      (is (some #(= "status cannot be set to Error via the API" %) (:errors result)))))

  (testing "invalid status transition Active -> Deleted"
    (let [result (logic/validate-update {:status "Deleted"}
                                        {:status "Active"})]
      (is (false? (:valid? result)))
      (is (some #(= "status cannot be set to Deleted via PUT; use DELETE endpoint" %) (:errors result))))))
