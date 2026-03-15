(ns franz.unit.ops.health-test
  (:require [clojure.test :refer :all]
            [franz.ops.health :as health]
            [franz.system :as system]))

(deftest run-checks-returns-healthy
  (testing "with a fresh health checker, all checks pass"
    (let [checker (system/new-health-checker)
          result  (health/run-checks checker)]
      (is (= :healthy (:status result)))
      (is (= {"self" :healthy} (:checks result))))))

(deftest ready?-returns-true-when-healthy
  (let [checker (system/new-health-checker)]
    (is (true? (health/ready? checker)))))
