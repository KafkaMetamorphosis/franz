(ns franz.unit.ops.health-test
  (:require [clojure.test :refer :all]
            [franz.ops.health :as health]))

(deftest run-checks-returns-healthy
  (testing "with a fresh health checker, all checks pass"
    (let [result (health/run-checks)]
      (is (= :healthy (:status result)))
      (is (= {"self" :healthy} (:checks result))))))

(deftest ready?-returns-true-when-healthy
  (is (true? (health/ready?))))
