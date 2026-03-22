(ns franz.unit.logic.pagination-test
  (:require [clojure.test :refer :all]
            [franz.logic.pagination :as pagination]))

(deftest parse-params-test
  (testing "defaults when no params"
    (is (= {:page 1 :size 20}
           (pagination/parse-params {}))))

  (testing "parses string page and size"
    (is (= {:page 3 :size 10}
           (pagination/parse-params {:page "3" :size "10"}))))

  (testing "parses integer page and size"
    (is (= {:page 2 :size 50}
           (pagination/parse-params {:page 2 :size 50}))))

  (testing "partial params use defaults"
    (is (= {:page 5 :size 20}
           (pagination/parse-params {:page "5"})))))

(deftest ->offset-test
  (testing "first page offset is 0"
    (is (= 0 (pagination/->offset {:page 1 :size 20}))))

  (testing "second page offset"
    (is (= 20 (pagination/->offset {:page 2 :size 20}))))

  (testing "custom size"
    (is (= 30 (pagination/->offset {:page 4 :size 10})))))

(deftest page-response-test
  (testing "builds page response"
    (is (= {:items [:a :b] :page 1 :size 20 :total 2}
           (pagination/page-response [:a :b] {:page 1 :size 20} 2)))))
