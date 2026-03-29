(ns franz.unit.logic.expansion-test
  (:require [clojure.test :refer :all]
            [franz.logic.expansion :as expansion])
  (:import [java.util UUID]))

;; --- parse-taint ---

(deftest parse-taint-test
  (testing "returns nil when cluster has no taint label"
    (is (nil? (expansion/parse-taint {:env "production"}))))

  (testing "returns nil when cluster labels are empty"
    (is (nil? (expansion/parse-taint {}))))

  (testing "parses drain taint"
    (is (= {:name "decommission" :effect :drain}
           (expansion/parse-taint {:franz.taint "decommission:drain"}))))

  (testing "parses no-creation taint"
    (is (= {:name "unstable" :effect :no-creation}
           (expansion/parse-taint {:franz.taint "unstable:no-creation"}))))

  (testing "returns nil for malformed taint without colon"
    (is (nil? (expansion/parse-taint {:franz.taint "malformed"})))))

;; --- parse-tolerations ---

(deftest parse-tolerations-test
  (testing "returns empty set when no toleration label"
    (is (= #{} (expansion/parse-tolerations {}))))

  (testing "parses single toleration"
    (is (= #{"unstable:no-creation"}
           (expansion/parse-tolerations {:franz.taint/toleration "unstable:no-creation"}))))

  (testing "parses comma-separated tolerations"
    (is (= #{"unstable:no-creation" "old:no-creation"}
           (expansion/parse-tolerations {:franz.taint/toleration "unstable:no-creation,old:no-creation"}))))

  (testing "trims whitespace around toleration entries"
    (is (= #{"unstable:no-creation" "old:no-creation"}
           (expansion/parse-tolerations {:franz.taint/toleration "unstable:no-creation , old:no-creation"})))))

;; --- taint-allows-placement? ---

(deftest taint-allows-placement?-test
  (testing "cluster with no taint always allows placement"
    (is (true? (expansion/taint-allows-placement?
                 {:id (UUID/randomUUID) :labels {}}
                 {:id (UUID/randomUUID) :labels {}}))))

  (testing "drain taint always prevents placement regardless of toleration"
    (is (false? (expansion/taint-allows-placement?
                  {:id (UUID/randomUUID) :labels {:franz.taint "decommission:drain"}}
                  {:id (UUID/randomUUID) :labels {:franz.taint/toleration "decommission:drain"}}))))

  (testing "no-creation taint without toleration prevents placement"
    (is (false? (expansion/taint-allows-placement?
                  {:id (UUID/randomUUID) :labels {:franz.taint "unstable:no-creation"}}
                  {:id (UUID/randomUUID) :labels {}}))))

  (testing "no-creation taint with matching toleration allows placement"
    (is (true? (expansion/taint-allows-placement?
                 {:id (UUID/randomUUID) :labels {:franz.taint "unstable:no-creation"}}
                 {:id (UUID/randomUUID) :labels {:franz.taint/toleration "unstable:no-creation"}})))))

;; --- cluster-is-draining? ---

(deftest cluster-is-draining?-test
  (testing "cluster with drain taint is draining"
    (is (true? (expansion/cluster-is-draining?
                 {:id (UUID/randomUUID) :labels {:franz.taint "decommission:drain"}}))))

  (testing "cluster with no-creation taint is not draining"
    (is (false? (expansion/cluster-is-draining?
                  {:id (UUID/randomUUID) :labels {:franz.taint "unstable:no-creation"}}))))

  (testing "cluster with no taint is not draining"
    (is (false? (expansion/cluster-is-draining?
                  {:id (UUID/randomUUID) :labels {}})))))

;; --- parse-affinity-selectors ---

(deftest parse-affinity-selectors-test
  (testing "returns empty vec when no selector label"
    (is (= [] (expansion/parse-affinity-selectors {}))))

  (testing "parses single selector"
    (is (= [{:key "env" :value "production"}]
           (expansion/parse-affinity-selectors {:franz.affinity/selector "env=production"}))))

  (testing "parses comma-separated selectors"
    (is (= [{:key "env" :value "production"} {:key "region" :value "us-east"}]
           (expansion/parse-affinity-selectors {:franz.affinity/selector "env=production,region=us-east"}))))

  (testing "trims whitespace"
    (is (= [{:key "env" :value "production"}]
           (expansion/parse-affinity-selectors {:franz.affinity/selector " env=production "})))))

;; --- cluster-matches-selectors? ---

(deftest cluster-matches-selectors?-test
  (testing "empty selectors always match"
    (is (true? (expansion/cluster-matches-selectors? {:env "test"} []))))

  (testing "matching selector returns true"
    (is (true? (expansion/cluster-matches-selectors?
                 {:env "production"}
                 [{:key "env" :value "production"}]))))

  (testing "non-matching selector returns false"
    (is (false? (expansion/cluster-matches-selectors?
                  {:env "staging"}
                  [{:key "env" :value "production"}]))))

  (testing "all selectors must match"
    (is (false? (expansion/cluster-matches-selectors?
                  {:env "production"}
                  [{:key "env" :value "production"} {:key "region" :value "us-east"}])))))

;; --- parse-shard-size ---

(deftest parse-shard-size-test
  (testing "returns nil when label absent"
    (is (nil? (expansion/parse-shard-size {}))))

  (testing "returns integer when label present"
    (is (= 3 (expansion/parse-shard-size {:franz.affinity/shard-size "3"}))))

  (testing "returns nil for non-numeric value"
    (is (nil? (expansion/parse-shard-size {:franz.affinity/shard-size "abc"})))))

;; --- cluster-weight ---

(deftest cluster-weight-test
  (testing "returns 0 when no weight label"
    (is (= 0 (expansion/cluster-weight {}))))

  (testing "returns parsed weight"
    (is (= 10 (expansion/cluster-weight {:franz.affinity/weight "10"}))))

  (testing "returns 0 for non-numeric value"
    (is (= 0 (expansion/cluster-weight {:franz.affinity/weight "abc"})))))

;; --- select-eligible-clusters ---

(deftest select-eligible-clusters-test
  (let [cluster-a {:id (UUID/randomUUID) :name "a" :labels {:env "production" :franz.affinity/weight "10"}}
        cluster-b {:id (UUID/randomUUID) :name "b" :labels {:env "staging" :franz.affinity/weight "5"}}
        cluster-c {:id (UUID/randomUUID) :name "c" :labels {:env "production" :franz.taint "decommission:drain"}}
        cluster-d {:id (UUID/randomUUID) :name "d" :labels {:env "production" :franz.taint "unstable:no-creation"}}]

    (testing "returns all non-tainted clusters when no selectors"
      (let [definition {:id (UUID/randomUUID) :labels {}}
            result (expansion/select-eligible-clusters [cluster-a cluster-b] definition)]
        (is (= 2 (count (:eligible-clusters result))))
        (is (= 0 (count (:draining-clusters result))))))

    (testing "separates draining clusters"
      (let [definition {:id (UUID/randomUUID) :labels {}}
            result (expansion/select-eligible-clusters [cluster-a cluster-c] definition)]
        (is (= 1 (count (:eligible-clusters result))))
        (is (= (:id cluster-a) (:id (first (:eligible-clusters result)))))
        (is (= 1 (count (:draining-clusters result))))
        (is (= (:id cluster-c) (:id (first (:draining-clusters result)))))))

    (testing "filters by no-creation taint without toleration"
      (let [definition {:id (UUID/randomUUID) :labels {}}
            result (expansion/select-eligible-clusters [cluster-a cluster-d] definition)]
        (is (= 1 (count (:eligible-clusters result))))
        (is (= (:id cluster-a) (:id (first (:eligible-clusters result)))))))

    (testing "no-creation taint with toleration allows placement"
      (let [definition {:id (UUID/randomUUID) :labels {:franz.taint/toleration "unstable:no-creation"}}
            result (expansion/select-eligible-clusters [cluster-a cluster-d] definition)]
        (is (= 2 (count (:eligible-clusters result))))))

    (testing "selector filters clusters and sorts by weight desc"
      (let [definition {:id (UUID/randomUUID) :labels {:franz.affinity/selector "env=production"}}
            result (expansion/select-eligible-clusters [cluster-a cluster-b] definition)]
        (is (= 1 (count (:eligible-clusters result))))
        (is (= (:id cluster-a) (:id (first (:eligible-clusters result)))))))

    (testing "shard-size limits when selectors present"
      (let [cluster-e {:id (UUID/randomUUID) :name "e" :labels {:env "production" :franz.affinity/weight "1"}}
            definition {:id (UUID/randomUUID) :labels {:franz.affinity/selector "env=production"
                                                       :franz.affinity/shard-size "1"}}
            result (expansion/select-eligible-clusters [cluster-a cluster-e] definition)]
        (is (= 1 (count (:eligible-clusters result))))
        (is (= (:id cluster-a) (:id (first (:eligible-clusters result)))))))

    (testing "shard-size ignored when no selectors"
      (let [definition {:id (UUID/randomUUID) :labels {:franz.affinity/shard-size "1"}}
            result (expansion/select-eligible-clusters [cluster-a cluster-b] definition)]
        (is (= 2 (count (:eligible-clusters result))))))))

;; --- materialize-topic-configuration ---

(deftest materialize-topic-configuration-test
  (testing "merges three layers with override on top"
    (let [cluster-default {:partitions 3 :replication-factor 2 :retention-ms 86400000
                           :configs {:cleanup.policy "delete"}}
          definition-config {:partitions 6 :replication-factor 3 :retention-ms 172800000
                             :configs {:cleanup.policy "compact"}}
          override-config {:partitions 12 :replication-factor 3 :retention-ms 172800000
                           :configs {:cleanup.policy "compact" :segment.ms "100000"}}]
      (is (= {:partitions 12 :replication-factor 3 :retention-ms 172800000
              :configs {:cleanup.policy "compact" :segment.ms "100000"}}
             (expansion/materialize-topic-configuration cluster-default definition-config override-config)))))

  (testing "nil override uses definition config"
    (let [cluster-default {:partitions 3 :replication-factor 2 :retention-ms 86400000
                           :configs {:cleanup.policy "delete"}}
          definition-config {:partitions 6 :replication-factor 2 :retention-ms 86400000
                             :configs {:cleanup.policy "compact"}}]
      (is (= {:partitions 6 :replication-factor 2 :retention-ms 86400000
              :configs {:cleanup.policy "compact"}}
             (expansion/materialize-topic-configuration cluster-default definition-config nil)))))

  (testing "configs deep-merge preserves non-overlapping keys"
    (let [cluster-default {:partitions 3 :replication-factor 2 :retention-ms 86400000
                           :configs {:cleanup.policy "delete" :segment.bytes "1073741824"}}
          definition-config {:partitions 3 :replication-factor 2 :retention-ms 86400000
                             :configs {:cleanup.policy "compact"}}]
      (is (= {:partitions 3 :replication-factor 2 :retention-ms 86400000
              :configs {:cleanup.policy "compact" :segment.bytes "1073741824"}}
             (expansion/materialize-topic-configuration cluster-default definition-config nil))))))

;; --- compute-expansion-plan ---

(deftest compute-expansion-plan-test
  (let [cluster-id-1 (UUID/randomUUID)
        cluster-id-2 (UUID/randomUUID)
        definition-id (UUID/randomUUID)
        definition {:id definition-id :labels {} :topic-configuration-id (UUID/randomUUID)}]

    (testing "new claims for eligible clusters with no existing claims"
      (let [clusters [{:id cluster-id-1 :labels {}} {:id cluster-id-2 :labels {}}]
            plan (expansion/compute-expansion-plan definition clusters [] {})]
        (is (= 2 (count (:claims-to-upsert plan))))
        (is (= 2 (count (:revisions-to-create plan))))
        (is (= "Expanded" (:expansion-status plan)))))

    (testing "skips cluster with existing non-deleted claim and non-terminal revision (stale guard)"
      (let [claim-id (UUID/randomUUID)
            existing-claims [{:id claim-id :cluster-id cluster-id-1 :status "Pending"}]
            revision-statuses {claim-id "PendingReconciliation"}
            plan (expansion/compute-expansion-plan definition [{:id cluster-id-1 :labels {}}]
                                                   existing-claims revision-statuses)]
        (is (= 0 (count (:claims-to-upsert plan))))
        (is (= 0 (count (:revisions-to-create plan))))
        (is (= "PendingExpansion" (:expansion-status plan)))))

    (testing "creates revision for existing claim with terminal revision status"
      (let [claim-id (UUID/randomUUID)
            existing-claims [{:id claim-id :cluster-id cluster-id-1 :status "Ready"}]
            revision-statuses {claim-id "Created"}
            plan (expansion/compute-expansion-plan definition [{:id cluster-id-1 :labels {}}]
                                                   existing-claims revision-statuses)]
        (is (= 0 (count (:claims-to-upsert plan))))
        (is (= 1 (count (:revisions-to-create plan))))
        (is (= claim-id (:existing-claim-id (first (:revisions-to-create plan)))))))

    (testing "reactivates deleted claim"
      (let [claim-id (UUID/randomUUID)
            existing-claims [{:id claim-id :cluster-id cluster-id-1 :status "Deleted"}]
            plan (expansion/compute-expansion-plan definition [{:id cluster-id-1 :labels {}}]
                                                   existing-claims {})]
        (is (= 1 (count (:claims-to-upsert plan))))
        (is (= 1 (count (:revisions-to-create plan))))))

    (testing "drains clusters that are draining with existing claims"
      (let [claim-id (UUID/randomUUID)
            draining-cluster {:id cluster-id-1 :labels {:franz.taint "decommission:drain"}}
            existing-claims [{:id claim-id :cluster-id cluster-id-1 :status "Ready"}]
            revision-statuses {claim-id "Created"}
            plan (expansion/compute-expansion-plan definition [draining-cluster]
                                                   existing-claims revision-statuses)]
        (is (= 1 (count (:claims-to-drain plan))))
        (is (= 1 (count (:drain-revisions plan))))
        (is (= "Expanded" (:expansion-status plan)))))

    (testing "skips drain if latest revision is already PendingDelete"
      (let [claim-id (UUID/randomUUID)
            draining-cluster {:id cluster-id-1 :labels {:franz.taint "decommission:drain"}}
            existing-claims [{:id claim-id :cluster-id cluster-id-1 :status "Ready"}]
            revision-statuses {claim-id "PendingDelete"}
            plan (expansion/compute-expansion-plan definition [draining-cluster]
                                                   existing-claims revision-statuses)]
        (is (= 0 (count (:claims-to-drain plan))))
        (is (= 0 (count (:drain-revisions plan))))))

    (testing "expansion-status is PendingExpansion when shard-size not met with selectors"
      (let [definition-with-selectors {:id definition-id
                                       :labels {:franz.affinity/selector "env=production"
                                                :franz.affinity/shard-size "3"}
                                       :topic-configuration-id (UUID/randomUUID)}
            matching-cluster {:id cluster-id-1 :labels {:env "production"}}
            plan (expansion/compute-expansion-plan definition-with-selectors
                                                   [matching-cluster] [] {})]
        (is (= "PendingExpansion" (:expansion-status plan)))))))

;; --- compute-deletion-plan ---

(deftest compute-deletion-plan-test
  (let [claim-id-1 (UUID/randomUUID)
        claim-id-2 (UUID/randomUUID)
        cluster-id-1 (UUID/randomUUID)
        cluster-id-2 (UUID/randomUUID)]

    (testing "creates drain revisions for all active claims"
      (let [existing-claims [{:id claim-id-1 :cluster-id cluster-id-1 :status "Ready"}
                             {:id claim-id-2 :cluster-id cluster-id-2 :status "Pending"}]
            revision-statuses {claim-id-1 "Created" claim-id-2 nil}
            plan (expansion/compute-deletion-plan existing-claims revision-statuses)]
        (is (= 2 (count (:drain-revisions plan))))
        (is (= 2 (count (:claims-to-delete plan))))))

    (testing "skips claims already Deleted"
      (let [existing-claims [{:id claim-id-1 :cluster-id cluster-id-1 :status "Deleted"}]
            plan (expansion/compute-deletion-plan existing-claims {})]
        (is (= 0 (count (:drain-revisions plan))))
        (is (= 0 (count (:claims-to-delete plan))))))

    (testing "skips claims with PendingDelete latest revision"
      (let [existing-claims [{:id claim-id-1 :cluster-id cluster-id-1 :status "Ready"}]
            revision-statuses {claim-id-1 "PendingDelete"}
            plan (expansion/compute-deletion-plan existing-claims revision-statuses)]
        (is (= 0 (count (:drain-revisions plan))))
        (is (= 0 (count (:claims-to-delete plan))))))

    (testing "returns empty plan for empty claims"
      (let [plan (expansion/compute-deletion-plan [] {})]
        (is (= 0 (count (:drain-revisions plan))))
        (is (= 0 (count (:claims-to-delete plan))))))))
