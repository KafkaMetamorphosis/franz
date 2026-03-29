(ns franz.ops.expansion
  (:require [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [franz.db.clusters :as db-clusters]
            [franz.db.topic-claims :as db-topic-claims]
            [franz.db.topic-revisions :as db-topic-revisions]
            [franz.db.topic-definitions :as db-topic-definitions]
            [franz.db.topic-configurations :as db-topic-configs]
            [franz.logic.expansion :as logic]))

(defn- build-revision-status-map [db existing-claims]
  (into {}
        (map (fn [claim]
               (let [latest-revision (db-topic-revisions/find-latest-revision-by-claim-id db (:id claim))]
                 [(:id claim) (:status latest-revision)]))
             existing-claims)))

(defn- load-topic-configuration [db topic-configuration-id]
  (when topic-configuration-id
    (db-topic-configs/find-topic-configuration-by-id db (str topic-configuration-id))))

(defn expand-topic-definition! [db topic-definition]
  (let [all-clusters             (db-clusters/list-all-clusters db)
        existing-claims          (db-topic-claims/find-claims-by-topic-definition-id db (:id topic-definition))
        revision-status-map      (build-revision-status-map db existing-claims)
        plan                     (logic/compute-expansion-plan topic-definition all-clusters
                                                               existing-claims revision-status-map)
        definition-config        (load-topic-configuration db (:topic-configuration-id topic-definition))
        cluster-by-id        (into {} (map (juxt :id identity) all-clusters))
        cluster-config-cache (atom {})
        created-claims       (atom [])]

    (letfn [(cluster-default-config [cluster-id]
              (if-let [cached (get @cluster-config-cache cluster-id)]
                cached
                (let [cluster (get cluster-by-id cluster-id)
                      config  (when cluster
                                (load-topic-configuration db (:default-topic-configuration-id cluster)))]
                  (swap! cluster-config-cache assoc cluster-id config)
                  config)))]

      (jdbc/with-transaction [transaction db]
        ;; Upsert claims and create revisions for new/reactivated claims
        (doseq [claim-plan (:claims-to-upsert plan)]
          (let [upserted-claim  (db-topic-claims/upsert-topic-claim! transaction
                                  {:topic-definition-id (:id topic-definition)
                                   :cluster-id          (:cluster-id claim-plan)
                                   :status              (:status claim-plan)
                                   :labels              (:labels claim-plan)})
                cluster-config  (cluster-default-config (:cluster-id claim-plan))
                materialized    (logic/materialize-topic-configuration cluster-config definition-config nil)
                inserted-revision (db-topic-revisions/insert-topic-revision! transaction
                                    {:topic-claim-id      (:id upserted-claim)
                                     :kafka-cluster-id    (:cluster-id claim-plan)
                                     :topic-configuration materialized
                                     :status              "PendingReconciliation"})]
            (swap! created-claims conj
                   {:claim          upserted-claim
                    :cluster        (get cluster-by-id (:cluster-id claim-plan))
                    :last-revision  inserted-revision
                    :topic-definition-name (:topic-name topic-definition)})))

        ;; Create revisions for existing claims that need updating
        (doseq [revision-plan (:revisions-to-create plan)
                :when (:existing-claim-id revision-plan)]
          (when-not (some #(= (:cluster-id %) (:cluster-id revision-plan)) (:claims-to-upsert plan))
            (let [cluster-config    (cluster-default-config (:cluster-id revision-plan))
                  existing-claim    (some #(when (= (:id %) (:existing-claim-id revision-plan)) %)
                                         existing-claims)
                  override-config   (when (:topic-configuration-override-id existing-claim)
                                      (load-topic-configuration db (:topic-configuration-override-id existing-claim)))
                  materialized      (logic/materialize-topic-configuration cluster-config definition-config override-config)
                  inserted-revision (db-topic-revisions/insert-topic-revision! transaction
                                      {:topic-claim-id      (:existing-claim-id revision-plan)
                                       :kafka-cluster-id    (:cluster-id revision-plan)
                                       :topic-configuration materialized
                                       :status              "PendingReconciliation"})]
              (swap! created-claims conj
                     {:claim          existing-claim
                      :cluster        (get cluster-by-id (:cluster-id revision-plan))
                      :last-revision  inserted-revision
                      :topic-definition-name (:topic-name topic-definition)}))))

        ;; Process drains
        (doseq [drain-revision (:drain-revisions plan)]
          (db-topic-revisions/insert-topic-revision! transaction
            {:topic-claim-id      (:claim-id drain-revision)
             :kafka-cluster-id    (:cluster-id drain-revision)
             :topic-configuration {}
             :status              "PendingDelete"})
          (db-topic-claims/update-claim-status! transaction (:claim-id drain-revision) "Deleted"))

        ;; Update expansion status
        (db-topic-definitions/update-expansion-status! transaction
          (:id topic-definition) (:expansion-status plan))))

    {:claims           @created-claims
     :expansion-status (:expansion-status plan)}))

(defn expand-deleted-definition! [db topic-definition]
  (let [active-claims       (db-topic-claims/find-active-claims-by-topic-definition-id db (:id topic-definition))
        revision-status-map (build-revision-status-map db active-claims)
        plan                (logic/compute-deletion-plan active-claims revision-status-map)]

    (jdbc/with-transaction [transaction db]
      (doseq [drain-revision (:drain-revisions plan)]
        (db-topic-revisions/insert-topic-revision! transaction
          {:topic-claim-id      (:claim-id drain-revision)
           :kafka-cluster-id    (:cluster-id drain-revision)
           :topic-configuration {}
           :status              "PendingDelete"})
        (db-topic-claims/update-claim-status! transaction (:claim-id drain-revision) "Deleted")))

    {:drained (count (:claims-to-delete plan))}))

(defn expand-all-pending! [db]
  (let [pending-definitions (db-topic-definitions/find-definitions-by-expansion-status db "PendingExpansion")
        all-claims          (atom [])
        not-expanded        (atom [])]
    (doseq [definition pending-definitions]
      (try
        (let [result (expand-topic-definition! db definition)]
          (swap! all-claims into (:claims result))
          (when (= "PendingExpansion" (:expansion-status result))
            (swap! not-expanded conj {:topic-name    (:topic-name definition)
                                      :expansion-status "PendingExpansion"
                                      :reason        "stale-revision or shard-size not met"})))
        (catch Exception exception
          (log/error exception (str "expansion failed for topic-definition id=" (:id definition)))
          (swap! not-expanded conj {:topic-name    (:topic-name definition)
                                    :expansion-status "PendingExpansion"
                                    :reason        (.getMessage exception)}))))
    {:claims       @all-claims
     :not-expanded @not-expanded}))

(defn mark-all-active-definitions-pending-expansion! [db]
  (db-topic-definitions/mark-all-active-definitions-pending-expansion! db))
