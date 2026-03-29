(ns franz.logic.expansion
  (:require [clojure.string :as str]))

(def ^:private non-terminal-revision-statuses
  #{"PendingReconciliation" "PendingDelete"})

(defn parse-taint [cluster-labels]
  (when-let [taint-value (get cluster-labels :franz.taint)]
    (let [parts (str/split (str taint-value) #":")]
      (when (= 2 (count parts))
        {:name   (first parts)
         :effect (keyword (second parts))}))))

(defn parse-tolerations [topic-definition-labels]
  (if-let [toleration-value (get topic-definition-labels :franz.taint/toleration)]
    (->> (str/split (str toleration-value) #",")
         (map str/trim)
         (remove str/blank?)
         set)
    #{}))

(defn taint-allows-placement? [cluster topic-definition]
  (let [taint (parse-taint (:labels cluster))]
    (cond
      (nil? taint)           true
      (= :drain (:effect taint)) false
      (= :no-creation (:effect taint))
      (let [tolerations (parse-tolerations (:labels topic-definition))
            taint-entry (str (:name taint) ":" (name (:effect taint)))]
        (contains? tolerations taint-entry))
      :else false)))

(defn cluster-is-draining? [cluster]
  (let [taint (parse-taint (:labels cluster))]
    (= :drain (:effect taint))))

(defn parse-affinity-selectors [topic-definition-labels]
  (if-let [selector-value (get topic-definition-labels :franz.affinity/selector)]
    (->> (str/split (str/trim (str selector-value)) #",")
         (map str/trim)
         (remove str/blank?)
         (mapv (fn [token]
                 (let [parts (str/split token #"=" 2)]
                   (when (= 2 (count parts))
                     {:key (str/trim (first parts)) :value (str/trim (second parts))}))))
         (filterv some?))
    []))

(defn cluster-matches-selectors? [cluster-labels selectors]
  (if (empty? selectors)
    true
    (every? (fn [{:keys [key value]}]
              (= (get cluster-labels (keyword key)) value))
            selectors)))

(defn parse-shard-size [topic-definition-labels]
  (when-let [shard-value (get topic-definition-labels :franz.affinity/shard-size)]
    (try
      (Integer/parseInt (str shard-value))
      (catch NumberFormatException _ nil))))

(defn cluster-weight [cluster-labels]
  (if-let [weight-value (get cluster-labels :franz.affinity/weight)]
    (try
      (Integer/parseInt (str weight-value))
      (catch NumberFormatException _ 0))
    0))

(defn select-eligible-clusters [all-clusters topic-definition]
  (let [draining-clusters (filterv cluster-is-draining? all-clusters)
        non-draining      (remove cluster-is-draining? all-clusters)
        taint-passing     (filterv #(taint-allows-placement? % topic-definition) non-draining)
        selectors         (parse-affinity-selectors (:labels topic-definition))
        shard-size        (parse-shard-size (:labels topic-definition))
        eligible          (if (seq selectors)
                            (let [matching (filterv #(cluster-matches-selectors? (:labels %) selectors)
                                                    taint-passing)
                                  sorted   (sort-by #(- (cluster-weight (:labels %))) matching)]
                              (if shard-size
                                (vec (take shard-size sorted))
                                (vec sorted)))
                            (vec taint-passing))]
    {:eligible-clusters eligible
     :draining-clusters draining-clusters}))

(defn materialize-topic-configuration [cluster-default-config definition-config claim-override-config]
  (let [layers (remove nil? [cluster-default-config definition-config claim-override-config])
        merged-configs (apply merge (map :configs layers))]
    (-> (apply merge (map #(dissoc % :configs) layers))
        (select-keys [:partitions :replication-factor :retention-ms])
        (assoc :configs (or merged-configs {})))))

(defn compute-expansion-plan [topic-definition all-clusters existing-claims claims-latest-revision-status]
  (let [{:keys [eligible-clusters draining-clusters]} (select-eligible-clusters all-clusters topic-definition)
        existing-claims-by-cluster (into {} (map (fn [claim] [(:cluster-id claim) claim]) existing-claims))
        selectors                  (parse-affinity-selectors (:labels topic-definition))
        shard-size                 (parse-shard-size (:labels topic-definition))
        stale-guard-triggered      (atom false)
        claims-to-upsert          (atom [])
        revisions-to-create       (atom [])
        claims-to-drain           (atom [])
        drain-revisions           (atom [])]

    ;; Process eligible clusters
    (doseq [cluster eligible-clusters]
      (let [existing-claim (get existing-claims-by-cluster (:id cluster))]
        (cond
          ;; No claim or deleted claim -> upsert + create revision
          (or (nil? existing-claim) (= "Deleted" (:status existing-claim)))
          (do
            (swap! claims-to-upsert conj {:cluster-id (:id cluster) :status "Pending" :labels {}})
            (swap! revisions-to-create conj {:cluster-id (:id cluster)
                                             :existing-claim-id (:id existing-claim)
                                             :status "PendingReconciliation"}))

          ;; Existing non-deleted claim with non-terminal revision -> stale guard
          (and existing-claim
               (contains? non-terminal-revision-statuses
                          (get claims-latest-revision-status (:id existing-claim))))
          (reset! stale-guard-triggered true)

          ;; Existing non-deleted claim with terminal or no revision -> create revision only
          :else
          (swap! revisions-to-create conj {:cluster-id (:id cluster)
                                           :existing-claim-id (:id existing-claim)
                                           :status "PendingReconciliation"}))))

    ;; Process draining clusters
    (doseq [draining-cluster draining-clusters]
      (let [existing-claim (get existing-claims-by-cluster (:id draining-cluster))]
        (when (and existing-claim (not= "Deleted" (:status existing-claim)))
          (let [latest-revision-status (get claims-latest-revision-status (:id existing-claim))]
            (when (not= "PendingDelete" latest-revision-status)
              (swap! claims-to-drain conj (:id existing-claim))
              (swap! drain-revisions conj {:claim-id   (:id existing-claim)
                                           :cluster-id (:id draining-cluster)
                                           :status     "PendingDelete"}))))))

    (let [shard-size-not-met (and (seq selectors)
                                  shard-size
                                  (< (count eligible-clusters) shard-size))
          expansion-status   (if (or shard-size-not-met @stale-guard-triggered)
                               "PendingExpansion"
                               "Expanded")]
      {:claims-to-upsert   @claims-to-upsert
       :revisions-to-create @revisions-to-create
       :claims-to-drain    @claims-to-drain
       :drain-revisions    @drain-revisions
       :expansion-status   expansion-status})))

(defn compute-deletion-plan [existing-claims claims-latest-revision-status]
  (let [active-claims (remove #(= "Deleted" (:status %)) existing-claims)
        non-pending-delete (remove #(= "PendingDelete"
                                       (get claims-latest-revision-status (:id %)))
                                   active-claims)]
    {:drain-revisions (mapv (fn [claim]
                              {:claim-id   (:id claim)
                               :cluster-id (:cluster-id claim)
                               :status     "PendingDelete"})
                            non-pending-delete)
     :claims-to-delete (mapv :id non-pending-delete)}))
