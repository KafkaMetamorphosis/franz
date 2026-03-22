(ns franz.dev.seed
  (:require [com.stuartsierra.component :as component]
            [franz.db.clusters :as db-clusters]
            [franz.db.topic-configurations :as db-topic-configs]
            [franz.system :as system]))

(def ^:private topic-configuration-seed-data
  [{:name               "standard"
    :partitions         4
    :replication-factor 1
    :retention-ms       604800000
    :configs            {"cleanup.policy" "delete"}
    :labels             {:tier "standard"}}])

(defn- seed-topic-configurations! [ds]
  (reduce
    (fn [inserted-by-name topic-config]
      (if (db-topic-configs/find-topic-configuration-by-name ds (:name topic-config))
        (do (println "Skipping topic-configuration" (:name topic-config) "(already exists)")
            (assoc inserted-by-name (:name topic-config)
                   (db-topic-configs/find-topic-configuration-by-name ds (:name topic-config))))
        (let [inserted (db-topic-configs/insert-topic-configuration! ds topic-config)]
          (println "Inserted topic-configuration" (:name topic-config))
          (assoc inserted-by-name (:name topic-config) inserted))))
    {}
    topic-configuration-seed-data))

(defn- seed-clusters! [ds topic-configs-by-name]
  (let [cluster-seed-data
        [{:name                           "dev-cluster"
          :bootstrap-url                  "localhost:9092"
          :default-topic-configuration-id (:id (get topic-configs-by-name "standard"))
          :labels                         {:env "dev" :region "local"}}]]
    (doseq [cluster cluster-seed-data]
      (if (db-clusters/find-cluster-by-name ds (:name cluster))
        (println "Skipping cluster" (:name cluster) "(already exists)")
        (do (db-clusters/insert-cluster! ds cluster)
            (println "Inserted cluster" (:name cluster)))))))

(defn seed! [ds]
  (let [topic-configs-by-name (seed-topic-configurations! ds)]
    (seed-clusters! ds topic-configs-by-name)))

(defn -main [& _]
  (let [sys (component/start (system/new-system :test))]
    (try
      (seed! (get-in sys [:database :datasource]))
      (finally
        (component/stop sys)))))
