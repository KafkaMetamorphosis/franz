(ns franz.dev.seed
  (:require [com.stuartsierra.component :as component]
            [franz.db.clusters :as db]
            [franz.system :as system]))

(def ^:private seed-data
  [{:name "prod-cluster"    :bootstrap-url "kafka-prod-1:9092,kafka-prod-2:9092" :labels {:env "prod"    :region "us-east-1"}}
   {:name "staging-cluster" :bootstrap-url "kafka-staging-1:9092"                :labels {:env "staging" :region "us-east-1"}}
   {:name "dev-cluster"     :bootstrap-url "localhost:9092"                      :labels {:env "dev"     :region "local"}}])

(defn seed! [ds]
  (doseq [cluster seed-data]
    (if (db/find-cluster-by-name ds (:name cluster))
      (println "Skipping" (:name cluster) "(already exists)")
      (do (db/insert-cluster! ds cluster)
          (println "Inserted" (:name cluster))))))

(defn -main [& _]
  (let [sys (component/start (system/new-system :test))]
    (try
      (seed! (get-in sys [:database :datasource]))
      (finally
        (component/stop sys)))))
