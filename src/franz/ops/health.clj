(ns franz.ops.health
  (:require [next.jdbc :as jdbc]))

(defn check-self []
  {:name   "self"
   :status :healthy})

(defn database-check [ds]
  (try
    (jdbc/execute-one! ds ["SELECT 1"])
    {:name :database :status :ok}
    (catch Exception e
      {:name :database :status :error :message (.getMessage e)})))

(defn run-checks
  ([]
   (run-checks nil))
  ([ds]
   (let [checks       (cond-> [(check-self)]
                        ds (conj (database-check ds)))
         all-healthy? (every? #(#{:healthy :ok} (:status %)) checks)]
     {:status (if all-healthy? :healthy :unhealthy)
      :checks (into {} (map (juxt :name :status) checks))})))

(defn ready?
  ([]
   (ready? nil))
  ([ds]
   (= :healthy (:status (run-checks ds)))))
