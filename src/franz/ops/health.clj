(ns franz.ops.health)

(defn check-self []
  {:name   "self"
   :status :healthy})

(defn run-checks
  "Runs all health checks and returns an aggregate result.
   health-checker is the HealthChecker component."
  [_health-checker]
  (let [checks       [(check-self)]
        all-healthy? (every? #(= :healthy (:status %)) checks)]
    {:status (if all-healthy? :healthy :unhealthy)
     :checks (into {} (map (juxt :name :status) checks))}))

(defn ready?
  "Returns true if the system is ready to accept traffic."
  [health-checker]
  (= :healthy (:status (run-checks health-checker))))
