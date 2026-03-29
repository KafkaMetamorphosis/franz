(ns franz.logic.topic-definition
  (:require [clojure.string :as str]))

(def ^:private valid-api-transitions
  #{["Active" "Paused"]
    ["Paused" "Active"]
    ["Error"  "Paused"]})

(def ^:private all-statuses
  #{"Active" "Paused" "Error" "Deleted"})

(defn valid-api-transition? [current-status requested-status]
  (cond
    (= requested-status "Error")
    {:allowed? false :reason "status cannot be set to Error via the API"}

    (= requested-status "Deleted")
    {:allowed? false :reason "status cannot be set to Deleted via PUT; use DELETE endpoint"}

    (contains? valid-api-transitions [current-status requested-status])
    {:allowed? true}

    :else
    {:allowed? false :reason (str "transition from " current-status " to " requested-status " is not allowed")}))

(defn valid-status? [status]
  (contains? all-statuses status))

(defn validate-create [{:keys [topic-name]}]
  (let [errors (cond-> []
                 (str/blank? topic-name)
                 (conj "topic-name must not be empty")

                 (and (not (str/blank? topic-name))
                      (not= topic-name (str/trim topic-name)))
                 (conj "topic-name must not have leading or trailing whitespace"))]
    (if (seq errors)
      {:valid? false :errors errors}
      {:valid? true})))

(defn validate-update [changes existing]
  (let [errors (cond-> []
                 (and (contains? changes :status)
                      (let [transition (valid-api-transition? (:status existing) (:status changes))]
                        (not (:allowed? transition))))
                 (conj (:reason (valid-api-transition? (:status existing) (:status changes)))))]
    (if (seq errors)
      {:valid? false :errors errors}
      {:valid? true})))
