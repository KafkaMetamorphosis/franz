(ns franz.ops.handlers
  (:require [clojure.string :as string]
            [clojure.walk :as walk]
            [franz.ops.health :as health]
            [ring.util.response :as response]))

(def ^:private sensitive-key-patterns
  #{"password" "secret" "token" "api-key"})

(defn- sensitive-key? [key-name]
  (let [normalized (string/lower-case (name key-name))]
    (some #(string/includes? normalized %) sensitive-key-patterns)))

(defn- redact-sensitive [config-map]
  (walk/postwalk
    (fn [form]
      (if (map? form)
        (reduce-kv
          (fn [accumulated-map key value]
            (if (and (or (keyword? key) (string? key))
                     (sensitive-key? key))
              (assoc accumulated-map key "***REDACTED***")
              (assoc accumulated-map key value)))
          {}
          form)
        form))
    config-map))

(defn health-handler [{:keys [db]} _request]
  (let [result (health/run-checks db)
        status (if (= :healthy (:status result)) 200 503)]
    (-> (response/response result)
        (response/status status))))

(defn liveness-handler [_components _request]
  (response/response {:status "alive"}))

(defn readiness-handler [_components _request]
  (if (health/ready?)
    (response/response {:status "ready"})
    (-> (response/response {:status "not-ready"})
        (response/status 503))))

(defn config-dump-handler [{:keys [config]} _request]
  (response/response (redact-sensitive config)))
