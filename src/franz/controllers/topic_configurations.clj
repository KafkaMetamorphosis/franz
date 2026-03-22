(ns franz.controllers.topic-configurations
  (:require [clojure.tools.logging :as log]
            [franz.db.topic-configurations :as db-topic-configurations]
            [franz.logic.topic-configuration :as logic]
            [franz.wire.in :as wire-in]
            [franz.wire.out :as out]))

(defn create-topic-configuration [{:keys [db]} request]
  (let [coercion-result (wire-in/coerce-create-topic-configuration-request (:body request))]
    (if-not (:valid? coercion-result)
      (do (log/info (str "topic-configuration coercion failed errors=" (:errors coercion-result)))
          {:status 422 :body {:error "validation failed" :errors (:errors coercion-result)}})
      (let [body       (:value coercion-result)
            validation (logic/validate-create body)]
        (if-not (:valid? validation)
          (do (log/info (str "topic-configuration validation failed errors=" (:errors validation)))
              {:status 422 :body {:error "validation failed" :errors (:errors validation)}})
          (if (db-topic-configurations/find-topic-configuration-by-name db (:name body))
            (do (log/info (str "topic-configuration conflict name=" (:name body)))
                {:status 409 :body {:error "conflict" :message (str "topic configuration '" (:name body) "' already exists")}})
            (let [topic-config (db-topic-configurations/insert-topic-configuration! db
                                 {:name               (:name body)
                                  :partitions         (:partitions body)
                                  :replication-factor (:replication-factor body)
                                  :retention-ms       (:retention-ms body)
                                  :configs            (or (:configs body) {})
                                  :labels             (or (:labels body) {})})]
              (log/info (str "topic-configuration created name=" (:name body)))
              {:status 201 :body (out/topic-configuration->response topic-config)})))))))

(defn get-topic-configuration [{:keys [db]} request]
  (let [id (get-in request [:params :topic-configuration-id])]
    (if-let [topic-config (db-topic-configurations/find-topic-configuration-by-id db id)]
      {:status 200 :body (out/topic-configuration->response topic-config)}
      (do (log/info (str "topic-configuration not found id=" id))
          {:status 404 :body {:error "not found"}}))))

(defn list-topic-configurations [{:keys [db]} request]
  (let [result (db-topic-configurations/list-topic-configurations db (:params request))]
    {:status 200 :body (-> result (update :items #(mapv out/topic-configuration->response %)) out/page-response)}))

(defn update-topic-configuration [{:keys [db]} request]
  (let [id              (get-in request [:params :topic-configuration-id])
        coercion-result (wire-in/coerce-update-topic-configuration-request (:body request))]
    (if-not (:valid? coercion-result)
      (do (log/info (str "topic-configuration coercion failed errors=" (:errors coercion-result)))
          {:status 422 :body {:error "validation failed" :errors (:errors coercion-result)}})
      (let [body (:value coercion-result)]
        (if-let [existing (db-topic-configurations/find-topic-configuration-by-id db id)]
          (let [validation (logic/validate-update body existing)]
            (if-not (:valid? validation)
              (do (log/info (str "topic-configuration validation failed errors=" (:errors validation)))
                  {:status 422 :body {:error "validation failed" :errors (:errors validation)}})
              (let [topic-config (db-topic-configurations/update-topic-configuration! db id body)]
                (log/info (str "topic-configuration updated id=" id))
                {:status 200 :body (out/topic-configuration->response topic-config)})))
          (do (log/info (str "topic-configuration not found id=" id))
              {:status 404 :body {:error "not found"}}))))))

(defn delete-topic-configuration [{:keys [db]} request]
  (let [id (get-in request [:params :topic-configuration-id])]
    (if (db-topic-configurations/find-topic-configuration-by-id db id)
      (do (db-topic-configurations/delete-topic-configuration! db id)
          (log/info (str "topic-configuration deleted id=" id))
          {:status 204 :body nil})
      (do (log/info (str "topic-configuration not found id=" id))
          {:status 404 :body {:error "not found"}}))))
