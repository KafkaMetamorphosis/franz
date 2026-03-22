(ns franz.controllers.topic-configurations
  (:require [franz.db.topic-configurations :as db-topic-configurations]
            [franz.logic.topic-configuration :as logic]
            [franz.wire.out :as out]))

(defn create-topic-configuration [db request]
  (let [body (:body request)
        validation (logic/validate-create body)]
    (if-not (:valid? validation)
      {:status 422 :body {:error "validation failed" :errors (:errors validation)}}
      (if (db-topic-configurations/find-topic-configuration-by-name db (:name body))
        {:status 409 :body {:error "conflict" :message (str "topic configuration '" (:name body) "' already exists")}}
        {:status 201 :body (out/topic-configuration->response
                             (db-topic-configurations/insert-topic-configuration! db
                               {:name               (:name body)
                                :partitions         (:partitions body)
                                :replication-factor (:replication-factor body)
                                :retention-ms       (:retention-ms body)
                                :configs            (or (:configs body) {})
                                :labels             (or (:labels body) {})}))}))))

(defn get-topic-configuration [db request]
  (let [id (get-in request [:params :topic-configuration-id])]
    (if-let [topic-config (db-topic-configurations/find-topic-configuration-by-id db id)]
      {:status 200 :body (out/topic-configuration->response topic-config)}
      {:status 404 :body {:error "not found"}})))

(defn list-topic-configurations [db request]
  (let [result (db-topic-configurations/list-topic-configurations db (:params request))]
    {:status 200 :body (-> result (update :items #(mapv out/topic-configuration->response %)) out/page-response)}))

(defn update-topic-configuration [db request]
  (let [id   (get-in request [:params :topic-configuration-id])
        body (:body request)]
    (if-let [existing (db-topic-configurations/find-topic-configuration-by-id db id)]
      (let [validation (logic/validate-update body existing)]
        (if-not (:valid? validation)
          {:status 422 :body {:error "validation failed" :errors (:errors validation)}}
          {:status 200 :body (out/topic-configuration->response
                               (db-topic-configurations/update-topic-configuration! db id body))}))
      {:status 404 :body {:error "not found"}})))

(defn delete-topic-configuration [db request]
  (let [id (get-in request [:params :topic-configuration-id])]
    (if (db-topic-configurations/find-topic-configuration-by-id db id)
      (do (db-topic-configurations/delete-topic-configuration! db id)
          {:status 204 :body nil})
      {:status 404 :body {:error "not found"}})))
