(ns franz.controllers.topic-definitions
  (:require [clojure.tools.logging :as log]
            [franz.db.topic-definitions :as db-topic-definitions]
            [franz.db.topic-configurations :as db-topic-configs]
            [franz.logic.topic-definition :as logic]
            [franz.wire.in :as wire-in]
            [franz.wire.out :as out]))

(defn- validate-topic-configuration-reference [db topic-config-id]
  (when topic-config-id
    (when-not (db-topic-configs/find-topic-configuration-by-id db (str topic-config-id))
      {:status 422
       :body   {:error  "validation failed"
                :errors ["topic-configuration-id references a topic configuration that does not exist"]}})))

(defn create-topic-definition [{:keys [db]} request]
  (let [coercion-result (wire-in/coerce-create-topic-definition-request (:body request))]
    (if-not (:valid? coercion-result)
      (do (log/info (str "topic-definition coercion failed errors=" (:errors coercion-result)))
          {:status 422 :body {:error "validation failed" :errors (:errors coercion-result)}})
      (let [body       (:value coercion-result)
            validation (logic/validate-create body)]
        (if-not (:valid? validation)
          (do (log/info (str "topic-definition validation failed errors=" (:errors validation)))
              {:status 422 :body {:error "validation failed" :errors (:errors validation)}})
          (or (validate-topic-configuration-reference db (:topic-configuration-id body))
              (if (db-topic-definitions/find-topic-definition-by-name-any-status db (:topic-name body))
                (do (log/info (str "topic-definition conflict topic-name=" (:topic-name body)))
                    {:status 409 :body {:error "conflict" :message (str "topic definition '" (:topic-name body) "' already exists")}})
                (let [topic-definition (db-topic-definitions/insert-topic-definition! db
                                         {:topic-name             (:topic-name body)
                                          :topic-configuration-id (:topic-configuration-id body)
                                          :labels                 (or (:labels body) {})})]
                  (log/info (str "topic-definition created topic-name=" (:topic-name body)))
                  {:status 201 :body (out/topic-definition->response topic-definition)}))))))))

(defn get-topic-definition [{:keys [db]} request]
  (let [topic-name (get-in request [:params :topic-definition-name])]
    (if-let [topic-definition (db-topic-definitions/find-topic-definition-by-name db topic-name)]
      {:status 200 :body (out/topic-definition->response topic-definition)}
      (do (log/info (str "topic-definition not found topic-name=" topic-name))
          {:status 404 :body {:error "not found"}}))))

(defn list-topic-definitions [{:keys [db]} request]
  (let [params (:params request)
        status (:status params)]
    (if (and status (not (logic/valid-status? status)))
      {:status 422 :body {:error "validation failed" :errors [(str "invalid status '" status "'")]}}
      (let [result (db-topic-definitions/list-topic-definitions db params)]
        {:status 200 :body (-> result (update :items #(mapv out/topic-definition->response %)) out/page-response)}))))

(defn update-topic-definition [{:keys [db]} request]
  (let [topic-name      (get-in request [:params :topic-definition-name])
        coercion-result (wire-in/coerce-update-topic-definition-request (:body request))]
    (if-not (:valid? coercion-result)
      (do (log/info (str "topic-definition coercion failed errors=" (:errors coercion-result)))
          {:status 422 :body {:error "validation failed" :errors (:errors coercion-result)}})
      (let [body (:value coercion-result)]
        (if-let [existing (db-topic-definitions/find-topic-definition-by-name db topic-name)]
          (let [validation (logic/validate-update body existing)]
            (if-not (:valid? validation)
              (do (log/info (str "topic-definition validation failed errors=" (:errors validation)))
                  {:status 422 :body {:error "validation failed" :errors (:errors validation)}})
              (or (when (contains? body :topic-configuration-id)
                    (validate-topic-configuration-reference db (:topic-configuration-id body)))
                  (let [topic-definition (db-topic-definitions/update-topic-definition! db topic-name body)]
                    (log/info (str "topic-definition updated topic-name=" topic-name))
                    {:status 200 :body (out/topic-definition->response topic-definition)}))))
          (do (log/info (str "topic-definition not found topic-name=" topic-name))
              {:status 404 :body {:error "not found"}}))))))

(defn delete-topic-definition [{:keys [db]} request]
  (let [topic-name (get-in request [:params :topic-definition-name])]
    (if-let [_deleted (db-topic-definitions/soft-delete-topic-definition! db topic-name)]
      (do (log/info (str "topic-definition deleted topic-name=" topic-name))
          {:status 204 :body nil})
      (do (log/info (str "topic-definition not found topic-name=" topic-name))
          {:status 404 :body {:error "not found"}}))))
