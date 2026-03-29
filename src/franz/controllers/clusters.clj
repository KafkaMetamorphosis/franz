(ns franz.controllers.clusters
  (:require [clojure.tools.logging :as log]
            [franz.db.clusters :as db-clusters]
            [franz.db.topic-configurations :as db-topic-configs]
            [franz.logic.cluster :as logic]
            [franz.ops.expansion :as expansion]
            [franz.wire.in :as wire-in]
            [franz.wire.out :as out]))

(defn- validate-topic-configuration-reference [db topic-config-id]
  (when topic-config-id
    (when-not (db-topic-configs/find-topic-configuration-by-id db (str topic-config-id))
      {:status 422
       :body   {:error   "validation failed"
                :errors  ["default-topic-configuration-id references a topic configuration that does not exist"]}})))

(defn create-cluster [{:keys [db]} request]
  (let [coercion-result (wire-in/coerce-create-cluster-request (:body request))]
    (if-not (:valid? coercion-result)
      (do (log/info (str "cluster coercion failed errors=" (:errors coercion-result)))
          {:status 422 :body {:error "validation failed" :errors (:errors coercion-result)}})
      (let [body       (:value coercion-result)
            validation (logic/validate-create body)]
        (if-not (:valid? validation)
          (do (log/info (str "cluster validation failed errors=" (:errors validation)))
              {:status 422 :body {:error "validation failed" :errors (:errors validation)}})
          (or (validate-topic-configuration-reference db (:default-topic-configuration-id body))
              (if (db-clusters/find-cluster-by-name db (:name body))
                (do (log/info (str "cluster conflict name=" (:name body)))
                    {:status 409 :body {:error "conflict" :message (str "cluster '" (:name body) "' already exists")}})
                (let [cluster (db-clusters/insert-cluster! db
                                {:name                           (:name body)
                                 :bootstrap-url                  (:bootstrap-url body)
                                 :default-topic-configuration-id (:default-topic-configuration-id body)
                                 :labels                         (or (:labels body) {})})]
                  (try
                    (expansion/mark-all-active-definitions-pending-expansion! db)
                    (catch Exception exception
                      (log/warn exception "failed to mark definitions pending expansion after cluster create")))
                  (log/info (str "cluster created name=" (:name body)))
                  {:status 201 :body (out/cluster->response cluster)}))))))))

(defn get-cluster [{:keys [db]} request]
  (let [cluster-name (get-in request [:params :cluster-name])]
    (if-let [cluster (db-clusters/find-cluster-by-name db cluster-name)]
      {:status 200 :body (out/cluster->response cluster)}
      (do (log/info (str "cluster not found name=" cluster-name))
          {:status 404 :body {:error "not found"}}))))

(defn list-clusters [{:keys [db]} request]
  (let [result (db-clusters/list-clusters db (:params request))]
    {:status 200 :body (-> result (update :items #(mapv out/cluster->response %)) out/page-response)}))

(defn update-cluster [{:keys [db]} request]
  (let [cluster-name    (get-in request [:params :cluster-name])
        coercion-result (wire-in/coerce-update-cluster-request (:body request))]
    (if-not (:valid? coercion-result)
      (do (log/info (str "cluster coercion failed errors=" (:errors coercion-result)))
          {:status 422 :body {:error "validation failed" :errors (:errors coercion-result)}})
      (let [body       (:value coercion-result)
            validation (logic/validate-update body)]
        (if-not (:valid? validation)
          (do (log/info (str "cluster validation failed errors=" (:errors validation)))
              {:status 422 :body {:error "validation failed" :errors (:errors validation)}})
          (or (when (contains? body :default-topic-configuration-id)
                (validate-topic-configuration-reference db (:default-topic-configuration-id body)))
              (if-not (db-clusters/find-cluster-by-name db cluster-name)
                (do (log/info (str "cluster not found name=" cluster-name))
                    {:status 404 :body {:error "not found"}})
                (let [cluster (db-clusters/update-cluster! db cluster-name body)]
                  (when (contains? body :labels)
                    (try
                      (expansion/mark-all-active-definitions-pending-expansion! db)
                      (catch Exception exception
                        (log/warn exception "failed to mark definitions pending expansion after cluster update"))))
                  (log/info (str "cluster updated name=" cluster-name))
                  {:status 200 :body (out/cluster->response cluster)}))))))))

(defn delete-cluster [{:keys [db]} request]
  (let [cluster-name (get-in request [:params :cluster-name])]
    (if (db-clusters/find-cluster-by-name db cluster-name)
      (do (db-clusters/delete-cluster! db cluster-name)
          (log/info (str "cluster deleted name=" cluster-name))
          {:status 204 :body nil})
      (do (log/info (str "cluster not found name=" cluster-name))
          {:status 404 :body {:error "not found"}}))))
