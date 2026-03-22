(ns franz.controllers.clusters
  (:require [franz.db.clusters :as db-clusters]
            [franz.logic.cluster :as logic]
            [franz.wire.out :as out]))

(defn create-cluster [db request]
  (let [body (:body request)
        validation (logic/validate-create body)]
    (if-not (:valid? validation)
      {:status 422 :body {:error "validation failed" :errors (:errors validation)}}
      (if (db-clusters/find-cluster-by-name db (:name body))
        {:status 409 :body {:error "conflict" :message (str "cluster '" (:name body) "' already exists")}}
        {:status 201 :body (out/cluster->response (db-clusters/insert-cluster! db {:name (:name body) :bootstrap-url (:bootstrap-url body) :labels (or (:labels body) {})}))}))))

(defn get-cluster [db request]
  (let [cluster-name (get-in request [:params :cluster-name])]
    (if-let [cluster (db-clusters/find-cluster-by-name db cluster-name)]
      {:status 200 :body (out/cluster->response cluster)}
      {:status 404 :body {:error "not found"}})))

(defn list-clusters [db request]
  (let [result (db-clusters/list-clusters db (:params request))]
    {:status 200 :body (-> result (update :items #(mapv out/cluster->response %)) out/page-response)}))

(defn update-cluster [db request]
  (let [cluster-name (get-in request [:params :cluster-name])
        body (:body request)
        validation (logic/validate-update body)]
    (if-not (:valid? validation)
      {:status 422 :body {:error "validation failed" :errors (:errors validation)}}
      (if-not (db-clusters/find-cluster-by-name db cluster-name)
        {:status 404 :body {:error "not found"}}
        {:status 200 :body (out/cluster->response (db-clusters/update-cluster! db cluster-name body))}))))

(defn delete-cluster [db request]
  (let [cluster-name (get-in request [:params :cluster-name])]
    (if (db-clusters/find-cluster-by-name db cluster-name)
      (do (db-clusters/delete-cluster! db cluster-name)
          {:status 204 :body nil})
      {:status 404 :body {:error "not found"}})))
