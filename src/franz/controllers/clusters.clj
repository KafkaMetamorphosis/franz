(ns franz.controllers.clusters
  (:require [franz.db.clusters :as db-clusters]
            [franz.logic.cluster :as logic]
            [franz.wire.out :as out]))

(defn create-cluster [ds request]
  (let [body (:body request)
        validation (logic/validate-create body)]
    (if-not (:valid? validation)
      {:status 422 :body {:error "validation failed" :errors (:errors validation)}}
      (if (db-clusters/find-cluster-by-name ds (:name body))
        {:status 409 :body {:error "conflict" :message (str "cluster '" (:name body) "' already exists")}}
        {:status 201 :body (out/cluster->response (db-clusters/insert-cluster! ds {:name (:name body) :bootstrap-url (:bootstrap-url body) :labels (or (:labels body) {})}))}))))

(defn get-cluster [ds request]
  (let [cluster-name (get-in request [:params :cluster-name])]
    (if-let [cluster (db-clusters/find-cluster-by-name ds cluster-name)]
      {:status 200 :body (out/cluster->response cluster)}
      {:status 404 :body {:error "not found"}})))

(defn list-clusters [ds request]
  (let [result (db-clusters/list-clusters ds (:params request))]
    {:status 200 :body (-> result (update :items #(mapv out/cluster->response %)) out/page-response)}))

(defn update-cluster [ds request]
  (let [cluster-name (get-in request [:params :cluster-name])
        body (:body request)
        validation (logic/validate-update body)]
    (if-not (:valid? validation)
      {:status 422 :body {:error "validation failed" :errors (:errors validation)}}
      (if-not (db-clusters/find-cluster-by-name ds cluster-name)
        {:status 404 :body {:error "not found"}}
        {:status 200 :body (out/cluster->response (db-clusters/update-cluster! ds cluster-name body))}))))

(defn delete-cluster [ds request]
  (let [cluster-name (get-in request [:params :cluster-name])]
    (if (db-clusters/find-cluster-by-name ds cluster-name)
      (do (db-clusters/delete-cluster! ds cluster-name)
          {:status 204 :body nil})
      {:status 404 :body {:error "not found"}})))
