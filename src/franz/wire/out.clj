(ns franz.wire.out)

(defn cluster->response [cluster]
  {:id            (str (:id cluster))
   :name          (:name cluster)
   :bootstrap-url (:bootstrap-url cluster)
   :labels        (:labels cluster)
   :created-at    (str (:created-at cluster))
   :updated-at    (str (:updated-at cluster))})

(defn page-response [{:keys [items page size total]}]
  {:items items
   :page  page
   :size  size
   :total total})
