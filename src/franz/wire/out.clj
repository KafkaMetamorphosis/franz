(ns franz.wire.out)

(defn topic-configuration->response [topic-config]
  {:id                 (str (:id topic-config))
   :name               (:name topic-config)
   :partitions         (:partitions topic-config)
   :replication-factor (:replication-factor topic-config)
   :retention-ms       (:retention-ms topic-config)
   :configs            (:configs topic-config)
   :labels             (:labels topic-config)
   :created-at         (str (:created-at topic-config))
   :updated-at         (str (:updated-at topic-config))})

(defn cluster->response [cluster]
  {:id                          (str (:id cluster))
   :name                        (:name cluster)
   :default-topic-configuration (topic-configuration->response (:default-topic-configuration cluster))
   :bootstrap-url               (:bootstrap-url cluster)
   :labels                      (:labels cluster)
   :created-at                  (str (:created-at cluster))
   :updated-at                  (str (:updated-at cluster))})

(defn page-response [{:keys [items page size total]}]
  {:items items
   :page  page
   :size  size
   :total total})
