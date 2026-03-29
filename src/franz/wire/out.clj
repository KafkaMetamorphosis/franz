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

(defn topic-definition->response [topic-definition]
  {:id                     (str (:id topic-definition))
   :topic-name             (:topic-name topic-definition)
   :topic-configuration-id (str (:topic-configuration-id topic-definition))
   :status                 (:status topic-definition)
   :expansion-status       (:expansion-status topic-definition)
   :labels                 (:labels topic-definition)
   :created-at             (str (:created-at topic-definition))
   :updated-at             (str (:updated-at topic-definition))})

(defn page-response [{:keys [items page size total]}]
  {:items items
   :page  page
   :size  size
   :total total})

(defn- revision->response [revision]
  {:id                       (str (:id revision))
   :status                   (:status revision)
   :topic-configuration      (:topic-configuration revision)
   :created-at               (str (:created-at revision))})

(defn- cluster-summary->response [cluster]
  {:name          (:name cluster)
   :bootstrap-url (:bootstrap-url cluster)
   :labels        (:labels cluster)})

(defn- expanded-claim->response [{:keys [claim cluster last-revision topic-definition-name]}]
  {:id                    (str (:id claim))
   :topic-definition-name topic-definition-name
   :cluster               (cluster-summary->response cluster)
   :status                (:status claim)
   :labels                (:labels claim)
   :last-revision         (revision->response last-revision)})

(defn expansion-result->response [{:keys [claims not-expanded]}]
  (cond-> {:claims (map expanded-claim->response claims)}
    (seq not-expanded) (assoc :not-expanded not-expanded)))
