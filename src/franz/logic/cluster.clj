(ns franz.logic.cluster
  (:require [clojure.string :as str]))

(defn validate-create [{:keys [name bootstrap-url]}]
  (let [errors (cond-> []
                 (str/blank? name)
                 (conj "name must not be empty")

                 (and (not (str/blank? name))
                      (not= name (str/trim name)))
                 (conj "name must not have leading or trailing whitespace")

                 (str/blank? bootstrap-url)
                 (conj "bootstrap-url must not be empty"))]
    (if (seq errors)
      {:valid? false :errors errors}
      {:valid? true})))

(defn validate-update [changes]
  (let [errors (cond-> []
                 (and (contains? changes :bootstrap-url)
                      (str/blank? (:bootstrap-url changes)))
                 (conj "bootstrap-url must not be empty"))]
    (if (seq errors)
      {:valid? false :errors errors}
      {:valid? true})))
