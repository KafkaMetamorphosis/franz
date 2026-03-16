(ns franz.logic.pagination)

(def default-page 1)
(def default-size 20)

(defn parse-params [{:keys [page size]}]
  {:page (if page (Integer/parseInt (str page)) default-page)
   :size (if size (Integer/parseInt (str size)) default-size)})

(defn ->offset [{:keys [page size]}]
  (* (dec page) size))

(defn page-response [items {:keys [page size]} total]
  {:items items
   :page  page
   :size  size
   :total total})
