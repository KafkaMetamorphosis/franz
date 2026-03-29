(ns franz.controllers.expansion
  (:require [clojure.tools.logging :as log]
            [franz.ops.expansion :as expansion]
            [franz.wire.out :as out]))

(defn trigger-expansion [{:keys [db]} _request]
  (log/info "triggering expansion for all pending topic definitions")
  (let [result (expansion/expand-all-pending! db)]
    (log/info (str "expansion complete claims-count=" (count (:claims result))
                   " not-expanded-count=" (count (:not-expanded result))))
    {:status 200 :body (out/expansion-result->response result)}))
