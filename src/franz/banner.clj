(ns franz.banner)

(def ^:private ascii-art
  "  ___ ___   _   _  _ __
 | __| _ \\ /_\\ | \\| |  /
 | _||   // _ \\| .` | /\\_
 |_| |_|_/_/ \\_\\_|\\_|____|
")

(def ^:private route-manifest
  [["GET"    "/ops/health"]
   ["GET"    "/ops/liveness"]
   ["GET"    "/ops/readiness"]
   ["GET"    "/api/v0/clusters"]
   ["POST"   "/api/v0/clusters"]
   ["GET"    "/api/v0/clusters/:cluster-name"]
   ["PUT"    "/api/v0/clusters/:cluster-name"]
   ["DELETE" "/api/v0/clusters/:cluster-name"]
   ["GET"    "/api/v0/topic_configuration"]
   ["POST"   "/api/v0/topic_configuration"]
   ["GET"    "/api/v0/topic_configuration/:topic-configuration-id"]
   ["PUT"    "/api/v0/topic_configuration/:topic-configuration-id"]
   ["DELETE" "/api/v0/topic_configuration/:topic-configuration-id"]])

(defn- format-route [[method path]]
  (format "  %-8s %s" method path))

(defn print! []
  (println ascii-art)
  (println "Routes:")
  (doseq [route route-manifest]
    (println (format-route route)))
  (println))
