(ns franz.banner
  (:require [franz.controllers.router :as router]))

(def ^:private ascii-art
  "███████╗██████╗  █████╗ ███╗   ██╗███████╗  ██╗  ██╗ █████╗ ███████╗██╗  ██╗ █████╗
██╔════╝██╔══██╗██╔══██╗████╗  ██║╚══███╔╝  ██║ ██╔╝██╔══██╗██╔════╝██║ ██╔╝██╔══██╗
█████╗  ██████╔╝███████║██╔██╗ ██║  ███╔╝   █████╔╝ ███████║█████╗  █████╔╝ ███████║
██╔══╝  ██╔══██╗██╔══██║██║╚██╗██║ ███╔╝    ██╔═██╗ ██╔══██║██╔══╝  ██╔═██╗ ██╔══██║
██║     ██║  ██║██║  ██║██║ ╚████║███████╗  ██║  ██╗██║  ██║██║     ██║  ██╗██║  ██║
╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝╚══════╝  ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝

                          \"He was a tool of the boss, without brains or backbone.\"
                                                               — The Metamorphosis
")

(defn- format-route [[method path]]
  (format "  %-8s %s" method path))

(defn print! []
  (println ascii-art)
  (println "Routes:")
  (doseq [[method path] router/route-manifest]
    (println (format-route [method path])))
  (println))
