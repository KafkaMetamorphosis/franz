(defproject franz "0.1.0-SNAPSHOT"
  :description "Franz - Kafka Fleet Management"
  :url "https://github.com/KafkaMetamorphosis/franz"
  :license {:name "EPL-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}

  :dependencies [[org.clojure/clojure "1.12.0"]
                 [com.stuartsierra/component "1.2.0"]
                 [ring/ring-core "1.12.2"]
                 [ring/ring-jetty-adapter "1.12.2"]
                 [ring/ring-json "0.5.1"]
                 [compojure "1.7.1"]
                 [aero "1.1.6"]
                 ;; ADR-001
                 [prismatic/schema "1.4.1"]
                 [org.apache.kafka/kafka-clients "3.9.0"]
                 [com.github.seancorfield/next.jdbc "1.3.955"]
                 [org.postgresql/postgresql "42.7.4"]

                 [cheshire "5.13.0"]
                 [org.clojure/tools.logging "1.3.0"]
                 [org.slf4j/slf4j-api "2.0.13"]
                 [ch.qos.logback/logback-classic "1.5.6"]]

  :source-paths ["src"]
  :test-paths ["test"]
  :resource-paths ["resources"]
  :target-path "target/%s/"

  :main franz.core

  :jvm-opts ["-Xmx512m"
             "-Dclojure.compiler.direct-linking=true"]

  :profiles
  {:dev     {:dependencies [[nubank/state-flow "5.15.0"]
                            [nubank/matcher-combinators "3.9.1"]
                            [clj-http "3.13.0"]]
             :source-paths ["dev"]
             :repl-options {:init-ns user}}

   :test    {:resource-paths ["test-resources"]}

   :uberjar {:aot :all
             :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}}

  :aliases {"test-all" ["do" "clean" ["test"]]})
