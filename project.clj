(defproject clojure-kafka-stream-demo "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :plugins [
            [com.jakemccrary/lein-test-refresh "0.26.0"]
            [lein-ancient "1.0.0"]
            ]

  :dependencies [[org.clojure/clojure "1.12.5"]

                 [com.fasterxml.jackson.core/jackson-annotations "2.22"]

                 [fundingcircle/jackdaw "0.9.12"]
                 [org.apache.kafka/kafka-streams-test-utils "4.3.1"]

                 [org.testcontainers/testcontainers "2.0.5"]
                 [org.testcontainers/kafka "1.21.4"]

                 [org.slf4j/slf4j-api "2.0.18"]
                 [org.slf4j/slf4j-simple "2.0.18"]

                 [aleph "0.9.10"]
                 ]

  :main ^:skip-aot clojure-kafka-stream-demo.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
