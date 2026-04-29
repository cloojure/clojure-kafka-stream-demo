(defproject clojure-kafka-stream-demo "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [fundingcircle/jackdaw "0.9.12"]
                 [aleph/aleph "0.9.5"]

                 [org.apache.kafka/kafka-streams-test-utils "4.1.1"]

                 [org.testcontainers/testcontainers "2.0.3"]
                 [org.testcontainers/kafka "1.21.4"]

                 [org.slf4j/slf4j-api "2.0.17"]
                 [org.slf4j/slf4j-simple "2.0.17"]]
  :main ^:skip-aot clojure-kafka-stream-demo.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]
                      }
            }
  )
