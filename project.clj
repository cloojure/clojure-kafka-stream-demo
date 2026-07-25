(defproject clojure-kafka-stream-demo "0.1.0-SNAPSHOT"

  :plugins [[com.jakemccrary/lein-test-refresh "0.26.0"]
            [lein-ancient "1.0.0"]]

  :dependencies [[org.clojure/clojure "1.12.5"]

                 ; Kafka is capped at 3.x by jackdaw 0.9.12: it implements the
                 ; Transformer API that Kafka 4.0 removed, so 4.x won't compile.
                 ; 3.9.1 is the newest that works -- override jackdaw's 3.3.2 pin
                 ; here and exclude its transitive Kafka so versions stay aligned.
                 ; (kafka-streams-test-utils is required by jackdaw.data.producer
                 ; for org.apache.kafka.streams.test.TestRecord.)
                 [org.apache.kafka/kafka-clients "3.9.1"]
                 [org.apache.kafka/kafka-streams "3.9.1"]
                 [org.apache.kafka/kafka-streams-test-utils "3.9.1"]

                 [fundingcircle/jackdaw "0.9.12"
                  :exclusions [org.apache.kafka/kafka-clients
                               org.apache.kafka/kafka-streams
                               org.apache.kafka/kafka-streams-test-utils]]

                 [org.testcontainers/testcontainers "1.21.4"] ; *** MUST KEEP VERSIONS IN SYNC
                 [org.testcontainers/kafka "1.21.4"]

                 [org.slf4j/slf4j-api "2.0.18"]
                 [org.slf4j/slf4j-simple "2.0.18"]

                 [aleph "0.9.10"]
                 ]

  :target-path "target/%s"

  )
