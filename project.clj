(defproject clojure-kafka-stream-demo "0.1.0-SNAPSHOT"

  :plugins [[com.jakemccrary/lein-test-refresh "0.26.0"]
            [lein-ancient "1.0.0"]]

  :dependencies [[org.clojure/clojure "1.12.5"]

                 [fundingcircle/jackdaw "0.9.12"]

                 ; jackdaw.data.producer needs org.apache.kafka.streams.test.TestRecord
                 ; from this jar; pin to jackdaw's Kafka (3.3.2) to avoid a 3.x/4.x skew.
                 [org.apache.kafka/kafka-streams-test-utils "3.3.2"]

                 [org.testcontainers/testcontainers "1.21.4"] ; *** MUST KEEP VERSIONS IN SYNC
                 [org.testcontainers/kafka "1.21.4"]

                 [org.slf4j/slf4j-api "2.0.18"]
                 [org.slf4j/slf4j-simple "2.0.18"]

                 [aleph "0.9.10"]
                 ]

  :target-path "target/%s"

  )
