(ns clojure-kafka-stream-demo.core
  (:require
    [jackdaw.serdes :as js]
    [jackdaw.streams :as j])
  (:import (org.apache.kafka.streams KafkaStreams)
           (org.testcontainers.kafka ConfluentKafkaContainer)
           (org.testcontainers.utility DockerImageName)))

(defn topic-config
  [topic-name]
  {:topic-name         topic-name
   :key-serde          (js/edn-serde)
   :value-serde        (js/edn-serde)
   :partition-count    1
   :replication-factor 1})

(defn kafka-bootstrap-servers
  []
  (let [
        ; ConfluentKafkaContainer (cp-kafka images) replaces the deprecated
        ; org.testcontainers.containers.KafkaContainer; it is KRaft-native by default.
        kafka-test-container (delay (-> (DockerImageName/parse "confluentinc/cp-kafka:7.8.0")
                                              (ConfluentKafkaContainer.)

                                              ; required to reuse the container across local test runs (significantly faster);
                                              ; also set testcontainers.reuse.enable=true in ` ~/.testcontainers.properties`
                                              (.withReuse true)))

        ^ConfluentKafkaContainer c @kafka-test-container

        ]
    (.start c)
    (.getBootstrapServers c)))


(defn start-kafka-stream!
  [topology-fn]
  (let [kafka-config               {"bootstrap.servers"         (kafka-bootstrap-servers)
                                    "application.id"            (str "clojure-kafka-stream-demo-" (random-uuid))
                                    "auto.offset.reset"         "earliest"
                                    "default.key.serde"         "jackdaw.serdes.EdnSerde"
                                    "default.value.serde"       "jackdaw.serdes.EdnSerde"
                                    "cache.max.bytes.buffering" "0"}
        topology                   (topology-fn (j/streams-builder))
        ^KafkaStreams kafka-stream (j/kafka-streams topology kafka-config)]
    (j/start kafka-stream)
    kafka-stream))