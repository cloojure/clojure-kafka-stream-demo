(ns clojure-kafka-stream-demo.core
  (:require
    [jackdaw.serdes :as js]
    [jackdaw.streams :as j])
  (:import (org.apache.kafka.streams KafkaStreams)))

(defn topic-config
  [topic-name]
  {:topic-name         topic-name
   :key-serde          (js/edn-serde)
   :value-serde        (js/edn-serde)
   :partition-count    1
   :replication-factor 1})

(defn start-kafka-stream!
  "Build and start a Kafka Streams app. `broker-config` supplies the connection
   config (e.g. {\"bootstrap.servers\" ...}); the caller decides where the broker
   comes from, keeping this namespace free of any test/container dependencies."
  [broker-config topology-fn]
  (let [kafka-config               (merge broker-config
                                          {"application.id"            (str "clojure-kafka-stream-demo-" (random-uuid))
                                           "auto.offset.reset"         "earliest"
                                           "default.key.serde"         "jackdaw.serdes.EdnSerde"
                                           "default.value.serde"       "jackdaw.serdes.EdnSerde"
                                           "cache.max.bytes.buffering" "0"})
        topology                   (topology-fn (j/streams-builder))
        ^KafkaStreams kafka-stream (j/kafka-streams topology kafka-config)]
    (j/start kafka-stream)
    kafka-stream))
