(ns clojure-kafka-stream-demo.core
  (:gen-class)
  (:require
    [clojure.string :as str]
    [jackdaw.streams :as j])
  (:import (org.apache.kafka.streams KafkaStreams)
           (org.testcontainers.containers KafkaContainer)
           (org.testcontainers.utility DockerImageName)))

(def kafka-test-container
  (-> (DockerImageName/parse "confluentinc/cp-kafka:7.8.0")
      (KafkaContainer.)

      ; we can easily switch from Zookeeper to Kraft
      (.withKraft)

      ; those 2 are required if you want to reuse container for tests locally (significantly faster)
      ; required additional property to be set locally in ` ~/.testcontainers.properties`:
      ; testcontainers.reuse.enable=true
      (.withNetwork nil)
      (.withReuse true)))

(defn kafka-bootstrap-servers
  []
  ; a hacky way just for demo purposes, should be a fixture in tests
  (.start kafka-test-container)
  (.getBootstrapServers kafka-test-container))


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