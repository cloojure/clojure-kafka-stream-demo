(ns clojure-kafka-stream-demo.test-support
  (:require
    [jackdaw.test :as jdt]
    [jackdaw.test.fixtures :refer [topic-fixture]])
  (:import (org.testcontainers.kafka ConfluentKafkaContainer)
           (org.testcontainers.utility DockerImageName)))

(def kafka-test-container
  ; ConfluentKafkaContainer (cp-kafka images) replaces the deprecated
  ; org.testcontainers.containers.KafkaContainer; it is KRaft-native by default.
  ; A single shared (delayed) instance so every caller hits the SAME broker --
  ; without this, each call would build a separate container and the fixture,
  ; test-machine, and stream would end up on different brokers.
  (delay (-> (DockerImageName/parse "confluentinc/cp-kafka:7.8.0")
             (ConfluentKafkaContainer.)

             ; reuse the container across local test runs (significantly faster);
             ; also set testcontainers.reuse.enable=true in ` ~/.testcontainers.properties`
             (.withReuse true))))

(defn kafka-bootstrap-servers
  []
  (let [^ConfluentKafkaContainer c @kafka-test-container]
    (.start c)
    (.getBootstrapServers c)))

(defn broker-config
  "The single source of Kafka connection config; streams, fixtures and
   test transports all build on this."
  []
  {"bootstrap.servers" (kafka-bootstrap-servers)})

(defn kafka-topic-fixture
  "A :once fixture that creates `topics` on the shared test broker."
  [topics]
  (topic-fixture (broker-config) topics))

(defn kafka-test-transport
  "A jackdaw test-machine transport bound to the shared test broker."
  [topics group-id]
  (jdt/kafka-transport (assoc (broker-config) "group.id" group-id) topics))
