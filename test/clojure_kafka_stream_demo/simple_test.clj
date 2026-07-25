(ns clojure-kafka-stream-demo.simple-test
  (:use clojure-kafka-stream-demo.core)
  (:require
    [clojure.test :refer :all]
    [jackdaw.streams :as j]
    [jackdaw.test :as jdt :refer [test-machine]]
    [jackdaw.test.commands :as cmd]
    [jackdaw.test.fixtures :refer [topic-fixture]]
    ))

(def topics {:input-topic  (topic-config (str "input-topic-" (random-uuid)))
             :output-topic (topic-config (str "output-topic-" (random-uuid)))})

(use-fixtures
  :once (topic-fixture
          {"bootstrap.servers" (kafka-bootstrap-servers)}
          topics))

(defn build-simple-kafka-stream-topology
  [builder]
  (-> (j/kstream builder (:input-topic topics))
      (j/peek println)
      (j/map (fn [[k v]]
               [k (str "Hello " (:name v))]))
      (j/peek println)
      (j/to (:output-topic topics)))
  builder)

(deftest simple-kafka-stream-test

  (with-open [machine (test-machine
                        (jdt/kafka-transport
                          {"bootstrap.servers" (kafka-bootstrap-servers)
                           "group.id"          "simple-kafka-stream-test-machine"}
                          topics))]

    ; just a simplification for the demo, usually it will be started as part of the test system, via component or integrant
    (with-open [kafka-stream (start-kafka-stream! build-simple-kafka-stream-topology)]
      (let [write (cmd/write! :input-topic {:name "world"}
                              {:partition 0})
            watch (cmd/watch
                    (fn [journal]
                      (->> (get-in journal [:topics :output-topic])
                        (first)))
                    {:timeout 5000})
            {:keys [results]} (jdt/run-test machine [write watch])
            [_ watch-result] results]

        (is (= {:headers   {}
                :key       nil
                :offset    0
                :partition 0
                :topic     :output-topic
                :value     "Hello world"}

               (get-in watch-result [:result :info])))))))

