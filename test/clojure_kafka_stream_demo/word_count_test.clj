(ns clojure-kafka-stream-demo.word-count-test
  (:use clojure-kafka-stream-demo.core)
  (:require
    [clojure.string :as str]
    [clojure.test :refer :all]
    [jackdaw.streams :as j]
    [jackdaw.test :as jdt :refer [test-machine]]
    [jackdaw.test.commands :as cmd]
    [jackdaw.test.fixtures :refer [topic-fixture]]))

(def topics {:input-topic (topic-config (str "input-topic-" (random-uuid)))
              :output-topic  (topic-config (str "output-topic-" (random-uuid)))})

(defn build-word-count-kafka-stream-topology
  [topics builder]
  (-> (j/kstream builder (:input-topic topics))
      (j/peek println)
      (j/flat-map-values
        (fn [value]
          (str/split (:line value) #" ")))
      (j/group-by
        (fn [[_ value]] value))
      (j/count)
      (j/to-kstream)
      (j/peek println)
      (j/to (:output-topic topics)))
  builder)

(use-fixtures
  :once
  (topic-fixture
    {"bootstrap.servers" (kafka-bootstrap-servers)}
    topics))

(deftest word-count-kafka-stream-test
  (with-open [machine (test-machine
                        (jdt/kafka-transport
                          {"bootstrap.servers" (kafka-bootstrap-servers)
                           "group.id"          (str "test-machine-" (random-uuid))}
                          topics))]

    ; just a simplification for the demo, usually it will be started as part of the test system, via component or integrant
    (with-open [kafka-stream (start-kafka-stream!
                               (partial build-word-count-kafka-stream-topology topics))]
      (let [write-1 (cmd/write! :input-topic {:line "a b c"}
                                {:key       (random-uuid)
                                 :partition 0})
            write-2 (cmd/write! :input-topic {:line "x y z a b"}
                                {:key       (random-uuid)
                                 :partition 0})
            watch   (cmd/watch (fn [journal]
                                 (let [output (->> (get-in journal [:topics :output-topic])
                                                (remove nil?))]
                                   (when (= 8 (count output))
                                     output))

                                 )
                               {:timeout 10000})
            {:keys [results journal]} (jdt/run-test machine [write-1 write-2 watch])
            [_ _ watch-result] results]

        (is (= [{:key "a" :value 1}
                {:key "b" :value 1}
                {:key "c" :value 1}
                {:key "x" :value 1}
                {:key "y" :value 1}
                {:key "z" :value 1}
                {:key "a" :value 2}
                {:key "b" :value 2}]
               (mapv (fn [m]
                       (select-keys m [:key :value]))
                     (get-in watch-result [:result :info]))))))))