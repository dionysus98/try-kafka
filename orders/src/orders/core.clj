(ns orders.core
  (:require
   [clojure.tools.logging :as log]
   [orders.topology.orders-topology :as topology]
   [orders.nrepl :as nrepl]
   [orders.utils :as utils])
  (:import
   [org.apache.kafka.clients.consumer ConsumerConfig]
   [org.apache.kafka.streams KafkaStreams StreamsConfig Topology]
   #_[org.apache.kafka.streams.errors LogAndContinueExceptionHandler])
  (:gen-class))

(def ^:const APP_ID "orders-app")

(def config
  {StreamsConfig/APPLICATION_ID_CONFIG     APP_ID
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG  "localhost:9092"
   ;;  StreamsConfig/DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG
   ;;  (.getName LogAndContinueExceptionHandler)
   ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "latest"
   StreamsConfig/NUM_STREAM_THREADS_CONFIG "2"})

(defonce !streams (atom nil))

(defn init! ^KafkaStreams []
  (let [^Topology topo (topology/build-topology!)
        _    (utils/create-topics! config (vals topology/topics))
        ^KafkaStreams streams (KafkaStreams. topo (utils/make-config config))]
    (reset! !streams streams)
    (.start streams)
    streams))

(defn de-init!
  ([^KafkaStreams streams] (.close streams))
  ([] (when (instance? KafkaStreams @!streams) (de-init! @!streams))))

(defn restart! []
  (de-init!)
  (init!))

(defn -main
  "I don't do a whole lot ... yet."
  [& _]
  (utils/add-shutdown-hook! (fn [] (de-init!) (nrepl/stop!)))
  (try
    (nrepl/start!)
    (init!)
    (catch Exception e
      (log/error e "Failed to start Order Streams app :c"))))



(comment
  (-main)
  (de-init!)
  (restart!)

  :rcf)