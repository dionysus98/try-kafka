(ns orders.topology.orders-topology
  (:require
   [clojure.tools.logging :as log]
   [orders.interop :as interop]
   [orders.serdes.serdes-factory :as serdes-factory])
  (:import
   [org.apache.kafka.common.serialization Serdes]
   [org.apache.kafka.streams StreamsBuilder]
   [org.apache.kafka.streams.kstream
    Branched
    BranchedKStream
    Consumed
    KStream
    Named
    Predicate
    Produced]))

(def topics
  {:orders            "orders"
   :restaurant-orders "restaurant_orders"
   :general-orders    "general_orders"})

(def ^Consumed default-consumed-serde (Consumed/with (Serdes/String) (serdes-factory/edn-serdes)))
(def ^Produced default-produced-serde (Produced/with (Serdes/String) (serdes-factory/edn-serdes)))

(defn order->revenue [{:order/keys [final-amount location-id]}]
  {:revenue/location-id location-id
   :revenue/final-amount final-amount})

(def ^Predicate general-order-predicate
  (interop/as-predicate
   (fn [_ order]
     (= (:order/type order) :general))))

(def ^Branched general-order-branched
  (Branched/withConsumer
   (interop/as-consumer
    (fn [^KStream ks]
      (-> ks
          (.peek ^KStream (fn [k v] (log/info {:dev.general-order/peek {k v}})))
          (.mapValues ^KStream (fn [_ v] (order->revenue v)))
          (.to (:general-orders topics) default-produced-serde))))))

(def ^Predicate restaurant-order-predicate
  (interop/as-predicate
   (fn [_ order]
     (= (:order/type order) :restaurant))))

(def ^Branched restaurant-order-branched
  (Branched/withConsumer
   (interop/as-consumer
    (fn [^KStream restaurant-order-stream]
      (println {:dev/rest restaurant-order-stream})
      (-> restaurant-order-stream
          (.peek ^KStream (fn [k v] (log/info {:dev.restaurant-order/peek {k v}})))
          (.mapValues ^KStream (fn [_ v] (order->revenue v)))
          (.to (:restaurant-orders topics) default-produced-serde))))))

(defn build-topology! []
  (let [^StreamsBuilder builder (StreamsBuilder.)]
    (-> builder
        (.stream ^KStream (:orders topics) default-consumed-serde)

        (.peek ^KStream (fn [k v] (log/info {:dev.stream/peek {k v}})))
        (.split ^BranchedKStream (Named/as "General-restaurant-stream"))

        ;; General Orders
        (.branch ^BranchedKStream general-order-predicate general-order-branched)

        ;; Restaurant Orders
        (.branch ^BranchedKStream restaurant-order-predicate restaurant-order-branched)

        ;; :default
        (.defaultBranch
         ;; withConsumer is more suited in this case, probably
         (Branched/withFunction
          (fn [^KStream ks]
            (.peek ks ^KStream (fn [k v] (log/warn "NO predicate matched for order" {:log/order {k v}})))))))
    ;; 
    (.build builder)))
