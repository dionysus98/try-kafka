(ns orders.topology.orders-topology
  (:require
   [clojure.tools.logging :as log]
   [orders.interop :as interop]
   [orders.serdes.serdes-factory :as serdes-factory])
  (:import
   [org.apache.kafka.common.serialization Serdes]
   [org.apache.kafka.streams StreamsBuilder KeyValue]
   [org.apache.kafka.streams.kstream
    Branched
    BranchedKStream
    Consumed
    Grouped
    KGroupedStream
    KStream
    KTable
    Materialized
    Named
    Predicate
    Produced
    Printed]))

(def topics
  {:orders                    "orders"
   :restaurant-orders         "restaurant_orders"
   :restaurant-orders-count   "restaurant_orders_count"
   :restaurant-orders-revenue "restaurant_orders_revenue"
   :general-orders            "general_orders"
   :general-orders-count      "general_orders_count"
   :general-orders-revenue    "general_orders_revenue"})

(def ^Consumed default-consumed-serde (Consumed/with (Serdes/String) (serdes-factory/edn-serdes)))
(def ^Produced default-produced-serde (Produced/with (Serdes/String) (serdes-factory/edn-serdes)))

(defn order->revenue [{:order/keys [final-amount location-id]}]
  {:revenue/location-id location-id
   :revenue/final-amount final-amount})

(def ^Predicate general-order-predicate
  (interop/as-predicate
   (fn [_ order]
     (= (:order/type order) :general))))

(def total-revenue-init
  (constantly
   {:location-id         ""
    :running-order-count 0
    :running-revenue     (bigdec 0)}))

(defn total-revenue-aggregator
  [_ v agg]
  (-> agg
      (update :running-order-count inc)
      (update :running-revenue + (:order/final-amount v))))

(defn group-by-location ^KGroupedStream [^KStream ks]
  (-> ks
      (.map ^KStream (fn agg-count-kv-pair [_k {:order/keys [location-id] :as order}] (KeyValue/pair (str location-id) order)))
      ;; dev/note: `selectKey` Pretty much does the same operation as `map` above, in this case.
      ;;       But much more simpler and semantically right.
      ;; (.selectKey ^KStream (fn agg-count-select-key [_k {:order/keys [location-id]}] (str location-id)))
      (.groupByKey ^KGroupedStream (Grouped/with (Serdes/String) (serdes-factory/edn-serdes)))))

(defn print> ^KStream [^KStream ks arg]
  (.print ks arg)
  ks)

(defn aggregate-order-by-count! ^KStream [^KStream ks ^String store]
  (-> ks
      (group-by-location)
      (.count ^KTable (Named/as store) (Materialized/as store))
      (.toStream)
      (print> (.withLabel (Printed/toSysOut) store)))
  ks)

(defn aggregate-order-by-revenue! ^KStream [^KStream ks ^String store]
  (-> ks
      (group-by-location)
      (.aggregate ^KTable
       total-revenue-init
       total-revenue-aggregator
       (-> (Materialized/as store)
           (.withKeySerde (Serdes/String))
           (.withValueSerde (serdes-factory/edn-serdes))))
      (.toStream)
      (print> (.withLabel (Printed/toSysOut) store)))
  ks)

(def ^Branched general-order-branched
  (Branched/withConsumer
   (interop/as-consumer
    (fn [^KStream ks]
      (-> ks
          (.peek ^KStream (fn general-order-peek! [k v] (log/info {:dev.general-order/peek {k v}})))
          #_(.mapValues ^KStream (fn general-order-map-values [v] (order->revenue v)))
          #_(.to (:general-orders topics) default-produced-serde)
          (aggregate-order-by-count! (:general-orders-count topics))
          (aggregate-order-by-revenue! (:general-orders-revenue topics)))))))

(def ^Predicate restaurant-order-predicate
  (interop/as-predicate
   (fn [_ order]
     (= (:order/type order) :restaurant))))

(def ^Branched restaurant-order-branched
  (Branched/withConsumer
   (interop/as-consumer
    (fn [^KStream ks]
      (-> ks
          (.peek ^KStream (fn restaurant-order-peek! [k v] (log/info {:dev.restaurant-order/peek {k v}})))
          #_(.mapValues ^KStream (fn restaurant-order-map-values [v] (order->revenue v)))
          #_(.to (:restaurant-orders topics) default-produced-serde)
          (aggregate-order-by-count! (:restaurant-orders-count topics))
          (aggregate-order-by-revenue! (:restaurant-orders-revenue topics)))))))

(defn build-topology! []
  (let [^StreamsBuilder builder (StreamsBuilder.)]
    (-> builder
        (.stream ^KStream (:orders topics) default-consumed-serde)

        (.peek ^KStream (fn order-topo-peek! [k v] (log/info {:dev.stream/peek {k v}})))
        (.split ^BranchedKStream (Named/as "General-restaurant-stream"))

        ;; General Orders
        (.branch ^BranchedKStream general-order-predicate general-order-branched)

        ;; Restaurant Orders
        (.branch ^BranchedKStream restaurant-order-predicate restaurant-order-branched)

        ;; :default
        (.defaultBranch
         ;; withConsumer is more suited in this case, probably
         (Branched/withFunction
          (fn order-topo-default-branch-peek! [^KStream ks]
            (.peek ks ^KStream (fn [k v] (log/warn "NO predicate matched for order" {:log/order {k v}})))))))
    ;; 
    (.build builder)))
