(ns dev.user
  (:require
   [clojure.tools.logging :as log]
   [orders.topology.orders-topology :as topology]
   [orders.utils :as utils])
  (:import
   [java.time LocalDateTime]
   [org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerRecord]
   [org.apache.kafka.common.serialization StringSerializer]))

(def order-type #{:general :restaurant})

(defn order-line-item
  [item count amount]
  {:order-line/item   item
   :order-line/count  count
   :order-line/amount amount})

(defn order [{:keys [id location-id amount type items datetime]}]
  (assert (order-type type) "invalid order-type")
  {:order/id           id
   :order/location-id  location-id
   :order/final-amount amount
   :order/type         (order-type type)
   :order/line-items   items
   :order/date-time    (str datetime)})

(def orders-items
  [(order-line-item "Bananas" 2 (bigdec "2.00"))
   (order-line-item "Iphone Charger" 1  (bigdec "25.00"))])

(def restaurant-orders-items
  [(order-line-item "Pizza" 2 (bigdec "12.00"))
   (order-line-item "Coffee" 1  (bigdec "3.00"))])

(defn build-orders []
  [(order {:id          12345
           :location-id "store_1234",
           :amount      (bigdec "27.00"),
           :type        (order-type :general),
           :items       orders-items,
           :datetime    (LocalDateTime/now)})

   (order {:id          54321
           :location-id "store_1234",
           :amount      (bigdec "15.00"),
           :type        (order-type :restaurant),
           :items       restaurant-orders-items,
           :datetime    (LocalDateTime/now)})

   (order {:id          12345
           :location-id "store_4567",
           :amount      (bigdec "27.00"),
           :type        (order-type :general),
           :items       orders-items,
           :datetime    (LocalDateTime/now)})

   (order {:id          12345
           :location-id "store_4567",
           :amount      (bigdec "27.00"),
           :type        (order-type :restaurant),
           :items       orders-items,
           :datetime    (LocalDateTime/now)})

   ;; fail  
   #:order{:id           123451
           :location-id  "store_4567"
           :final-amount 27.00M
           :type         :default
           :line-items   [#:order-line{:item   "Bananas"
                                       :count  2
                                       :amount 2.00M} #:order-line{:item   "Iphone Charger"
                                                                   :count  1
                                                                   :amount 25.00M}]
           :date-time    "2026-01-24T17:54:03.289162150"}])

(def !producer (atom nil))

(def producer-config
  {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"
   ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG
   (.getName StringSerializer)
   ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG
   (.getName StringSerializer)})

(defn producer! []
  (if (instance? KafkaProducer @!producer)
    @!producer
    (KafkaProducer. (utils/make-config producer-config))))


(defn publish-message-sync! [topic k message]
  (let [^ProducerRecord r (ProducerRecord. topic k message)
        ^KafkaProducer producer (producer!)]
    (try
      (log/info "ProducerRecord" r)
      (.send producer r)
      (catch Exception e
        (log/error e "Exception in publish-message-sync!")))))


(defn publish-orders! [orders]
  (doseq [order orders]
    (try
      (->> (pr-str order)
           (publish-message-sync!
            (:orders topology/topics)
            (str (:order/id order)))
           (str "Published order message: ")
           (log/info))
      (catch Exception e
        (log/error e "Expection while publishing order!")
        (throw (RuntimeException. e))))))

(defn publish-bulk-orders! [n]
  (doseq [_ (range n)]
    (publish-orders! (build-orders))
    (Thread/sleep 1000)))


(comment
  (publish-bulk-orders! 10)

  (build-orders)
  :rcf)
