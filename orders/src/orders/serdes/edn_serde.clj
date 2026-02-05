(ns orders.serdes.edn-serde
  (:require
   [clojure.edn :as edn])
  (:import
   [java.nio.charset StandardCharsets]
   [org.apache.kafka.common.serialization Deserializer Serde Serializer]))

(deftype EdnSerializer []
  Serializer
  (serialize [_this _topic data]
    (-> ^String (pr-str data)
        ;; got this from jackdaw
        (.getBytes StandardCharsets/UTF_8))))

(deftype EdnDeserializer []
  Deserializer
  (deserialize [_this _topic data]
    (-> data
        ;; got this from jackdaw
        (String. StandardCharsets/UTF_8)
        (edn/read-string))))

(deftype EdnSerde []
  Serde
  (serializer [_] (EdnSerializer.))
  (deserializer [_] (EdnDeserializer.)))

(comment
  (let [edn (EdnSerde.)]
    (-> edn
        (.serializer)
        (.serialize "" {:a 3 :as/new 3})
        (->>
         (.deserialize (.deserializer edn) ""))))
  :rcf)

