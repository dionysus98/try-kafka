(ns orders.serdes.json-serde
  (:require [jsonista.core :as json])
  (:import [org.apache.kafka.common.serialization Serializer Deserializer Serde]))

(deftype JsonSerializer []
  Serializer
  (serialize [_this _topic data]
    (json/write-value-as-bytes data json/default-object-mapper)))

(deftype JsonDeserializer []
  Deserializer
  (deserialize [_this _topic data]
    (json/read-value data json/default-object-mapper)))

(deftype JsonSerde []
  Serde
  (serializer [_] (JsonSerializer.))
  (deserializer [_] (JsonDeserializer.)))
