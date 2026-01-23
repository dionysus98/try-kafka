(ns orders.serdes.serdes-factory
  (:require [orders.serdes.json-serde :as json-serde]))

(defn json-serdes []
  (json-serde/->JsonSerde))

