(ns orders.serdes.serdes-factory
  (:require
   [orders.serdes.edn-serde :as edn-serde]
   [orders.serdes.json-serde :as json-serde]))

(defn json-serdes []
  (json-serde/->JsonSerde))

(defn edn-serdes []
  (edn-serde/->EdnSerde))

