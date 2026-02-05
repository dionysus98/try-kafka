(ns orders.interop
  ;;  probably reify all this!
  (:import
   [java.util.function Consumer]
   [org.apache.kafka.streams.kstream KStream Predicate]))


;; 
(deftype AsPredicate [predicate-fn]
  Predicate
  (test [_ k v]
    (boolean (predicate-fn k v))))

(defn as-predicate
  [predicate-fn]
  (AsPredicate. predicate-fn))

;; 
(deftype AsConsumer [^clojure.lang.IFn f]
  Consumer
  (accept ^KStream [_ arg] (f arg)))

(defn as-consumer [^clojure.lang.IFn f]
  (AsConsumer. f))

