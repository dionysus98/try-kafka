(ns orders.utils
  (:require
   [clojure.tools.logging :as log])
  (:import
   [org.apache.kafka.clients.admin AdminClient CreateTopicsResult NewTopic]))

(defn map->props ^java.util.Properties [m]
  (doto (java.util.Properties.)
    (.putAll m)))

(defn make-config ^java.util.Properties [m]
  (if (instance? java.util.Properties m)
    m
    (map->props m)))

(defn add-shutdown-hook! [^clojure.lang.IFn sfn]
  (-> (Runtime/getRuntime)
      (.addShutdownHook (Thread. sfn))))

(defn make-topics! [topics partitions replicatons]
  (mapv
   (fn [^String topic]
     (NewTopic. topic (int partitions) (short replicatons)))
   topics))

(defn create-topics!
  ([config topics] (create-topics! config topics {:partitions 3 :replications 1}))
  ([config topics {:keys [partitions replications]}]
   (let [^AdminClient admin (AdminClient/create (make-config config))
         ^CreateTopicsResult res (->> (make-topics! topics partitions replications)
                                      (.createTopics admin))]
     (try
       (-> res (.all) (.get))
       (log/info "Topics created successfully!")
       :ok
       (catch Exception e
         (log/error e "Exception creating topics!"))))))

