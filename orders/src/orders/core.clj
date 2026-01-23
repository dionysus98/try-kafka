(ns orders.core
  (:require [clojure.tools.logging :as log])
  (:gen-class))

(defn greet
  "Callable entry point to the application."
  [data]
  (println (str "Hello, " (or (:name data) "World") "!")))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (loop [n 5]
    (when-not (< n 0)
      (log/info {:at n})
      (recur (dec n))))
  (greet {:name (first args)}))
