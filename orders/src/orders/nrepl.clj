(ns orders.nrepl
  (:require [nrepl.server :refer [start-server stop-server]]))

(defonce !server (atom nil))

(def port 7888)

(defn start! []
  (when-not @!server
    (reset! !server (start-server :port port :bind "127.0.0.1"))))

(defn stop! []
  (when-let [s @!server]
    (stop-server s)
    (reset! !server nil)))

(defn restart! []
  (stop!)
  (start!))
