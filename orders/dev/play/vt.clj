(ns dev.play.vt
  (:require [clojure.java.shell :as sh]))


(sh/sh "java" "--version")
