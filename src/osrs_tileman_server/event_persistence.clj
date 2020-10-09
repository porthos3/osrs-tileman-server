(ns osrs-tileman-server.event-persistence
  (:require [clojure.core.async :refer [chan close! go-loop <!! >!!]]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [mount.core :as mount])
  (:import (java.io File RandomAccessFile)
           (java.time.format DateTimeFormatter)
           (java.time ZonedDateTime)
           (java.nio.charset Charset)))

(def MARKER_OUTSIDE_VALID_RANGE_MESSAGE "Provided a read marker outside of the available range.")

(def write-channel (atom nil))
(def tail-pointer (atom nil))
(declare initialize-event-processing)
(declare start-write-consumer-thread)

(mount/defstate ^{:on-reload :noop} random-access-file-writer
  :start (let [raf (RandomAccessFile. ^File (:event-file (mount/args)) "rw")]
           (.seek raf (.length raf))
           raf)
  :stop (.close random-access-file-writer))

(mount/defstate ^{:on-reload :noop} write-consumer
  :start (do (initialize-event-processing)
             (future (start-write-consumer-thread)))
  :stop (do (swap! write-channel #(when % (close! %)))
            @write-consumer)) ;block waiting for the thread to complete and return

(defn- generate-backup-file-name []
  (.format (ZonedDateTime/now) (DateTimeFormatter/ofPattern "yyyy_MM_dd_AAAAAAAA")))

(defn- backup-files []
  (let [backup-name (generate-backup-file-name)
        event-backup-file (doto (File. (str backup-name ".events"))
                            (.createNewFile))
        tail-backup-file (doto (File. (str backup-name ".tail"))
                           (.createNewFile))]
    (println (str "Backing up events and tail to files named " backup-name " due to possible corruption."))
    (io/copy (:event-file (mount/args)) event-backup-file)
    (io/copy (:tail-file (mount/args)) tail-backup-file)
    (println "Backup complete.")))

(defn- initialize-event-processing []
  (let [{:keys [event-file tail-file]} (mount/args)]
    (.createNewFile ^File event-file)
    (when (or (not (.exists tail-file)) (zero? (.length tail-file)))
      (if (zero? (.length event-file))
        (spit tail-file 0)
        (throw (IllegalStateException. "Tail file is not populated and event file is non-empty. Cannot safely assume tail location."))))
    (reset! tail-pointer (Long/parseLong (slurp tail-file)))
    (let [len (.length event-file)]
      (cond (> @tail-pointer len) (throw (IllegalStateException. "Tail file references a location beyond the end of the event file."))
            (< @tail-pointer len) (backup-files)))
    (.seek random-access-file-writer @tail-pointer))
  (reset! write-channel (chan 10)))

(defn- append-events-to-file [events]
  (doseq [event events]
    (.write random-access-file-writer (.getBytes ^String (str (json/write-str event) ",\n"))))
  (let [len (.getFilePointer random-access-file-writer)]
    (spit (:tail-file (mount/args)) len)
    (reset! tail-pointer len)))

(defn- start-write-consumer-thread []
  (locking write-consumer ;ensure only one write-consumer can run at a time
    (loop []
      (when-some [[events return-promise] (<!! @write-channel)]
        (try (append-events-to-file events)
             (deliver return-promise {:success true})
             (catch Exception e
               (spit (:tail-file (mount/args)) @tail-pointer) ;revert to tail stored in memory to roll back update
               (.setLength random-access-file-writer @tail-pointer) ;truncate event file to old tail-pointer
               (deliver return-promise {:success false :exception e})))
        (recur)))))

(defn- read-bytes-between [a b]
  (let [num-bytes-to-read (max 0 (- b a 2)) ;chop off trailing ",\n"
        arr (byte-array num-bytes-to-read)]
    (doto (RandomAccessFile. ^File (:event-file (mount/args)) "r")
      (.seek a)
      (.read arr))
    (String. arr (Charset/defaultCharset))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Public functions for persistence contract
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn write-events [events]
  (let [result-promise (promise)
        _ (>!! @write-channel [events result-promise])
        {:keys [success exception]} @result-promise]
    (or success (throw exception))))

(defn read-events-since-marker [marker]
  (when-not (<= 0 marker @tail-pointer)
    (throw (IllegalArgumentException. ^String MARKER_OUTSIDE_VALID_RANGE_MESSAGE)))
  (let [new-marker @tail-pointer]
    {:new-marker new-marker
     :events-json (str "[" (read-bytes-between marker new-marker) "]")}))