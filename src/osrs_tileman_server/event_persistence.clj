(ns osrs-tileman-server.event-persistence
  (:require [clojure.core.async :refer [chan close! go-loop <!! >!!]]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [mount.core :as mount])
  (:import (java.io File RandomAccessFile)
           (java.nio.charset Charset)
           (java.time ZonedDateTime)
           (java.time.format DateTimeFormatter)))

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

(defn- read-batch-after-marker [marker]
  (let [raf (doto (RandomAccessFile. ^File (:event-file (mount/args)) "r")
              (.seek marker))]
    (loop [marker marker, str-so-far "", get-index-fn #(.lastIndexOf % "\n")]
      (let [arr (byte-array (min (- @tail-pointer marker)
                                 (* 1000 (get-in (mount/args) [:properties :chunk-size-in-kb] 1000)))) ;default 1MB chunk size
            _ (.read raf arr)
            s (String. ^bytes arr ^Charset (Charset/defaultCharset))
            i (get-index-fn s)]
        (if (neg? i) ;if a delimiter does not exist in the chunk (chunk is one massive event), we must read more chunks
          (recur (+ marker (alength arr)) (str str-so-far s) #(.indexOf % "\n"))
          (let [last-index (dec (+ (count str-so-far) i))] ;decrement to remove the "," that precedes the line break
            {:next-marker (+ marker i 1) ;add one so next read will start after the line break
             :events-json (str "[" (subs (str str-so-far s) 0 last-index) "]")}))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Public functions for persistence contract
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn write-events [events]
  (let [result-promise (promise)
        _ (>!! @write-channel [events result-promise])
        {:keys [success exception]} @result-promise]
    (or success (throw exception))))

(defn read-events-since-marker [marker]
  (cond (= marker @tail-pointer) {:next-marker marker :events-json "[]"}
        (<= 0 marker @tail-pointer) (read-batch-after-marker marker)
        :else (throw (IllegalArgumentException. ^String MARKER_OUTSIDE_VALID_RANGE_MESSAGE))))