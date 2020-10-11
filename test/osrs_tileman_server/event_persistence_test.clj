(ns osrs-tileman-server.event-persistence-test
  (:require [clojure.test :refer :all]
            [mount.core :as mount]
            [osrs-tileman-server.event-persistence :refer :all]
            [clojure.data.json :as json])
  (:import (java.io File)))

(def TEST_CHUNK_SIZE_KB 1) ;normally defaults to 1MB, but we'll use smaller here to keep tests lightweight

(defn event-with-byte-size [bytes]
  (when (< bytes 9)
    (throw (IllegalArgumentException. "This method does not generate events smaller than 9 bytes")))
  {(apply str (repeat (- bytes 8) "a")) 1}) ;'{"a":1},\n' is 9 bytes in the log file

(defmacro with-temp-files [& body]
  `(try (mount/start-with-args {:event-file (doto (File/createTempFile "current" ".events")
                                              (.deleteOnExit))
                                :tail-file  (doto (File/createTempFile "current" ".tail")
                                              (.deleteOnExit))
                                :properties {:chunk-size-in-kb TEST_CHUNK_SIZE_KB}})
        ~@body
        (finally (mount/stop))))

(deftest can-read-events-existing-in-file-at-startup
  (mount/start-with-args {:event-file (let [f (doto (File/createTempFile "current" ".events")
                                                (.deleteOnExit))]
                                        (spit f "{\"a\":1},\n{\"a\":2},\n")
                                        f)
                          :tail-file  (let [f (doto (File/createTempFile "current" ".tail")
                                                (.deleteOnExit))]
                                        (spit f 18)
                                        f)})
  (let [{:keys [next-marker events-json]} (read-events-since-marker 0)]
    (testing "Starts up with marker as saved in file"
      (is (= next-marker 18)))
    (testing "When marker in file is equal to size of events file, all files are readable"
      (is (= events-json "[{\"a\":1},\n{\"a\":2}]"))))
  (mount/stop))

(deftest startup-with-tail-too-large
  (try (mount/start-with-args {:event-file (doto (File/createTempFile "current" ".events")
                                             (.deleteOnExit))
                               :tail-file  (let [f (doto (File/createTempFile "current" ".tail")
                                                     (.deleteOnExit))]
                                             (spit f 100)
                                             f)})
       (assert false "Startup should not succeed when tail is larger than event file size")
       (catch Exception e
         (testing "Mount throws RuntimeException when it fails to start"
           (is (instance? RuntimeException e)))
         (testing "Cause of mount's RuntimeException is an IllegalStateException due to the invalid tail state"
           (is (instance? IllegalStateException (.getCause e))))))
  (mount/stop))

(deftest startup-with-tail-too-small
  (let [event-file-payload "{\"a\":1},\n{\"a\":2},\n{\"a" ;deliberately cut-off mid-write to simulate a mid-write crash
        tail-file-payload "9"]
    ;rebind file naming to be deterministic so they can be inspected and cleaned up
    (with-redefs [osrs-tileman-server.event-persistence/generate-backup-file-name (fn [] "testname")]
      (mount/start-with-args {:event-file (let [f (doto (File/createTempFile "current" ".events")
                                                    (.deleteOnExit))]
                                            (spit f event-file-payload)
                                            f)
                              :tail-file  (let [f (doto (File/createTempFile "current" ".tail")
                                                    (.deleteOnExit))]
                                            (spit f tail-file-payload)
                                            f)}))
    (let [read1 (read-events-since-marker 0)
          _ (write-events [{:b 1}])
          read2 (read-events-since-marker 0)]
      (testing "Starts up with marker as saved in file"
        (is (= (:next-marker read1) 9)))
      (testing "When marker in file is less than size of events file, only events up to the marker are readable"
        (is (= (:events-json read1) "[{\"a\":1}]")))
      (testing "Marker advances from initial value after write(s)"
        (is (= (:next-marker read2) 18)))
      (testing "New events are safely written over the old data after the starting tail pointer"
        (is (= (:events-json read2) "[{\"a\":1},\n{\"b\":1}]"))))
    (let [event-backup-file (File. "testname.events")
          tail-backup-file (File. "testname.tail")]
      (testing "Backup files should exist"
        (is (true? (.exists event-backup-file)))
        (is (true? (.exists tail-backup-file))))
      (testing "Backup file payloads should be identical to the original file payloads"
        (is (= event-file-payload (slurp event-backup-file)))
        (is (= tail-file-payload (slurp tail-backup-file))))
      (.delete event-backup-file)
      (.delete tail-backup-file))
    (mount/stop)))

(deftest new-tail-persisted-to-file-after-write
  (let [tail-file (doto (File/createTempFile "current" ".tail")
                    (.deleteOnExit))]
    (mount/start-with-args {:event-file (doto (File/createTempFile "current" ".events")
                                          (.deleteOnExit))
                            :tail-file tail-file})
    (testing "Empty tail file is initialized to a tail of 0"
      (is (zero? (Long/parseLong (slurp tail-file)))))
    (write-events [{:a 1}])
    (testing "Pointer contained within tail file is advanced after a write"
      (is (pos? (Long/parseLong (slurp tail-file)))))
    (mount/stop)))

(defn generate-slow-lazy-payload [k elements]
  (when (pos? elements)
    (lazy-seq
      (Thread/sleep 500) ;sleep for half a second per element when evaluated
      (cons {k elements}
            (generate-slow-lazy-payload k (dec elements))))))

(deftest read-unaffected-by-in-progress-write
  (with-temp-files
    (let [_ (write-events [{:a 1} {:a 2} {:a 3}])
          read1 (read-events-since-marker 0)
          _ (future (write-events (generate-slow-lazy-payload :i 5))) ;2.5 seconds to write
          _ (Thread/sleep 1000) ;give time to make sure write is underway
          read2 (read-events-since-marker 0)
          _ (Thread/sleep 2000) ;give time for write to complete
          read3 (read-events-since-marker 0)]
      (testing "results of read during an in-progress write should be identical to results of read before it started"
        (is (= read1 read2)))
      (testing "results of read are changed by write once it completes"
        (is (not= read2 read3))))))

(deftest multiple-simultaneous-writes-do-not-conflict
  (with-temp-files
    (let [payload1 (generate-slow-lazy-payload "a" 3)
          payload2 (generate-slow-lazy-payload "b" 3)
          _ (future (write-events payload1)) ;1.5 seconds to write
          _ (Thread/sleep 500) ;give enough time for first write to be in-progress
          _ (future (write-events payload2)) ;1.5 seconds to write
          _ (Thread/sleep 3000) ;give enough time for writes to complete
          {:keys [events-json]} (read-events-since-marker 0)]
      (testing "writes submitted during an ongoing write are written in sequence rather than causing race conditions"
        (is (= (vec (concat payload1 payload2))
               (json/read-str events-json)))))))

(deftest read-with-markers-outside-acceptable-range
  (with-temp-files
    (write-events [{:a 1} {:a 2} {:a 3}])
    (testing "exception thrown for negative marker"
      (is (thrown? IllegalArgumentException (read-events-since-marker -1))))
    (testing "exception thrown if marker provided is larger than data that has been written"
      (is (thrown? IllegalArgumentException (read-events-since-marker 28))))))

(deftest events-totaling-to-chunk-size-all-returned
  (let [ten-byte-events-in-a-chunk (* 100 TEST_CHUNK_SIZE_KB)]
    (with-temp-files
      (write-events (vec (repeat ten-byte-events-in-a-chunk (event-with-byte-size 10))))
      (let [{:keys [next-marker events-json]} (read-events-since-marker 0)]
        (testing "event bytes total to chunk size, so all events should be returned in one batch"
          (is (= ten-byte-events-in-a-chunk (count (json/read-str events-json)))))
        (testing "after reading all ten-byte events, next event marker should be equal to the size of the chunk"
          (is (= (* 10 ten-byte-events-in-a-chunk) next-marker)))))))

(deftest event-spilling-outside-of-chunk-by-one-byte-omitted
  (let [ten-byte-events-in-a-chunk (* 100 TEST_CHUNK_SIZE_KB)]
    (with-temp-files
      (write-events (conj (vec (repeat (dec ten-byte-events-in-a-chunk) (event-with-byte-size 10)))
                          (event-with-byte-size 11)))
      (let [{:keys [next-marker events-json]} (read-events-since-marker 0)]
        (testing "events add up to one larger than chunk size, so last event should be omitted"
          (is (= (dec ten-byte-events-in-a-chunk) (count (json/read-str events-json)))))
        (testing "after reading ten-byte events on shy of chunk size, next event marker should be chunk size - 10"
          (is (= (- (* 1000 TEST_CHUNK_SIZE_KB) 10) next-marker)))))))

(defn- large-event-test [large-event-size]
  (with-temp-files
    (when (< large-event-size (get-in (mount/args) [:properties :chunk-size-in-kb]))
      (throw (IllegalArgumentException. "These assertions are only true when the event size is >= to the chunk size")))
    (write-events [(event-with-byte-size large-event-size) (event-with-byte-size 10) (event-with-byte-size 10)])
    (let [read1 (read-events-since-marker 0)
          read2 (read-events-since-marker (:next-marker read1))]
      (testing "(if batch is one event, only that event should be returned, even if it ends in a following batch)"
        (is (= 1 (count (json/read-str (:events-json read1))))))
      (testing "(after reading one large event, the next marker should equal the size of the event)"
        (is (= large-event-size (:next-marker read1))))
      (testing "(following events can still be read normally in follow-up requests)"
        (is (= 2 (count (json/read-str (:events-json read2)))))))))

(deftest chunking-works-with-events-exceeding-chunk-size
  (testing "event equal to batch size"
    (large-event-test (* 1000 TEST_CHUNK_SIZE_KB)))
  ;must correctly deal with the two-byte delimiter being separated from its entity, or split across chunk boundaries
  (testing "event one byte greater than batch size"
    (large-event-test (+ 1 (* 1000 TEST_CHUNK_SIZE_KB))))
  (testing "event two bytes greater than batch size"
    (large-event-test (+ 2 (* 1000 TEST_CHUNK_SIZE_KB))))
  (testing "event multiple times chunk size"
    (large-event-test (int (* 2.5 (* 1000 TEST_CHUNK_SIZE_KB))))))