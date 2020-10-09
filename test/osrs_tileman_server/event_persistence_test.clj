(ns osrs-tileman-server.event-persistence-test
  (:require [clojure.test :refer :all]
            [mount.core :as mount]
            [osrs-tileman-server.event-persistence :refer :all]
            [clojure.data.json :as json])
  (:import (java.io File)))

(deftest can-read-events-existing-in-file-at-startup
  (mount/start-with-args {:event-file (let [f (doto (File/createTempFile "current" ".events")
                                                (.deleteOnExit))]
                                        (spit f "{\"a\":1},\n{\"a\":2},\n")
                                        f)
                          :tail-file  (let [f (doto (File/createTempFile "current" ".tail")
                                                (.deleteOnExit))]
                                        (spit f 18)
                                        f)})
  (let [{:keys [new-marker events-json]} (read-events-since-marker 0)]
    (testing "Starts up with marker as saved in file"
      (is (= new-marker 18)))
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
        (is (= (:new-marker read1) 9)))
      (testing "When marker in file is less than size of events file, only events up to the marker are readable"
        (is (= (:events-json read1) "[{\"a\":1}]")))
      (testing "Marker advances from initial value after write(s)"
        (is (= (:new-marker read2) 18)))
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
  (mount/start-with-args {:event-file (doto (File/createTempFile "current" ".events")
                                        (.deleteOnExit))
                          :tail-file  (doto (File/createTempFile "current" ".tail")
                                        (.deleteOnExit))})
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
      (is (not= read2 read3))))
  (mount/stop))

(deftest multiple-simultaneous-writes-do-not-conflict
  (mount/start-with-args {:event-file (doto (File/createTempFile "current" ".events")
                                        (.deleteOnExit))
                          :tail-file  (doto (File/createTempFile "current" ".tail")
                                        (.deleteOnExit))})
  (let [payload1 (generate-slow-lazy-payload "a" 3)
        payload2 (generate-slow-lazy-payload "b" 3)
        _ (future (write-events payload1)) ;1.5 seconds to write
        _ (Thread/sleep 500) ;give enough time for first write to be in-progress
        _ (future (write-events payload2)) ;1.5 seconds to write
        _ (Thread/sleep 3000) ;give enough time for writes to complete
        {:keys [events-json]} (read-events-since-marker 0)]
    (testing "writes submitted during an ongoing write are written in sequence rather than causing race conditions"
      (is (= (vec (concat payload1 payload2))
             (json/read-str events-json)))))
  (mount/stop))

(deftest read-with-markers-outside-acceptable-range
  (mount/start-with-args {:event-file (doto (File/createTempFile "current" ".events")
                                        (.deleteOnExit))
                          :tail-file  (doto (File/createTempFile "current" ".tail")
                                        (.deleteOnExit))})
  (write-events [{:a 1} {:a 2} {:a 3}])
  (testing "exception thrown for negative marker"
    (is (thrown? IllegalArgumentException (read-events-since-marker -1))))
  (testing "exception thrown if marker provided is larger than data that has been written"
    (is (thrown? IllegalArgumentException (read-events-since-marker 28))))
  (mount/stop))