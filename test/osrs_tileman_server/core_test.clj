(ns osrs-tileman-server.core-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer :all]
            [mount.core :as mount]
            [org.httpkit.client :as http]
            [osrs-tileman-server.core :refer [web-server]])
  (:import (java.io File)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Test initialization
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn mount-state [f]
  (mount/start-with-args {:event-file (doto (File/createTempFile "current" ".events")
                                        (.deleteOnExit))
                          :tail-file  (doto (File/createTempFile "current" ".tail")
                                        (.deleteOnExit))
                          :properties {:port 3001 :secret "secret"}})
  (f)
  (mount/stop))

(use-fixtures :each mount-state)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Test utilities
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-all-events-expect-ok []
  (let [{:keys [status body]} @(http/get "http://localhost:3001/event-log/"
                                         {:headers {"authorization" "Basic secret"}})]
    (testing "expect ok"
      (is (= 200 status)))
    (json/read-str body)))

(defn get-events-after-marker-expect-ok [after]
  (let [{:keys [status body]} @(http/get (str "http://localhost:3001/event-log/after/" after)
                                         {:headers {"authorization" "Basic secret"}})]
    (testing "expect ok"
      (is (= 200 status)))
    (json/read-str body)))

(defn get-events-after-marker-expect-bad-request [after-marker]
  (let [{:keys [status body]} @(http/get (str "http://localhost:3001/event-log/after/" after-marker)
                                         {:headers {"authorization" "Basic secret"}})]
    (testing "expect bad request"
      (is (= 400 status)))
    body))

(defn post-events-expect-ok [events]
  (let [{:keys [status]} @(http/post "http://localhost:3001/event-log/"
                                     {:headers {"Content-Type" "application/json"
                                                "authorization" "Basic secret"}
                                      :body (json/write-str events)})]
    (testing "expect ok"
      (is (= 200 status)))))

(defn post-ten-events []
  @(http/post "http://localhost:3001/event-log/"
              {:headers {"Content-Type" "application/json"
                         "authorization" "Basic secret"}
               :body "[{\"a\":1},{\"b\":2},{\"c\":3},{\"d\":4},{\"e\":5},{\"f\":6},{\"g\":7},{\"h\":8},{\"i\":9},{\"j\":10}]"}))

(defn post-events-string-expect-bad-request [event-str]
  (let [{:keys [status]} @(http/post "http://localhost:3001/event-log/"
                                     {:headers {"Content-Type" "application/json"
                                                "authorization" "Basic secret"}
                                      :body event-str})]
    (testing "expect bad request"
      (is (= 400 status)))))

(defn post-events-with-auth-expect-unauthorized [events auth-header]
  (let [{:keys [status]} @(http/post "http://localhost:3001/event-log/"
                                     {:headers {"Content-Type" "application/json"
                                                "authorization" auth-header}
                                      :body (json/write-str events)})]
    (testing "expect unauthorized"
      (is (= 401 status)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Tests begin here
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest get-all-events
  (let [response1 (get-all-events-expect-ok)
        _ (post-events-expect-ok [{"a" 1} {"a" 2}])
        response2 (get-all-events-expect-ok)
        _ (post-events-expect-ok [{"a" 3}])
        response3 (get-all-events-expect-ok)
        response4 (get-all-events-expect-ok)]
    (testing "new marker should be zero when stream empty"
      (is (= 0 (get response1 "new-marker"))))
    (testing "new markers should monotonically increase as events are posted"
      (is (< (get response1 "new-marker") (get response2 "new-marker") (get response3 "new-marker"))))
    (testing "get results should be identical if no posts have been performed in-between"
      (is (= response3 response4)))
    (testing "empty array returned when getting an empty event log"
      (is (= [] (get response1 "events"))))
    (testing "after first post, read from 0 will return events from that post"
      (is (= [{"a" 1} {"a" 2}] (get response2 "events"))))
    (testing "after more posts, read from 0 will return array of events from all posts"
      (is (= [{"a" 1} {"a" 2} {"a" 3}] (get response3 "events"))))))

(deftest chained-get-events
  (let [response1 (get-all-events-expect-ok)
        _ (post-events-expect-ok [{"a" 1}])
        _ (post-events-expect-ok [{"a" 2}])
        response2 (get-events-after-marker-expect-ok (get response1 "new-marker"))
        _ (post-events-expect-ok [{"a" 3}])
        response3 (get-events-after-marker-expect-ok (get response2 "new-marker"))
        response4 (get-events-after-marker-expect-ok (get response3 "new-marker"))]
    (testing "empty array returned when event log is empty"
      (is (= [] (get response1 "events"))))
    (testing "after consecutive posts, next get should return the sum of all posted events so far"
      (is (= [{"a" 1} {"a" 2}] (get response2 "events"))))
    (testing "after more posts, following get should return only the events posted since the marker provided by the last get"
      (is (= [{"a" 3}] (get response3 "events"))))
    (testing "when no posts have occurred since the last get, an empty array should be returned"
      (is (= [] (get response4 "events"))))))

(deftest get-events-with-marker-outside-acceptable-range
  (post-events-expect-ok [{"a" 1} {"a" 2} {"a" 3}])
  (let [response1 (get-events-after-marker-expect-ok 0)
        _ (get-events-after-marker-expect-bad-request -1)
        _ (get-events-after-marker-expect-bad-request 28)
        response2 (get-events-after-marker-expect-ok 0)]
    (testing "get events responses should be unchanged by invalid requests"
      (is (= response1 response2)))))

(deftest zero-event-post-acceptable
  (let [response1 (get-all-events-expect-ok)
        _ (post-events-expect-ok [])
        response2 (get-all-events-expect-ok)]
    (testing "get results should remain unchanged after a zero-event post"
      (is (= response1 response2)))))

(deftest requests-with-invalid-auth
  (post-events-expect-ok [{"a" 1}])
  (post-events-with-auth-expect-unauthorized [{"a" 2}] "Basic invalid") ;right auth scheme, wrong secret
  (post-events-with-auth-expect-unauthorized [{"a" 3}] "Bearer secret") ;right secret, wrong auth scheme
  (post-events-with-auth-expect-unauthorized [{"a" 4}] nil) ;no auth
  (post-events-expect-ok [{"a" 5}])
  (let [response (get-all-events-expect-ok)]
    (testing "get results should be unaffected by posts with invalid auth"
      (= (get response "events")
         [{"a" 1} {"a" 5}]))))

(deftest simple-load-test
  (let [start-time (System/currentTimeMillis)
        _ (doseq [i (range 1000)] (post-ten-events))
        elapsed-ms (- (System/currentTimeMillis) start-time)]
    (testing "server be able to post events reasonably quickly"
      (is (< elapsed-ms 3000)))
    (testing "server should have the correct number of events"
      (is (= 10000 (count (get (get-all-events-expect-ok) "events")))))))

(deftest post-with-invalid-payloads
  (let [_ (post-events-expect-ok [{"a" 1}])
        response1 (get-all-events-expect-ok)
        _ (post-events-string-expect-bad-request "[{\"a\":") ;malformed json
        response2 (get-all-events-expect-ok)
        _ (post-events-string-expect-bad-request "{\"a\":1}") ;valid json, but not an array of maps
        response3 (get-all-events-expect-ok)]
    (testing "events should be unchanged by post with malformed json"
      (is (= response1 response2)))
    (testing "events should be unchanged by post with invalid schema"
      (is (= response2 response3)))))