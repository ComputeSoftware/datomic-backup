(ns computesoftware.datomic-backup.retry
  (:import (clojure.lang ExceptionInfo)))

(defn capped-exponential-backoff
  "Returns a function of the num-retries (so far), which returns the
  lesser of max-backoff and an exponentially increasing multiple of
  base, or nil when (>= num-retries max-retries).
  See with-retry to see how it is used.
  Alpha. Subject to change."
  ([] (capped-exponential-backoff 100 10000 8))
  ([base max-backoff max-retries]
   (fn [num-retries]
     (when (< num-retries max-retries)
       (min max-backoff
         (* base (bit-shift-left 1 num-retries)))))))

(defn capped-exponential-backoff-with-jitter
  ([] (capped-exponential-backoff-with-jitter {}))
  ([{:keys [base
            max-backoff
            max-retries
            max-jitter-ms
            rand-int]
     :or   {base          100
            max-backoff   10000
            max-retries   5
            max-jitter-ms 100
            rand-int      rand-int}}]
   (let [backoff-fn (capped-exponential-backoff
                      base
                      max-backoff
                      max-retries)]
     (fn [num-retries]
       (when-let [backoff-ms (backoff-fn num-retries)]
         ;; adding "jitter" can help reduce throttling on retries:
         ;; https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
         (let [jitter-ms (rand-int max-jitter-ms)]
           (+ jitter-ms (min backoff-ms max-backoff))))))))

(defn default-retriable?
  "Returns true if x is an anomaly or if it is an ExceptionInfo with an anomaly
  in its ex-data."
  [x]
  (or
    (contains? #{:cognitect.anomalies/busy
                 :cognitect.anomalies/unavailable
                 :cognitect.anomalies/interrupted}
      (:cognitect.anomalies/category x))
    (and (instance? ExceptionInfo x)
      (default-retriable? (ex-data x)))))

(defn with-retry
  "Calls work-fn until retriable? is false or backoff returns nil. If work-fn
  throws, the exception will be passed to retriable?. If it is not retriable, the
  exception will be thrown. work-fn is a function of no arguments. retriable? is
  passed the result or exception from calling work-fn. backoff is a function of
  the number of times work-fn has been called."
  ([work-fn] (with-retry work-fn nil))
  ([work-fn {:keys [retriable? backoff]}]
   (let [retriable? (or retriable? default-retriable?)
         backoff (or backoff (capped-exponential-backoff-with-jitter))
         maybe-throw #(if (instance? Throwable %) (throw %) %)]
     (loop [retries 0]
       (let [resp (try (work-fn) (catch Throwable t t))]
         (if (retriable? resp)
           (if-let [bo (backoff retries)]
             (do
               (Thread/sleep bo)
               (recur (inc retries)))
             (maybe-throw resp))
           (maybe-throw resp)))))))
