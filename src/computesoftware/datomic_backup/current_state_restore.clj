(ns computesoftware.datomic-backup.current-state-restore
  (:require
    [computesoftware.datomic-backup.impl :as impl]
    [datomic.client.api :as d]
    [datomic.client.api.async :as d.a]
    [clojure.set :as sets]
    [clojure.walk :as walk]
    [clojure.tools.logging :as log]
    [computesoftware.datomic-backup.retry :as retry]
    [clojure.core.async :as async])
  (:import (clojure.lang ExceptionInfo)
           (java.util.concurrent Executors)))

(defn resolve-datom
  [datom schema old-id->new-id]
  (let [[e a v] datom
        attr-schema (get schema a)
        get-value-type #(get-in schema [% :db/valueType])
        e-id (or (get old-id->new-id e) (str e))
        resolve-v (fn [value-type v]
                    (if (= value-type :db.type/ref)
                      (or (get old-id->new-id v) (str v))
                      v))
        value-type (:db/valueType attr-schema)
        [v-id req-eids] (cond
                          (= value-type :db.type/ref)
                          [(resolve-v (:db/valueType attr-schema) v) #{v}]
                          (= value-type :db.type/tuple)
                          (let [types (or
                                        (seq (map get-value-type (:db/tupleAttrs attr-schema)))
                                        (:db/tupleTypes attr-schema)
                                        (repeat (:db/tupleType attr-schema)))
                                eids (into #{}
                                       (comp
                                         (filter (fn [[t]] (= t :db.type/ref)))
                                         (map (fn [[_ v]] v)))
                                       (map vector types v))]
                            [(mapv (fn [type v] (resolve-v type v)) types v)
                             eids]))
        tx [:db/add e-id (get attr-schema :db/ident) (or v-id v)]]
    {:tx            tx
     ;; set of eids required for this datom to get transacted
     :required-eids (or req-eids #{})
     :resolved-e-id e-id}))

(comment
  (sc.api/defsc 41)
  [92358976733272 86 92358976733273 13194139533319 true]
  )

;; tx1 [1 :many-ref 2] (should result in empty tx b/c 2 does not exist yet)
;; tx2 [2 :name "a] (should know about the relation between [1 :many 2] and add it in
;; tx3 [2 :many-ref 3]

;; can transact a datom if:
;;    1) eid exists in old-id->new-id
;;    2) eid will be transacted in this transaction. only true if

(defn txify-datoms
  [datoms pending schema old-id->new-id]
  (let [;; an attempt to add a tuple or ref value pointing to an eid NOT in this
        ;; set should be attempted later
        eids-exposed (into (set (keys old-id->new-id))
                       (comp
                         (remove (fn [[_ a]]
                                   (contains? #{:db.type/tuple
                                                :db.type/ref}
                                     (get-in schema [a :db/valueType]))))
                         (map :e))
                       datoms)
        datoms-and-pending (concat datoms (map :datom pending))]
    (reduce (fn [acc [e a v :as datom]]
              (if (= :db/txInstant (get-in schema [a :db/ident]))
                ;; not actually used, only collected for reporting purposes
                (update acc :tx-eids (fnil conj #{}) e)
                (let [{:keys [tx
                              required-eids
                              resolved-e-id]
                       :as   resolved} (resolve-datom datom schema old-id->new-id)
                      can-include? (sets/subset? required-eids eids-exposed)]
                  (cond-> acc
                    can-include?
                    (update :tx-data (fnil conj []) tx)
                    (not can-include?)
                    (update :pending (fnil conj []) (assoc resolved :datom datom))
                    (string? resolved-e-id)
                    (assoc-in [:old-id->tempid e] resolved-e-id)))))
      {} datoms-and-pending)))

(defn transact-with-max-batch-size
  [conn tx-argm max-batch-size]
  (let [batches (partition-all max-batch-size (:tx-data tx-argm))]
    (reduce
      (fn [tx-ret tx-data]
        (let [{:keys [db-before
                      db-after
                      tx-data
                      tempids]} (d/transact conn (assoc tx-argm :tx-data tx-data))]
          (cond-> (-> tx-ret
                    (assoc :db-after db-after)
                    (update :tx-data #(apply conj % tx-data))
                    (update :tempids merge tempids))
            (nil? db-before)
            (assoc :db-before db-before))))
      {} batches)))

(defn copy-datoms
  [{:keys [dest-conn datom-batches source-schema debug]}]
  (reduce
    (fn [{:keys [old-id->new-id] :as acc} batch]
      (let [{:keys [old-id->tempid
                    tx-data
                    tx-eids
                    pending]}
            (txify-datoms batch (:pending acc) source-schema old-id->new-id)]
        (assoc
          (if (seq tx-data)
            (let [{:keys [tempids
                          tx-data]} (try
                                      #_(transact-with-max-batch-size dest-conn {:tx-data tx-data} max-batch-size)
                                      (retry/with-retry #(d/transact dest-conn {:tx-data tx-data})
                                        {:backoff (retry/capped-exponential-backoff-with-jitter
                                                    {:max-retries 20})})
                                      (catch Exception ex
                                        ;(sc.api/spy)
                                        (throw ex)))
                  next-old-id->new-id (into old-id->new-id
                                        (map (fn [[old-id tempid]]
                                               (when-let [eid (get tempids tempid)]
                                                 [old-id eid])))
                                        old-id->tempid)
                  next-acc (-> acc
                             (assoc
                               :old-id->new-id next-old-id->new-id)
                             (update :input-datom-count (fnil + 0) (count batch))
                             (update :tx-count (fnil inc 0))
                             (update :tx-datom-count (fnil + 0) (count tx-data))
                             (update :tx-eids sets/union tx-eids))]
              (when (and debug (zero? (mod (:tx-count next-acc) 10)))
                (log/debug "Batch complete"
                  :tx-datom-count (:tx-datom-count next-acc)
                  :tx-count (:tx-count next-acc)))
              ;(prn 'batch batch)
              ;(prn 'tx-data tx-data)
              ;(prn 'old-id->tempid old-id->tempid)
              ;(prn 'tempids tempids)
              ;(prn 'next-old-id->new-id next-old-id->new-id)
              ;(prn '---)
              next-acc)
            acc)
          :pending pending)))
    {:input-datom-count 0
     :old-id->new-id    {}
     :tx-count          0
     :tx-datom-count    0
     :tx-eids           #{}} datom-batches))

(defn read-datoms-in-parallel-sync
  [source-db {:keys [dest-ch parallelism]}]
  (let [a-eids (d/q
                 {:query '[:find ?a
                           :where
                           [:db.part/db :db.install/attribute ?a]]
                  :limit -1
                  :args  [source-db]})
        in-ch (async/chan)]
    (async/onto-chan!! in-ch a-eids)
    (async/pipeline-blocking parallelism dest-ch
      (comp
        (map first)
        (mapcat (fn [a-eid]
                  (try
                    (d/datoms source-db
                      {:index      :aevt
                       :components [a-eid]
                       :limit      -1})
                    (catch ExceptionInfo ex (ex-data ex))))))
      in-ch)
    dest-ch))

(defn read-datoms-with-retry!
  [db argm dest-ch]
  (let [datoms (d/datoms db argm)
        *offset (volatile! (:offset argm 0))]
    (try
      (doseq [d datoms]
        (async/>!! dest-ch d)
        (vswap! *offset inc))
      (catch ExceptionInfo ex
        (if (retry/default-retriable? ex)
          (do
            (read-datoms-with-retry! db (assoc argm :offset @*offset) dest-ch)
            (log/warn "Retryable anomaly while reading datoms. Retrying from offset..."
              :anomaly (ex-data ex)
              :offset @*offset))
          (throw ex))))))

(defn read-datoms-in-parallel-sync2
  [source-db {:keys [dest-ch parallelism read-chunk]}]
  (let [a-eids (d/q
                 {:query '[:find ?a
                           :where
                           [:db.part/db :db.install/attribute ?a]]
                  :limit -1
                  :args  [source-db]})
        exec (Executors/newFixedThreadPool parallelism)
        done-ch (async/chan)]
    (doseq [[a] a-eids]
      (.submit exec ^Runnable
        (fn []
          (read-datoms-with-retry! source-db
            {:index      :aevt
             :components [a]
             :chunk      read-chunk
             :limit      -1}
            dest-ch)
          (async/>!! done-ch true))))
    (async/go-loop [n 0]
      (async/<! done-ch)
      (if (< (inc n) (count a-eids))
        (recur (inc n))
        (do (async/close! dest-ch) (async/thread (.shutdown exec)))))
    dest-ch))

(defn anom!
  [x]
  ;; check for map? since dev-local will throw when get'ing a field that does
  ;; not exist. Throws java.lang.IllegalArgumentException: No matching clause: :cognitect.anomalies/category
  (if (and (map? x) (:cognitect.anomalies/category x))
    (throw (ex-info (:cognitect.anomalies/message x) x))
    x))

(defn <anom!!
  [ch]
  (-> ch async/<!! anom!))

(defn unchunk
  [ch]
  (async/transduce (halt-when :cognitect.anomalies/category) into [] ch))

(defn read-datoms-in-parallel-async
  [a-source-db {:keys [dest-ch parallelism]}]
  (let [a-eids (-> (d.a/q
                     {:query '[:find ?a
                               :where
                               [:db.part/db :db.install/attribute ?a]]
                      :limit -1
                      :args  [a-source-db]})
                 (unchunk)
                 (<anom!!))
        in-ch (async/chan)]
    (async/onto-chan!! in-ch a-eids)
    (async/pipeline-async parallelism dest-ch
      (fn [[a-eid] result-ch]
        (let [ds-ch (d.a/datoms a-source-db
                      {:index      :aevt
                       :components [a-eid]
                       :chunk      1000
                       :limit      -1})]
          (async/go-loop []
            (if-some [ds (async/<! ds-ch)]
              (if (:cognitect.anomalies/category ds)
                (do (async/>! result-ch ds) (async/close! result-ch))
                (do
                  (doseq [d ds] (async/>! result-ch d))
                  (recur)))
              (async/close! result-ch)))))
      in-ch)
    dest-ch))

(defn ch->seq
  [ch]
  (if-let [v (<anom!! ch)]
    (lazy-seq (cons v (ch->seq ch)))
    nil))

(defn monitored-chan!
  [ch {:keys [runningf channel-name every-ms]
       :or   {every-ms 10000}}]
  (async/thread
    (while (runningf)
      (let [buf (.buf ch)
            cur (.count buf)
            total (.n buf)]
        (log/debug (str channel-name " channel, reporting in...")
          :current-size cur
          :total-size total
          :perc (format "%.3f" (double (* 100 (/ cur total)))))
        (Thread/sleep every-ms))))
  ch)

(defn -full-copy
  [{:keys [source-db
           source-schema
           dest-conn
           max-batch-size
           debug
           init-state
           read-parallelism
           read-chunk]}]
  (let [*running? (atom true)
        datoms (let [ch (cond-> (async/chan 20000)
                          debug
                          (monitored-chan! {:runningf     #(deref *running?)
                                            :channel-name "datoms"}))]
                 (-> source-db
                   (read-datoms-in-parallel-sync2
                     {:parallelism read-parallelism
                      :dest-ch     ch
                      :read-chunk  read-chunk})
                   (ch->seq)))
        batches (let [max-bootstrap-tx (impl/bootstrap-datoms-stop-tx source-db)
                      schema-ids (into #{} (comp (filter (fn [[x]] (number? x))) (map first)) source-schema)]
                  (->> datoms
                    (remove (fn [[e _ _ tx]]
                              (or
                                (contains? schema-ids e)
                                (<= tx max-bootstrap-tx))))
                    (partition-all max-batch-size)))
        *state (atom nil)]
    (try
      (copy-datoms
        (cond-> {:dest-conn        dest-conn
                 :datom-batches    batches
                 :source-schema    source-schema
                 :on-batch-success #(reset! *state %)}
          debug (assoc :debug debug)
          init-state (assoc :init-state init-state)))
      (finally (reset! *running? false)))))

(defn copy-schema!
  [{:keys [source-db dest-conn]}]
  (let [source-schema (impl/q-schema source-db)]
    (retry/with-retry
      #(d/transact dest-conn
         {:tx-data (into []
                     (comp
                       (map (fn [[_ schema]] (walk/postwalk (fn [x] (if (map? x) (dissoc x :db/id) x)) schema)))
                       (distinct))
                     source-schema)}))
    {:source-schema source-schema}))

(defn full-copy
  [argm]
  (let [schema-result (copy-schema! argm)
        copy-argm (merge argm schema-result)]
    (-full-copy copy-argm)))

(comment
  (def testc (d/client {:server-type :dev-local
                        :storage-dir :mem
                        :system      "test"}))
  (d/create-database testc {:db-name "a"})
  (def aconn (d/connect testc {:db-name "a"}))
  (d/transact aconn {:tx-data [{:db/ident :foo}]})
  (def tx-report *1)
  (apply max-key :e (:tx-data tx-report))

  (seq (d/datoms (d/db aconn) {:index      :eavt
                               :components [45]}))
  )

(comment
  (require 'sc.api)
  (sc.api/defsc 806)
  (:tx-count acc)
  (def samplesc (d/client {:server-type :dev-local
                           :system      "datomic-samples"}))
  (d/list-databases samplesc {})
  (def source-db (d/db (d/connect samplesc {:db-name "mbrainz-subset"})))

  (def the-e *e)

  (impl/q-schema source-db)
  (def schema *1)
  (:language/name schema)

  (def destc (d/client {:server-type :dev-local
                        :storage-dir :mem
                        :system      "dest"}))
  (d/create-database destc {:db-name "test"})
  (d/delete-database destc {:db-name "test"})
  (def dest-conn (d/connect destc {:db-name "test"}))

  (full-copy
    {:source-db      source-db
     :dest-conn      dest-conn
     :max-batch-size 100})

  (def sm (impl/q-schema source-db))
  (get sm :language/name)
  (def stx (into []
             (comp
               (map (fn [[_ schema]] (walk/postwalk (fn [x] (if (map? x) (dissoc x :db/id) x)) schema)))
               (distinct))
             sm))
  (filter (fn [x]
            (= :language/name (:db/ident x))) stx)
  )
