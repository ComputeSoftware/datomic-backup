(ns computesoftware.datomic-backup.current-state-restore
  (:require
    [computesoftware.datomic-backup.impl :as impl]
    [datomic.client.api :as d]
    [datomic.client.api.async :as d.a]
    [clojure.set :as sets]
    [clojure.walk :as walk]
    [clojure.tools.logging :as log]
    [computesoftware.datomic-backup.retry :as retry]
    [clojure.core.async :as async]
    [clojure.string :as str])
  (:import (clojure.lang ExceptionInfo)
           (java.util.concurrent Executors)))

(defn resolve-datom
  [datom eid->schema old-id->new-id]
  (let [[e a v] datom
        attr-schema (get eid->schema a)
        get-value-type #(get-in eid->schema [% :db/valueType])
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
  [datoms pending eid->schema old-id->new-id]
  (let [;; an attempt to add a tuple or ref value pointing to an eid NOT in this
        ;; set should be attempted later
        eids-exposed (into (set (keys old-id->new-id))
                       (comp
                         (remove (fn [[_ a]]
                                   (contains? #{:db.type/tuple
                                                :db.type/ref}
                                     (get-in eid->schema [a :db/valueType]))))
                         (map :e))
                       datoms)
        datoms-and-pending (concat datoms (map :datom pending))]
    (reduce (fn [acc [e a v :as datom]]
              (if (= :db/txInstant (get-in eid->schema [a :db/ident]))
                ;; not actually used, only collected for reporting purposes
                (update acc :tx-eids (fnil conj #{}) e)
                (let [{:keys [tx
                              required-eids
                              resolved-e-id]
                       :as   resolved} (resolve-datom datom eid->schema old-id->new-id)
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
  [{:keys [dest-conn datom-batches eid->schema init-state debug]
    :or   {init-state {:input-datom-count 0
                       :old-id->new-id    {}
                       :tx-count          0
                       :tx-datom-count    0
                       :tx-eids           #{}}}}]
  (reduce
    (fn [{:keys [old-id->new-id] :as acc} batch]
      (let [{:keys [old-id->tempid
                    tx-data
                    tx-eids
                    pending]}
            (txify-datoms batch (:pending acc) eid->schema old-id->new-id)]
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
    init-state datom-batches))

(comment (sc.api/defsc 14)
  )

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
  (let [;; use start-exclusive b/c we can only set *start after we have SUCCESSFULLY
        ;; received a datom. If we have successfully received the datom, we don't
        ;; want to start there again. Instead, we want to start at the next value.
        start-exclusive (::start-exclusive argm)
        index-range-argm (cond-> argm
                           start-exclusive
                           (assoc :start start-exclusive))
        datoms (cond->> (d/index-range db index-range-argm)
                 start-exclusive
                 (drop 1))
        *start (volatile! nil)]
    (try
      (doseq [d datoms]
        (async/>!! dest-ch d)
        (vreset! *start (:v d)))
      (catch ExceptionInfo ex
        (if (retry/default-retriable? ex)
          (do
            (read-datoms-with-retry! db (assoc argm ::start-exclusive @*start) dest-ch)
            (log/warn "Retryable anomaly while reading datoms. Retrying with :start set..."
              :anomaly (ex-data ex)
              :start-exclusive @*start))
          (throw ex))))))

(defn read-datoms-in-parallel-sync2
  [source-db {:keys [attribute-eids dest-ch parallelism read-chunk]}]
  (let [exec (Executors/newFixedThreadPool parallelism)
        done-ch (async/chan)]
    (doseq [a attribute-eids]
      (.submit exec ^Runnable
        (fn []
          (log/debug "Start reading datoms..." :attrid a)
          (try
            (read-datoms-with-retry! source-db
              {:attrid a
               :chunk  read-chunk
               :limit  -1}
              dest-ch)
            (catch Exception ex (async/>!! dest-ch ex)))
          (async/>!! done-ch a))))
    (async/go-loop [n 0]
      (let [attr (async/<! done-ch)]
        (log/debug "Done reading attr." :attrid attr))
      (if (< (inc n) (count attribute-eids))
        (recur (inc n))
        (do (async/close! dest-ch) (async/thread (.shutdown exec)))))
    dest-ch))

(defn anom!
  [x]
  ;; check for map? since dev-local will throw when get'ing a field that does
  ;; not exist. Throws java.lang.IllegalArgumentException: No matching clause: :cognitect.anomalies/category
  (cond
    (and (map? x) (:cognitect.anomalies/category x))
    (throw (ex-info (:cognitect.anomalies/message x) x))
    (instance? Throwable x)
    (throw x)
    :else x))

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
           schema-lookup
           dest-conn
           max-batch-size
           debug
           read-parallelism
           read-chunk
           init-state
           attribute-eids]}]
  (let [*running? (atom true)
        datoms (let [ch (cond-> (async/chan 20000)
                          debug
                          (monitored-chan! {:runningf     #(deref *running?)
                                            :channel-name "datoms"}))]
                 (-> source-db
                   (read-datoms-in-parallel-sync2
                     {:attribute-eids attribute-eids
                      :parallelism    read-parallelism
                      :dest-ch        ch
                      :read-chunk     read-chunk})
                   (ch->seq)))
        eid->schema (::impl/eid->schema schema-lookup)
        ident->schema (::impl/ident->schema schema-lookup)
        batches (let [max-bootstrap-tx (impl/bootstrap-datoms-stop-tx source-db)
                      schema-ids (into #{} (map key) eid->schema)]
                  (->> datoms
                    (remove (fn [[e a _ tx]]
                              (or
                                (contains? schema-ids e)
                                (= (get-in ident->schema [:db.install/attribute :db/id]) a)
                                (<= tx max-bootstrap-tx))))
                    (partition-all max-batch-size)))]
    (try
      (copy-datoms
        (cond-> {:dest-conn     dest-conn
                 :datom-batches batches
                 :eid->schema   eid->schema}
          debug (assoc :debug debug)
          init-state (assoc :init-state init-state)))
      ;(catch Exception ex (sc.api/spy) (throw ex))
      (finally (reset! *running? false)))))

(defn copy-schema!
  [{:keys [dest-conn schema idents-to-copy]}]
  (let [source-schema (filter (comp (set idents-to-copy) :db/ident) schema)]
    (retry/with-retry
      #(d/transact dest-conn {:tx-data source-schema}))
    {:source-schema source-schema}))

(defn one-restore-pass
  [{::keys [schema-lookup] :as argm} {::keys [idents]}]
  (let [copy-schema-argm (assoc argm
                           :schema (::impl/schema-raw schema-lookup)
                           :idents-to-copy idents)
        _ (copy-schema! copy-schema-argm)
        attribute-eids (map (fn [i]
                              (get-in schema-lookup [::impl/ident->schema i :db/id]))
                         idents)
        copy-argm (assoc argm
                    :attribute-eids attribute-eids
                    :schema-lookup schema-lookup)]
    (-full-copy copy-argm)))

(defn get-passes
  [schema]
  (let [filter-idents (fn [pred]
                        (into #{}
                          (comp
                            (filter (fn [schema] (pred schema)))
                            (map :db/ident)
                            (remove (fn [ident]
                                      (and (qualified-keyword? ident)
                                        (or
                                          (= "db" (namespace ident))
                                          (str/starts-with? (namespace ident) "db."))))))
                          schema))
        batch2-pred #(= :db.type/tuple (:db/valueType %))
        a-idents-1 (filter-idents #(not (batch2-pred %)))
        a-idents-2 (filter-idents #(batch2-pred %))]
    [{::idents a-idents-1}
     {::idents a-idents-2}]))

(defn restore
  [{:keys [source-db] :as argm}]
  (let [source-schema-lookup (impl/q-schema-lookup source-db)
        passes (get-passes (::impl/schema-raw source-schema-lookup))
        one-pass-argm (assoc argm
                        ::schema-lookup source-schema-lookup)]
    (reduce
      (fn [full-copy-init-state pass]
        (one-restore-pass (cond-> one-pass-argm
                            full-copy-init-state
                            (assoc :init-state full-copy-init-state))
          pass))
      nil passes)
    true))

(comment (sc.api/defsc 3))

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

  (def schema *1)
  (:language/name schema)

  (def destc (d/client {:server-type :dev-local
                        :storage-dir :mem
                        :system      "dest"}))
  (d/create-database destc {:db-name "test"})
  (d/delete-database destc {:db-name "test"})
  (def dest-conn (d/connect destc {:db-name "test"}))

  (restore
    {:source-db      source-db
     :dest-conn      dest-conn
     :max-batch-size 100})

  (get sm :language/name)
  (def stx (into []
             (comp
               (map (fn [[_ schema]] (walk/postwalk (fn [x] (if (map? x) (dissoc x :db/id) x)) schema)))
               (distinct))
             sm))
  (filter (fn [x]
            (= :language/name (:db/ident x))) stx)
  )
