(ns computesoftware.datomic-backup.impl
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.edn :as edn]
    [datomic.client.api :as d]
    [datomic.client.api.protocols :as client-protocols]))

(defrecord Datom [e a v tx added]
  clojure.lang.Indexed
  (nth [d idx] (nth d idx nil))
  (nth [_ idx _]
    (case idx
      0 e
      1 a
      2 v
      3 tx
      4 added)))

;; a bit naughty...
(defmethod print-method Datom [[e a v tx added] ^java.io.Writer w]
  (.write w (str "#datom" (pr-str [e a v tx added]))))

(comment
  (clojure.edn/read-string
    {:readers {'datom datum-reader}}
    "#datom[74766790688859 79 \"BIO-102\" 13194139533321 false]"))

(defn datum-reader
  [[e a v tx added]]
  (map->Datom {:e e :a a :v v :tx tx :added added}))

(defn read-edn-string
  [s]
  (edn/read-string {:readers {'datom datum-reader}} s))

(defn last-backed-up-tx-id
  [f]
  (let [f (io/file f)]
    (when (.exists f)
      (with-open [rdr (io/reader (io/file f))]
        ;; hack for now...
        (-> (line-seq rdr)
          (last)
          (read-edn-string)
          (first)
          :tx)))))

(defn bootstrap-datoms
  [db]
  (let [ds (d/datoms db
             {:index      :avet
              :components [:db/txInstant #inst"1970-01-01"]})
        stop-tx (:tx (apply max-key :tx ds))
        bootstraped-db (d/as-of db stop-tx)]
    (d/datoms bootstraped-db {:index :eavt})))

(defn get-transaction-stream
  [conn {:keys [ignore-datom start stop timeout]}]
  (eduction
    (comp
      (map (fn [{:keys [data]}] (remove ignore-datom data)))
      (filter seq))
    (d/tx-range conn (cond-> {:limit -1}
                       start (assoc :start start)
                       stop (assoc :stop stop)
                       timeout (assoc :timeout timeout)))))

(comment
  (def bs-eids (into #{} (map :e) (bootstrap-datoms (d/db conn))))

  (initial-eid-mapping (d/db dest-conn))
  (def transactions
    (get-transaction-stream conn {:ignore-datom (fn [d] (contains? bs-eids (:e d)))})))

(defn initial-eid-mapping
  [dest-db]
  (into {}
    (map (fn [d] [(:e d) (:e d)]))
    (d/datoms dest-db {:index :eavt})))

(defn attr-value-type
  [db attr]
  (get-in (d/pull db [:db/valueType] attr) [:db/valueType :db/ident]))

(def tid-prefix "__tid")
(defn tempid [x] (str tid-prefix x))

(defn datom-batch-tx-data
  [dest-db datoms eid->real-eid]
  (let [effective-eid (fn [eid] (get eid->real-eid eid (tempid eid)))
        ref? (fn [a] (= :db.type/ref (attr-value-type dest-db a)))]
    (map (fn [d]
           (let [[e a v tx added] d]
             [(if added :db/add :db/retract)
              (if (= e tx) "datomic.tx" (effective-eid (:e d)))
              (effective-eid a)
              (if (ref? a) (effective-eid v) v)]))
      datoms)))

(comment
  (let [[e a v tx] (first (d/datoms (d/db dest-conn) {:index :eavt}))]
    [e])
  (datom-batch-tx-data
    (d/db dest-conn)
    (second (first transactions))
    (initial-eid-mapping (d/db dest-conn)))
  )

(defn next-data
  [tx-report]
  (let [{:keys [tempids]} tx-report]
    {:source-eid->dest-eid
     (into {}
       (comp
         (filter (fn [[tid]] (str/starts-with? tid tid-prefix)))
         (map (fn [[tid eid]]
                [(Long/parseLong (subs tid (count tid-prefix))) eid])))
       tempids)}))

(defn transactions-from-file
  [reader]
  (map read-edn-string (line-seq reader)))

(defn transactions-from-conn
  [source-conn {:keys [start stop]}]
  (let [db (d/db source-conn)
        stop-t (or stop (:t db))
        ignore-ids (into #{} (map :e) (bootstrap-datoms db))]
    (get-transaction-stream source-conn
      (cond-> {:ignore-datom (fn [d] (contains? ignore-ids (:e d)))
               :stop         stop-t}
        start
        (assoc :start start)))))

(defn conn? [x] (satisfies? client-protocols/Connection x))

(defn transactions-from-source
  [source arg-map]
  (if (conn? source)
    (transactions-from-conn source arg-map)
    (transactions-from-file source)))

(defn next-datoms-state
  [tx! {:keys [source-eid->dest-eid db-before] :as acc} datoms]
  (let [tx (:tx (first datoms))
        tx-data (datom-batch-tx-data db-before datoms source-eid->dest-eid)
        tx-report (try
                    (tx! {:tx-data tx-data})
                    (catch Exception ex
                      (throw
                        (ex-info (.getMessage ex)
                          {:tx-data              tx-data
                           :source-eid->dest-eid source-eid->dest-eid
                           :tx-count             (:tx-count acc)} ex))))
        nd (next-data tx-report)]
    (-> acc
      (assoc
        :db-before (:db-after tx-report)
        :last-imported-tx tx)
      (update :source-eid->dest-eid merge (:source-eid->dest-eid nd))
      (update :tx-count inc))))

(def separator (System/getProperty "line.separator"))

(defn next-file-state
  [writer acc datoms]
  (.write writer (pr-str datoms))
  (.write writer separator)
  (let [tx (:tx (first datoms))]
    (-> acc
      (assoc :last-imported-tx tx)
      (update :tx-count inc))))

(defn write-state-file!
  [state-file {:keys [:source-eid->dest-eid
                      :last-imported-tx]}]
  (spit state-file
    (cond-> {:version          1
             :last-imported-tx last-imported-tx}
      source-eid->dest-eid (assoc :source-eid->dest-eid source-eid->dest-eid))))