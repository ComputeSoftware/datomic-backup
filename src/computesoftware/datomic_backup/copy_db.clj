(ns computesoftware.datomic-backup.copy-db
  (:require
    [computesoftware.datomic-backup.impl :as impl]
    [datomic.client.api :as d]
    [clojure.set :as sets]
    [clojure.walk :as walk]))

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
  [{:keys [dest-conn datom-batches source-schema]}]
  (reduce
    (fn [{:keys [old-id->new-id] :as acc} batch]
      (let [{:keys [old-id->tempid
                    tx-data
                    tx-eids
                    pending]}
            (txify-datoms batch (:pending acc) source-schema old-id->new-id)]
        (assoc
          (if (seq tx-data)
            (let [{:keys [tempids]} (try
                                      #_(transact-with-max-batch-size dest-conn {:tx-data tx-data} max-batch-size)
                                      (d/transact dest-conn {:tx-data tx-data})
                                      (catch Exception ex
                                        ;(sc.api/spy)
                                        (throw ex)))
                  next-old-id->new-id (into old-id->new-id
                                        (map (fn [[old-id tempid]]
                                               (when-let [eid (get tempids tempid)]
                                                 [old-id eid])))
                                        old-id->tempid)]
              ;(prn 'batch batch)
              ;(prn 'tx-data tx-data)
              ;(prn 'old-id->tempid old-id->tempid)
              ;(prn 'tempids tempids)
              ;(prn 'next-old-id->new-id next-old-id->new-id)
              ;(prn '---)
              (-> acc
                (assoc
                  :old-id->new-id next-old-id->new-id)
                (update :tx-count (fnil inc 0))
                (update :tx-eids sets/union tx-eids)))
            acc)
          :pending pending)))
    {:tx-count       0
     :old-id->new-id {}
     :tx-eids        #{}} datom-batches))

(defn full-copy
  [{:keys [source-db dest-conn max-batch-size]}]
  (let [datoms (d/datoms source-db {:index :eavt :limit -1})
        source-schema (impl/q-schema source-db)
        batches (let [max-bootstrap-tx (impl/bootstrap-datoms-stop-tx source-db)
                      schema-ids (into #{} (comp (filter (fn [[x]] (number? x))) (map first)) source-schema)]
                  (->> datoms
                    (remove (fn [[e _ _ tx]]
                              (or
                                (contains? schema-ids e)
                                (<= tx max-bootstrap-tx))))
                    (partition-all max-batch-size)))]
    ;; schema tx
    (d/transact dest-conn {:tx-data (into []
                                      (comp
                                        (map (fn [[_ schema]] (walk/postwalk (fn [x] (if (map? x) (dissoc x :db/id) x)) schema)))
                                        (distinct))
                                      source-schema)})
    ;; regular txes
    (copy-datoms {:dest-conn     dest-conn
                  :datom-batches batches
                  :source-schema source-schema})))

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
