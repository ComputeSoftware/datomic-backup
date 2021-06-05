(ns computesoftware.datomic-backup
  (:require
    [clojure.set :as sets]
    [datomic.client.api :as d]
    [clojure.java.io :as io]
    [computesoftware.datomic-backup.impl :as impl]
    [clojure.walk :as walk]
    [computesoftware.datomic-backup.copy-db :as copy-db])
  (:import (java.io Closeable)))

(defn restore-db
  [{:keys [source dest-conn stop init-state with? transact progress]
    :or   {transact d/transact}}]
  (let [max-tx-id (when progress (impl/max-tx-id-from-source source))
        source (if (impl/conn? source) source (io/reader (io/file source)))
        init-state (assoc init-state :tx-count 0)
        transactions (impl/transactions-from-source source
                       (cond-> {}
                         (:last-imported-tx init-state)
                         (assoc :start (inc (:last-imported-tx init-state)))
                         stop (assoc :stop stop)))
        init-db ((if with? d/with-db d/db) dest-conn)]
    (try
      (reduce
        (fn [state datoms]
          (cond-> (impl/next-datoms-state state
                    datoms
                    (if with?
                      #(d/with (:db-before state) %)
                      #(transact dest-conn %)))
            progress
            (impl/next-progress-report progress (:tx (first datoms)) max-tx-id)))
        (assoc init-state
          :db-before init-db
          :source-eid->dest-eid (or
                                  (:source-eid->dest-eid init-state)
                                  (impl/initial-eid-mapping init-db)))
        transactions)
      (finally
        (when (instance? Closeable source) (.close source))))))

(defn backup-db
  [{:keys [source-conn backup-file stop transform-datoms progress] :as arg-map}]
  (let [filter-fn (when-let [fmap (:filter arg-map)]
                    (impl/filter-map->fn (d/db source-conn) fmap))
        max-tx-id (when progress (impl/max-tx-id-from-source source-conn))
        last-imported-tx (impl/last-backed-up-tx-id backup-file)
        init-state (cond-> {:tx-count 0}
                     last-imported-tx
                     (assoc :last-imported-tx last-imported-tx))
        transactions (impl/transactions-from-source source-conn
                       (cond-> {}
                         (:last-imported-tx init-state)
                         (assoc :start (inc (:last-imported-tx init-state)))
                         stop (assoc :stop stop)
                         (or filter-fn transform-datoms)
                         (assoc :transform-datoms
                                (fn [datoms]
                                  ((comp
                                     (or transform-datoms identity)
                                     (or filter-fn identity))
                                   datoms)))))]
    (with-open [wtr (io/writer (io/file backup-file) :append true)]
      (reduce
        (fn [state datoms]
          (cond-> (impl/next-file-state state datoms wtr)
            progress
            (impl/next-progress-report progress (:tx (first datoms)) max-tx-id)))
        init-state
        transactions))))

(comment
  (def c (d/client {:server-type :dev-local
                    :storage-dir :mem
                    :system      "dev2"}))
  (d/create-database c {:db-name "db1"})
  (d/delete-database c {:db-name "db1"})
  (def conn (d/connect c {:db-name "db1"}))
  (d/create-database c {:db-name "dest"})
  (def dest (d/connect c {:db-name "dest"}))

  (d/transact conn {:tx-data [{:db/ident       :tuple1
                               :db/valueType   :db.type/tuple
                               :db/tupleType   :db.type/ref
                               :db/cardinality :db.cardinality/one}
                              {:db/ident       :tuple2
                               :db/valueType   :db.type/tuple
                               :db/tupleTypes  [:db.type/ref :db.type/ref]
                               :db/cardinality :db.cardinality/one}]})

  (d/transact conn {:tx-data [#_{:number 1
                                 :db/id  "1"}
                              {:tuple1 [96757023244364 96757023244364]}]})

  (d/transact conn {:tx-data [{:db/ident       :number
                               :db/cardinality :db.cardinality/one
                               :db/valueType   :db.type/long}
                              {:db/ident       :id
                               :db/cardinality :db.cardinality/one
                               :db/valueType   :db.type/long
                               :db/unique      :db.unique/identity}]})
  (d/transact conn {:tx-data [{:id     1
                               :number 1}]})
  (d/transact conn {:tx-data [[:db/retractEntity [:id 1]]]})
  (d/transact conn {:tx-data []})
  (type conn)

  (backup-db {:source-conn conn
              :backup-file "my-backup.txt"})

  (with-open [rdr (io/reader (io/file "my-backup.txt"))]
    (restore-db
      {:source     rdr
       :dest-conn  dest
       :with?      true
       :state-file "resource-state.edn"}))

  (:t (d/db dest))

  (def b (backup-from-conn conn {}))
  (backup-to-file b {:file "test.txt"})
  (backup-from-file "test.txt" {})
  (apply-backup dest {:backup b :with? true})
  )

(defn backup-db-no-history
  [{:keys [remove-empty-transactions?] :as backup-arg-map}]
  (let [db (d/db (:source-conn backup-arg-map))]
    (backup-db
      (assoc backup-arg-map
        :transform-datoms
        (impl/no-history-transform-fn db remove-empty-transactions?)))))

(comment
  (backup-db-no-history
    {:source-conn conn
     :backup-file "backup.txt"}))

(defn copy-db
  [{:keys [source-db dest-conn max-batch-size]
    :or   {max-batch-size 500}}]
  (copy-db/full-copy {:source-db      source-db
                      :dest-conn      dest-conn
                      :max-batch-size max-batch-size}))

(comment
  (def c2 (d/client {:server-type :dev-local
                     :storage-dir :mem
                     :system      "t"}))
  (def conn (d/connect c2 {:db-name "cust-db__0535019e-79fe-44a1-a8d9-b19394abd958"}))
  (do
    (d/delete-database c {:db-name "dest"})
    (d/create-database c {:db-name "dest"})
    (def dest (d/connect c {:db-name "dest"})))

  (def copy-result (copy-db {:source-db (d/db conn) :dest-conn dest}))
  (count (:old-id->new-id copy-result))
  (get (:old-id->new-id copy-result) 87960930222593)

  (count (map :e (d/datoms (d/db conn) {:index :eavt :limit -1})))


  (d/q '[:find (pull ?c [*])
         :where
         [?c :customer/id]]
    (d/db dest))
  (d/pull (d/db dest)
    '[*]
    101155069867444)

  (d/q '[:find ?c
         :where
         [?c :integration/id]]
    (d/db conn))
  (d/pull (d/db conn)
    '[*]
    87960930222593)



  )

(comment
  (d/delete-database c {:db-name "dest"})
  (get schema-lookup 4)
  (impl/q-schema (d/db conn))

  (copy-db {:source-db (d/db conn)
            :dest-conn dest})

  (d/pull (d/db dest) '[*] [:id 1])

  (d/pull (d/db dest)
    '[*]
    [:customer/id #uuid"0535019e-79fe-44a1-a8d9-b19394abd958"])





  (d/with (d/with-db dest)
    {:tx-data [[:db/add "73" :db/ident :number]
               [:db/add "73" :db/valueType 22]
               [:db/add "73" :db/cardinality 35]
               [:db/add "74" :db/ident :id]
               [:db/add "74" :db/valueType 22]
               [:db/add "74" :db/cardinality 35]
               [:db/add "74" :db/unique 38]
               ;[:db/add "13194139533318" :db/txInstant #inst"2021-04-26T23:23:06.318-00:00"]
               ;[:db/add "13194139533319" :db/txInstant #inst"2021-04-26T23:23:07.996-00:00"]
               #_[:db/add "74766790688843" :number 1]
               #_[:db/add "74766790688843" :id 1]]
     })

  (def is (into #{} (map :e) (impl/bootstrap-datoms (d/db conn))))
  (contains? is 73)
  tx-data

  (require 'sc.api)


  (sc.api/defsc 2)

  (d/datoms (d/since (d/db conn) (impl/bootstrap-datoms-stop-tx (d/db conn))) {:index :eavt}))
