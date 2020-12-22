(ns computesoftware.datomic-backup
  (:require
    [datomic.client.api :as d]
    [clojure.java.io :as io]
    [computesoftware.datomic-backup.impl :as impl])
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
                    :system      "dev2"}))
  (d/create-database c {:db-name "db1"})
  (d/delete-database c {:db-name "db1"})
  (def conn (d/connect c {:db-name "db1"}))
  (d/create-database c {:db-name "dest"})
  (def dest (d/connect c {:db-name "dest"}))
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

(defn backup-current-db
  [{:keys [remove-empty-transactions?] :as backup-arg-map}]
  (let [db (d/db (:source-conn backup-arg-map))]
    (backup-db
      (assoc backup-arg-map
        :transform-datoms
        (impl/current-db-transform-fn db remove-empty-transactions?)))))

(comment
  (backup-current-db
    {:source-conn conn
     :backup-file "backup.txt"})
  )