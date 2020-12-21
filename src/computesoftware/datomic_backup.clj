(ns computesoftware.datomic-backup
  (:require
    [datomic.client.api :as d]
    [clojure.java.io :as io]
    [computesoftware.datomic-backup.impl :as impl])
  (:import (java.io Closeable)))

(defn restore-db
  [{:keys [source dest-conn stop init-state with? transact]
    :or   {transact d/transact}}]
  (let [source (if (impl/conn? source) source (io/reader (io/file source)))
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
          (impl/next-datoms-state
            (if with?
              #(d/with (:db-before state) %)
              #(transact dest-conn %))
            state datoms))
        (assoc init-state
          :db-before init-db
          :source-eid->dest-eid (or
                                  (:source-eid->dest-eid init-state)
                                  (impl/initial-eid-mapping init-db)))
        transactions)
      (finally
        (when (instance? Closeable source) (.close source))))))

(defn backup-db
  [{:keys [source-conn backup-file stop]}]
  (let [last-imported-tx (impl/last-backed-up-tx-id backup-file)
        init-state (cond-> {:tx-count 0}
                     last-imported-tx
                     (assoc :last-imported-tx last-imported-tx))
        transactions (impl/transactions-from-source source-conn
                       (cond-> {}
                         (:last-imported-tx init-state)
                         (assoc :start (inc (:last-imported-tx init-state)))
                         stop (assoc :stop stop)))]
    (with-open [wtr (io/writer (io/file backup-file) :append true)]
      (reduce
        (fn [state datoms] (impl/next-file-state wtr state datoms))
        init-state
        transactions))))

(comment
  (def c (d/client {:server-type :dev-local
                    :system      "dev2"}))
  (d/create-database c {:db-name "db1"})
  (def conn (d/connect c {:db-name "db1"}))
  (d/create-database c {:db-name "dest"})
  (def dest (d/connect c {:db-name "dest"}))
  (d/transact conn {:tx-data [{:db/ident       :number
                               :db/cardinality :db.cardinality/one
                               :db/valueType   :db.type/long}]})
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