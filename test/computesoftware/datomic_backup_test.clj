(ns computesoftware.datomic-backup-test
  (:require
    [clojure.test :refer :all]
    [datomic.client.api :as d]
    [computesoftware.datomic-backup :as backup]
    [computesoftware.datomic-backup.impl :as impl]
    [clojure.java.io :as io]
    [computesoftware.datomic-backup.test-helpers :as testh]))

(deftest get-backup-test
  (with-open [ctx (testh/test-ctx {})]
    (let [backup (backup/backup-db
                   {:source-conn (:source-conn ctx)
                    :backup-file (testh/tempfile)})]
      (is (= {:tx-count 0} backup)
        "db with no transactions yields empty list"))))

(deftest backup->conn-integration-test
  (with-open [ctx (testh/test-ctx {})]
    (testing "schema, test data additions only"
      (testh/test-data! (:source-conn ctx))
      (let [file (testh/tempfile)
            backup (backup/backup-db {:source-conn (:source-conn ctx)
                                      :backup-file file})]
        (backup/restore-db {:source    file
                            :dest-conn (:dest-conn ctx)})
        (is (= {:school/id       1
                :school/students [{:student/email "johndoe@university.edu"
                                   :student/first "John"
                                   :student/last  "Doe"}]}
              (d/pull (d/db (:dest-conn ctx))
                [:school/id
                 {:school/students [:student/first
                                    :student/last
                                    :student/email]}]
                [:school/id 1])))))))

(deftest conn->conn-integration-test
  (with-open [ctx (testh/test-ctx {})]
    (testing "restore conn -> conn"
      (testh/test-data! (:source-conn ctx))
      (backup/restore-db {:source    (:source-conn ctx)
                          :dest-conn (:dest-conn ctx)})
      (is (= {:school/id       1
              :school/students [{:student/email "johndoe@university.edu"
                                 :student/first "John"
                                 :student/last  "Doe"}]}
            (d/pull (d/db (:dest-conn ctx))
              [:school/id
               {:school/students [:student/first
                                  :student/last
                                  :student/email]}]
              [:school/id 1]))))))

(deftest backup-current-db-integration-test
  (with-open [ctx (testh/test-ctx {})]
    (testh/test-data! (:source-conn ctx))
    (testing "restore conn -> conn"
      (let [file (testh/tempfile)
            backup (backup/backup-current-db {:source-conn                (:source-conn ctx)
                                              :remove-empty-transactions? true
                                              :backup-file                file
                                              :filter                     {:exclude-attrs [:student/first]}})]
        (is (= 3
              (count
                (with-open [rdr (io/reader file)]
                  (vec (impl/transactions-from-source rdr {}))))))
        (backup/restore-db {:source    file
                            :progress? true
                            :dest-conn (:dest-conn ctx)})
        (is (= {:school/id       1
                :school/students [{:student/email "johndoe@university.edu"
                                   :student/last  "Doe"}]}
              (d/pull (d/db (:dest-conn ctx))
                [:school/id
                 {:school/students [:student/first
                                    :student/last
                                    :student/email]}]
                [:school/id 1])))
        (is (= (list)
              (d/datoms (d/history (d/db (:dest-conn ctx)))
                {:index      :eavt
                 :components [[:course/id "BIO-102"]]}))
          "no history of entity is included")))))