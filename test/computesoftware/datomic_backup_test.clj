(ns computesoftware.datomic-backup-test
  (:require
    [clojure.test :refer :all]
    [datomic.client.api :as d]
    [computesoftware.datomic-backup :as db-backup]
    [clojure.java.io :as io])
  (:import (java.io Closeable File)))

(defrecord TestCtx [closef]
  Closeable
  (close [_] (closef)))

(defn tempfile []
  (doto (File/createTempFile "datomic-backup-test" "")
    (.deleteOnExit)))

(def example-schema
  [{:db/ident       :student/first
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :student/last
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :student/email
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}

   {:db/ident       :semester/year
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident       :semester/season
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one}

   {:db/ident       :semester/year+season
    :db/valueType   :db.type/tuple
    :db/tupleAttrs  [:semester/year :semester/season]
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}

   {:db/ident       :course/id
    :db/valueType   :db.type/string
    :db/unique      :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident       :course/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :reg/course
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident       :reg/semester
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident       :reg/student
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident       :reg/course+semester+student
    :db/valueType   :db.type/tuple
    :db/tupleAttrs  [:reg/course :reg/semester :reg/student]
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}

   {:db/ident       :school/id
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}
   {:db/ident       :school/students
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/ident       :school/courses
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many}])

(defn test-ctx
  [{}]
  (let [client (d/client
                 {:server-type :dev-local
                  :system      (str *ns*)})
        cleanupf (fn []
                   (doseq [db (d/list-databases client {})]
                     (d/delete-database client {:db-name db})))
        conn! (fn [db-name]
                (d/create-database client {:db-name db-name})
                (d/connect client {:db-name db-name}))
        source-conn (conn! "source")
        dest-conn (conn! "dest")]
    (map->TestCtx
      {:closef      cleanupf
       :client      client
       :source-conn source-conn
       :dest-conn   dest-conn})))

(deftest get-backup-test
  (with-open [ctx (test-ctx {})]
    (let [backup (db-backup/backup-db
                   {:source-conn (:source-conn ctx)
                    :backup-file (tempfile)})]
      (is (= {:tx-count 0} backup)
        "db with no transactions yields empty list"))))

(deftest integration-test
  (with-open [ctx (test-ctx {})]
    (testing "schema, test data additions only"
      (d/transact (:source-conn ctx) {:tx-data example-schema})
      (d/transact (:source-conn ctx) {:tx-data [{:school/id       1
                                                 :school/students [{:student/first "John"
                                                                    :student/last  "Doe"
                                                                    :student/email "johndoe@university.edu"}]
                                                 :school/courses  [{:course/id "BIO-101"}
                                                                   {:course/id "BIO-102"}]}
                                                {:semester/year   2018
                                                 :semester/season :fall}]})
      (d/transact (:source-conn ctx) {:tx-data [{:reg/course   [:course/id "BIO-101"]
                                                 :reg/semester [:semester/year+season [2018 :fall]]
                                                 :reg/student  [:student/email "johndoe@university.edu"]}]})
      (d/transact (:source-conn ctx) {:tx-data [[:db/retract [:course/id "BIO-102"] :course/id]]})
      (let [file (tempfile)
            backup (db-backup/backup-db {:source-conn (:source-conn ctx)
                                         :backup-file file})]
        (db-backup/restore-db {:source    file
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