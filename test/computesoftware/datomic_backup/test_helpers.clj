(ns computesoftware.datomic-backup.test-helpers
  (:require
    [datomic.client.api :as d])
  (:import (java.io File Closeable)))

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

(defn tx-date
  [date]
  [:db/add "datomic.tx" :db/txInstant date])

(defn test-data!
  ([conn] (test-data! conn {}))
  ([conn {:keys [start-date]
          :or   {start-date #inst"2020"}}]
   (let [start-tx (tx-date start-date)
         transact #(d/transact conn (update % :tx-data conj start-tx))]
     (transact {:tx-data example-schema})
     (transact {:tx-data [{:school/id       1
                           :school/students [{:student/first "John"
                                              :student/last  "Doe"
                                              :student/email "johndoe@university.edu"}]
                           :school/courses  [{:course/id "BIO-101"}
                                             {:course/id "BIO-102"}]}
                          {:semester/year   2018
                           :semester/season :fall}]})
     (transact {:tx-data [{:reg/course   [:course/id "BIO-101"]
                           :reg/semester [:semester/year+season [2018 :fall]]
                           :reg/student  [:student/email "johndoe@university.edu"]}]})
     (transact {:tx-data [[:db/retract [:course/id "BIO-102"] :course/id]]}))))