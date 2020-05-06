(ns konserve-template.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!] :as async]
            [konserve.core :as k]
            [konserve-template.core :refer [new-your-store delete-store]]
            [malli.generator :as mg])
  (:import  [clojure.lang ExceptionInfo]))

(deftest core-test
  (testing "Test the core API."
    (let [_ (println "Core API test")
          store (<!! (new-your-store "critical"))]
      (is (= (<!! (k/get store :foo))
             nil))
      (is (not (<!! (k/exists? store :foo))))      
      (is (= nil (<!! (k/get-meta store :foo))))
      (<!! (k/bget store :foo 
        (fn [{:keys [input-stream]}] 
          (is (nil? input-stream))))) 
      (<!! (k/assoc store :foo :bar))
      (is (= (<!! (k/get store :foo))
             :bar))
      (is (<!! (k/exists? store :foo)))
      (<!! (k/assoc-in store [:foo] :bar2))
      (is (= :bar2 (<!! (k/get store :foo))))
      (is (= :foo (:key (<!! (k/get-meta store :foo)))))
      (is (= :default
             (<!! (k/get-in store [:fuu] :default))))
      (is (= :bar2 (<!! (k/get store :foo))))
      (is (= :default
             (<!! (k/get-in store [:fuu] :default))))
      (<!! (k/update-in store [:foo] name))
      (is (= "bar2"
             (<!! (k/get store :foo))))
      (<!! (k/assoc-in store [:baz] {:bar 42}))
      (is (= (<!! (k/get-in store [:baz :bar]))
             42))
      (<!! (k/update-in store [:baz :bar] inc))
      (is (= (<!! (k/get-in store [:baz :bar]))
             43))
      (<!! (k/update-in store [:baz :bar] + 2 3))
      (is (= (<!! (k/get-in store [:baz :bar]))
             48))
      (<!! (k/dissoc store :foo))
      (is (= (<!! (k/get-in store [:foo]))
             nil))
      (<!! (k/bassoc store :binbar (byte-array (range 10))))
      (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                                    (is (= (map byte (slurp input-stream))
                                           (range 10))))))
      (is (= #{:baz :binbar}
             (<!! (async/into #{} (k/keys store)))))
      (delete-store store)
      nil)))

(deftest append-test
  (testing "Test the append store functionality."
    (let [_ (println "Append test")
          store (<!! (new-your-store "critical"))]
      (<!! (k/append store :foo {:bar 42}))
      (<!! (k/append store :foo {:bar 43}))
      (is (= (<!! (k/log store :foo))
             '({:bar 42}{:bar 43})))
      (is (= (<!! (k/reduce-log store
                              :foo
                              (fn [acc elem]
                                (conj acc elem))
                              []))
             [{:bar 42} {:bar 43}]))
      (delete-store store))))

(deftest invalid-store-test
  (testing "Invalid store functionality."
    (let [_ (println "Invalid store test")
          store (<!! (new-your-store nil))]
      (is (= ExceptionInfo (type store))))))

(def home
  [:map
    [:name string?]
    [:description string?]
    [:rooms pos-int?]
    [:capacity float?]
    [:address
      [:map
        [:street string?]
        [:number int?]
        [:country [:enum "kenya" "lesotho" "south-africa" "italy" "mozambique" "spain" "india" "brazil" "usa" "germany"]]]]])

(deftest realistic-test
  (testing "Realistic data test."
    (let [_ (println "Realistic data test")
          store (<!! (new-your-store "critical"))
          home (mg/generate home {:size 20 :seed 2})
          address (:address home)
          addressless (dissoc home :address)
          name (mg/generate keyword? {:size 15 :seed 3})
          num1 (mg/generate pos-int? {:size 5 :seed 4})
          num2 (mg/generate pos-int? {:size 5 :seed 5})
          floater (mg/generate float? {:size 5 :seed 6})]
      
      (<!! (k/assoc store name addressless))
      (is (= addressless 
             (<!! (k/get store name))))

      (<!! (k/assoc-in store [name :address] address))
      (is (= home 
             (<!! (k/get store name))))

      (<!! (k/update-in store [name :capacity] * floater))
      (is (= (* floater (:capacity home)) 
             (<!! (k/get-in store [name :capacity]))))  

      (<!! (k/update-in store [name :address :number] + num1 num2))
      (is (= (+ num1 num2 (:number address)) 
             (<!! (k/get-in store [name :address :number]))))             
      
      (delete-store store))))   

(deftest bulk-test
  (testing "Bulk data test."
    (let [_ (println "Bulk data test")
          store (<!! (new-your-store "critical"))
          string20MB (apply str (vec (range 3000000)))
          range2MB 2097152
          sevens (repeat range2MB 7)]
      (print "\nWriting 20MB string: ")
      (time (<!! (k/assoc store :record string20MB)))
      (is (= (count string20MB) (count (<!! (k/get store :record)))))
      (println "Verifying 20MB string...\n")
      (print "Writing 20MB binary: ")
      (time (<!! (k/bassoc store :binary (byte-array sevens))))
      (println "Verifying 20MB binary...\n")
      (<!! (k/bget store :binary (fn [{:keys [input-stream]}]
                                    (is (= (pmap byte (slurp input-stream))
                                           sevens)))))
      (println "Done.")                            
      (delete-store store))))  

(deftest exceptions-test
  (testing "Test the append store functionality."
    (let [_ (println "Exceptions test")
          store (<!! (new-your-store "critical"))
          corrupt (update-in store [:store] reset! (atom {:data {}}))]
      (is (= ExceptionInfo (type (<!! (k/update-in corrupt {} 10)))))
      (is (= ExceptionInfo (type (<!! (k/get corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/get-meta corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/bget corrupt :bad (fn [_] nil))))))
      (is (= ExceptionInfo (type (<!! (k/assoc corrupt :bad 10)))))
      (is (= ExceptionInfo (type (<!! (k/bassoc store :binbar (byte-array (range 10)))))))
      (is (= ExceptionInfo (type (<!! (k/dissoc corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/assoc-in corrupt [:bad :robot] 10)))))
      (is (= ExceptionInfo (type (<!! (k/update-in corrupt [:bad :robot] inc)))))
      (is (= ExceptionInfo (type (<!! (k/exists? corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/keys corrupt)))))
      (delete-store store))))