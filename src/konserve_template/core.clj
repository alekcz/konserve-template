(ns konserve-template.core
  "Address globally aggregated immutable key-value store(s)."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [hasch.core :as hasch]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget
                                        -serialize -deserialize
                                        PKeyIterable
                                        -keys]]
            [incognito.edn :refer [read-string-safe]])
  (:import  [java.io ByteArrayInputStream ByteArrayOutputStream]))

(set! *warn-on-reflection* 1)

(defn- reality 
  "This function has nothing to do with konserve. 
   It is used to simulate a latency and faile in interact your store.
   You can remove this function once you've connected konserve to your store"
  [store]
  (Thread/sleep (rand-int 200))
  (if (nil? (:auth @store)) (throw (Exception. "Boo!")) nil)) 

(defn prep-write 
  "Doc string"
  [data]
  data)

(defn prep-read 
  "Doc string"
  [data']
  data')

(defn it-exists? 
  "Doc string"
  [store id]
  (reality store) ;simulate store failure
  ;returns a boolean
  (some? (get-in @store [:data id]))) ;example
  
(defn get-it 
  "Doc string"
  [store id]
  (reality store) ;simulate store failure
  ;returns deserialized data as a map
  (get-in @store [:data id]))

(defn update-it 
  "Doc string"
  [store id data]
  (reality store) ;simulate store failure
  ;1. serialize the data
  ;2. update the data
  ;3. deserialize the updated data
  ;4. return the data
  (swap! store assoc-in [:data id] data)) ;example

(defn delete-it 
  "Doc string"
  [store id]
  (reality store) ;simulate store failure
  ;delete the data and return nil on success
  (swap! store update-in [:data] dissoc id)) 

(defn get-keys 
  "Doc string"
  [store]
  (reality store) ;simulate store failure
  ;returns deserialized data as a map
  (let [keys (seq (vals (get-in @store [:data])))]
    keys)) ;example

(defn str-uuid 
  "Doc string"
  [key] ;using hasch we create a uuid and convert it to string. 
  (str (hasch/uuid key))) 

(defn prep-ex 
  "Doc string"
  [^String message ^Exception e]
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn prep-stream 
  "Doc string"
  [bytes]
  { :input-stream  (ByteArrayInputStream. bytes) 
    :size (count bytes)})

; Implementation of the konserve protocol starts here.
; All the functions above are helper functions to make the code more readable and 
; maintainable

(defrecord YourStore [store serializer read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? 
    ;"Doc string"
    [this key] 
      (let [res-ch (async/chan 1)]
        (async/thread
          (try
            (async/put! res-ch (it-exists? store (str-uuid key)))
            (catch Exception e (async/put! res-ch (prep-ex "Failed to determine if item exists" e)))))
        res-ch))

  (-get 
    ;Doc string"
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it store (str-uuid key))]
            (if (some? res) 
              (let [bais (ByteArrayInputStream. res)] 
                (async/put! res-ch (second (-deserialize serializer read-handlers bais))))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta 
    ;"Doc string"
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it store (str-uuid key))]
            (if (some? res) 
              (let [bais (ByteArrayInputStream. res)] 
                (async/put! res-ch (first (-deserialize serializer read-handlers bais))))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value metadata from store" e)))))
      res-ch))

  (-update-in 
    ;"Doc string"
    [this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[fkey & rkey] key-vec
                old' (get-it store (str-uuid fkey))
                old   (when old'
                        (let [bais (ByteArrayInputStream. old')]
                          (-deserialize serializer write-handlers bais)))
                new [(meta-up-fn (first old)) 
                     (if rkey (apply update-in (second old) rkey up-fn args) (apply up-fn (second old) args))]
                baos (ByteArrayOutputStream.)]
            (-serialize serializer baos write-handlers new)
            (update-it store (str-uuid fkey) (.toByteArray baos))
            (async/put! res-ch [(second old) (second new)]))
          (catch Exception e (.printStackTrace e) (async/put! res-ch (prep-ex "Failed to update/write value in store" e)))))
        res-ch))

  (-assoc-in [
    ;"Doc string"
    this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc 
    ;"Doc string"
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (delete-it store (str-uuid key))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))))
        res-ch))

  PBinaryAsyncKeyValueStore
  (-bget 
    ;"Doc string"
    [this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it store (str-uuid key) read-handlers)]
            (if (some? res) 
              (async/put! res-ch (locked-cb (prep-stream res)))  
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve binary value from store" e)))))
      res-ch))

  (-bassoc 
    ;"Doc string"
    [this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [old' (get-it store (str-uuid key))
                old   (when old'
                        (let [bais (ByteArrayInputStream. old')]
                          (-deserialize serializer write-handlers bais)))
                new [(meta-up-fn (first old)) input]
                baos (ByteArrayOutputStream.)]
            (-serialize serializer baos write-handlers new)
            (update-it store (str-uuid key) (.toByteArray baos))
            (async/put! res-ch [(second old) (second new)]))
          (catch Exception e (.printStackTrace e) (async/put! res-ch (prep-ex "Failed to update/write binary value in store" e)))))
        res-ch))

  PKeyIterable
  (-keys 
    ;"Doc string"
    [_]
    (let [res-ch (async/chan)]
      ;(async/thread
        (try
          (let [key-stream (get-keys store)
                keys' (when key-stream
                        (for [k key-stream]
                          (let [bais (ByteArrayInputStream. k)]
                            (first (-deserialize serializer read-handlers bais)))))
                keys (map :key keys')]
            (doall
              (map #(async/put! res-ch %) keys)))
          (async/close! res-ch) 
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve keys from store" e))));)
        res-ch)))

; Setting up your store

(defn- store-initializer 
  "Doc string"
  [critical config]
  (atom { :config config
          :auth critical
          :meta {}
          :data {}}))

(defn new-your-store
  "Creates a new store connected to your backend."
  [critical-data & {:keys [config serializer read-handlers write-handlers]
                    :or   {config {:config :default} ;add the specific atom or config for your store as an object
                           serializer (ser/fressian-serializer) ; or (ser/string-serializer)
                           read-handlers (atom {}) 
                           write-handlers (atom {})}}]
    (let [res-ch (async/chan 1)] 
      (async/thread
        (try
          (let [your-conn (store-initializer critical-data config)] 
            (async/put! res-ch 
              (map->YourStore { :store your-conn
                                :error (reality your-conn) ;simulate store init error
                                :serializer serializer
                                :read-handlers read-handlers
                                :write-handlers write-handlers
                                :locks (atom {})})))
          (catch Exception e
            (async/put! res-ch (ex-info "Could note connect to Realtime database." {:type :store-error :store critical-data  :exception e})))))
      res-ch))

(defn delete-store 
  "Doc string"
  [store]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
         ; do something to delete your store data.
        (reset! store nil)
        (catch Exception e (async/put! res-ch (prep-ex "Failed to delete store" e)))))          
        res-ch))