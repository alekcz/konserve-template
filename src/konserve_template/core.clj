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
                                        PKeyIterable
                                        -keys]]
            [incognito.edn :refer [read-string-safe]]
            [clojure.string :as str])
  (:import  [java.io ByteArrayInputStream]))

(set! *warn-on-reflection* 1)

(defn fail [store]
  (if (nil? (:auth @store)) (throw (Exception. "Boo!")) nil)) 

(defn serialize [data]
;the simplest way to serialize data is using pr-str 
  (if (bytes? data)
    {:data (identity data) :type "binary"}
    {:data (pr-str data) :type "regular"}))

(defn deserialize [data' read-handlers]
;and the simplest way to deserialize data is using incognito
  (if (= "binary" (:type data'))
    (:data data')
    (read-string-safe @read-handlers (:data data'))))

(defn it-exists? [store id]
  (fail store) ;simulate store failure
  ;returns a boolean
  (some? (get-in @store [:meta id]))) ;example
  
(defn get-it [store id read-handlers]
  (fail store) ;simulate store failure
  ;returns deserialized data as a map
  (let [meta (get-in @store [:meta id])
        data (get-in @store [:data id])]
    [(deserialize meta read-handlers) (deserialize data read-handlers)])) ;example

(defn get-it-only [store id read-handlers]
  (fail store) ;simulate store failure
  ;returns deserialized data as a map
  (deserialize (get-in @store [:data id]) read-handlers)) ;example

(defn get-meta-only [store id read-handlers]
  (fail store) ;simulate store failure
  ;returns deserialized data as a map
  (deserialize (get-in @store [:meta id]) read-handlers)) ;example

(defn update-it [store id data-and-meta read-handlers]
  (fail store) ;simulate store failure
  ;1. serialize the data
  ;2. update the data
  ;3. deserialize the updated data
  ;4. return the data
  (let [serialized-meta (serialize (first data-and-meta))
        serialized-data (serialize (second data-and-meta))
        stored-meta (swap! store assoc-in [:meta id] serialized-meta)
        stored-data (swap! store assoc-in [:data id] serialized-data)] ;example
        [(deserialize (get-in stored-meta [:meta id]) read-handlers) 
         (deserialize (get-in stored-data [:data id]) read-handlers)])) ;example

(defn delete-it [store id]
  (fail store) ;simulate store failure
  ;delete the data and return nil on success
  (swap! store update-in [:meta] dissoc id)
  (swap! store update-in [:data] dissoc id) ;example
  nil) 

(defn get-keys [store read-handlers]
  (fail store) ;simulate store failure
  ;returns deserialized data as a map
  (let [meta (get @store :meta)
        meta-vals (seq (vals meta))]
    (map #(-> (deserialize % read-handlers) :key) meta-vals))) ;example

(defn str-uuid [key] ;using hasch we creat a uuid and convert it to string. 
  (str (hasch/uuid key))) 

(defn prep-ex [^String message ^Exception e]
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn prep-stream [bytes]
 {:input-stream  (ByteArrayInputStream. bytes) 
  :size (count bytes)})

; Implementation of the konserve protocol starts here.
; All the functions above are helper functions to make the code more readable and 
; maintainable

(defrecord YourStore [store serializer read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (async/put! res-ch (it-exists? store (str-uuid key)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to determine if item exists" e)))))
      res-ch))

  (-get [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it-only store (str-uuid key) read-handlers)]
            (if (some? res) 
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-meta-only store (str-uuid key) read-handlers)]
            (if (some? res) 
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value metadata from store" e)))))
      res-ch))

  (-update-in [this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[fkey & rkey] key-vec
                old-val (get-it store (str-uuid fkey) read-handlers)
                new-val (update-it 
                              store
                              (str-uuid fkey)
                              (let [[meta data] old-val]
                                [(meta-up-fn meta) (if rkey (apply update-in data rkey up-fn args) (apply up-fn data args))])
                              read-handlers)]
            (async/put! res-ch [(second old-val) (second new-val)]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update or write value in store" e)))))
        res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (delete-it store (str-uuid key))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))))
        res-ch))

  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it-only store (str-uuid key) read-handlers)]
            (if (some? res) 
              (async/put! res-ch (locked-cb (prep-stream res)))  
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-bassoc [this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [old-val (get-it store (str-uuid key) read-handlers)
                new-val (update-it 
                              store
                              (str-uuid key)
                              (let [[meta _] old-val] ;We ignore the existing binary data and overwrite it.
                                [(meta-up-fn meta) input])
                              read-handlers)]
            (async/put! res-ch [(second old-val) (second new-val)]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update or write value in store" e)))))
        res-ch))

  PKeyIterable
  (-keys [_]
   (let [res-ch (async/chan)]
      (async/thread
        (try
          (doall
            (map 
              #(async/put! res-ch %)
              (get-keys store read-handlers)))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve keys from store" e)))))
        res-ch)))

(defn- store-initializer [critical config]
  (atom { :config config
          :auth critical
          :meta {}
          :data {}}))

(defn new-your-store
  "Creates a new store connected to your backend."
  [critical-data & {:keys [config serializer read-handlers write-handlers]
                    :or   {config {:config :default} ;add the specific atom or config for your store as an object
                           serializer (ser/string-serializer) ; or (ser/fressian-serializer)  
                           read-handlers (atom {}) 
                           write-handlers (atom {})}}]
    (let [res-ch (async/chan 1)] 
      (async/thread
        (try
          (let [your-conn (store-initializer critical-data config)] 
            (async/put! res-ch 
              (map->YourStore { :store your-conn
                                :error (fail your-conn) ;simulate store init error
                                :serializer serializer
                                :read-handlers read-handlers
                                :write-handlers write-handlers
                                :locks (atom {})})))
          (catch Exception e
            (async/put! res-ch (ex-info "Could note connect to Realtime database." {:type :store-error :store critical-data  :exception e})))))
      res-ch))

(defn delete-store [store]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
         ; do something to delete your store data.
        (reset! store nil)
        (catch Exception e (async/put! res-ch (prep-ex "Failed to delete store" e)))))          
        res-ch))