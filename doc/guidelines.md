# Implementing your conserve backend

This guide aims to help you implement konserve on top of a backend store of your choice. . It assumes that you're more or less familiar with clojure and the backend store you wish to integrate with. We'll not unpack the finer points of konserve but rather what you need to know to implement you store successfully. You can checkout the konserve repo for a [more detailed explanation](https://github.com/replikativ/konserve/blob/feature_metadata_support/doc/backend.org).

## Unpacking konserve
The idea of konserve is to abstract away the implementation differences across backend stores or databases and allow them all to be interacted with as if they are clojure maps. As a result the konserve API provides the following core functions:

- `exists?`
- `get` 
- `get-in`
- `assoc`
- `assoc-in`
- `update`
- `update-in`
- `dissoc`
- `bget`
- `bassoc`
- `keys`

These functions are conceptually identical to their clojure counterparts (with bget and bassoc) dealing with binary data. 

There are a few other functions in the API that are used internally for data integrity and performance and aren't particulary relevent at for us at this point. 

### Data representation in flight
Data moves through konserve as tuples containing the meta data as well as the actual data to be stored like so `[meta data]`. 
So if you see (first x) that is most probably meta data. And if you see (second x) it's probably that actual data.

### Data representation at rest
Because you have no control of what your users of will decide to store in your backend, the data needs to be serialized in a robust manner. To help you with this konserve ships with two serializers a string serializer and a fressian serializer. Both serializers use [incognito](https://github.com/replikativ/incognito) to enable the end user to serialize custom clojure types in your store. 

### I/O
All IO operation in conserve are asynchronouns and use [core.async](https://github.com/clojure/core.async). To help you avoid pitfalls in this regards this repo has simulates a backend store with inconsistent latency and implements the structure to handle that asynchronously. 

