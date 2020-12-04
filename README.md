# Þingvellir (`thingvellir`)

[![Build Status](https://travis-ci.org/discordapp/thingvellir.svg?branch=master)](https://travis-ci.org/discordapp/thingvellir)
[![License](https://img.shields.io/github/license/discordapp/thingvellir.svg)](LICENSE)
[![Documentation](https://docs.rs/thingvellir/badge.svg)](https://docs.rs/thingvellir)
[![Cargo](https://img.shields.io/crates/v/thingvellir.svg)](https://crates.io/crates/thingvellir)

A rust library that allows you to build an in-memory write/read-through cache. It uses a sharded, shared-nothing actor model underneath ontop of [`tokio`](https://tokio.rs/).

Given a struct, and an upstream, this _abstraction_ handles the loading and committing of data. `thingvellir` is meant to sit in-front of a database (e.g. `cassandra`, or `scylladb`), providing a way to allow for in-memory caching of "hot" data within the database.

`thingvellir` exposes a key/value interface, abstracting away the database load and persist operations, allowing you to write your business logic as a simple struct that can be persisted to a database. Out of the box, `thingvellir` provides an upstream implementation, that will persist structs to cassandra, using a simple key/value schema, where the value is the [CBOR](https://cbor.io/) serialized form, using [`serde`](https://serde.rs/).

`thingvellir` handles object expiry, as well allowing a bounded in-memory data-set (the # of keys the `thingvellir` will cache can be configured), and keys are able to be evicted as they expire (we use a random expiration probe similar to Redis), and also maintains a probabilistic LRU to evict keys if the cache is at capacity.

`thingvellir` coalesces reads and writes to the upstream data store. If your data model is amenable to delayed write durability (for example, counters that you are okay with data loss if the node crashes before data is committed upstream). Or a "hot" key might get queried a lot, or see a burst of queries, `thingvellir` will ensure that the upstream only sees a single read query. The write durability can be configured on a per-mutation basis, allowing for _immediate_ commit, or _delayed_ commit (depending on how much data-loss your application can tolerate.)
