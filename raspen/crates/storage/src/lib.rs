// raspen-storage: RocksDB-backed KV store for the replicated log.
//
// This crate depends on `raspen-types` for `KvEntry` and implements the
// `KvStoreOps` trait defined there. The `types` crate holds no dependency on
// this crate, avoiding a circular dependency.

pub mod kv_store;
pub use kv_store::KvStore;
