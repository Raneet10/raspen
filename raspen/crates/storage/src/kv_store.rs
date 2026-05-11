// Spec: KVStore (`int -> KVEntry`) — two backends:
//
//   Default (always compiled): `MemKvStore` — `HashMap<u64, KvEntry>` in-process store.
//   Optional (`rocksdb-backend` feature): `RocksKvStore` — RocksDB-backed persistent store.
//
// Both implement the `KvStoreOps` trait defined in `raspen-types`.
//
// The public `KvStore` type is an alias/newtype for the active backend, chosen at
// compile time.  In environments without LLVM / clang (needed to compile
// `librocksdb-sys`), the `MemKvStore` path is used and all tests still pass.
//
// Spec references:
//   Spec line 53: `type KVStore = int -> KVEntry`
//   Spec line 300: `kv_store.put(log_idx, entry)`
//   Spec line 198: `log_hash_at(kv_store, idx)` → KvStore::hash_at

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use sha2::{Digest, Sha256};

use raspen_types::{Hash, KvEntry, KvStoreOps};

// ===========================================================================
// Shared helpers (used by both backends)
// ===========================================================================

fn encode<T: bincode::Encode>(value: &T) -> anyhow::Result<Vec<u8>> {
    bincode::encode_to_vec(value, bincode::config::standard())
        .map_err(|e| anyhow::anyhow!("bincode encode: {e}"))
}

#[allow(dead_code)] // used in the rocksdb-backend feature path
fn decode<T: bincode::Decode<()>>(bytes: &[u8]) -> anyhow::Result<T> {
    let (value, _) = bincode::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| anyhow::anyhow!("bincode decode: {e}"))?;
    Ok(value)
}

/// Compute SHA-256 of a `KvEntry` (used by `hash_at`).
fn entry_hash(entry: &KvEntry) -> anyhow::Result<Hash> {
    let bytes = encode(entry)?;
    let digest = Sha256::digest(&bytes);
    Ok(digest.into())
}

// ===========================================================================
// MemKvStore — in-memory BTreeMap backend (always compiled)
// ===========================================================================

/// Interior-mutable state for `MemKvStore`.
#[derive(Debug, Default)]
struct MemInner {
    entries: BTreeMap<u64, KvEntry>,
    chkpt_idx: u64,
    chkpt_hash: Hash,
}

/// In-memory implementation of `KvStoreOps`.
///
/// Uses `Arc<RwLock<...>>` so the handle can be cheaply cloned by consensus
/// while still satisfying the `Send + Sync` bounds required by `KvStoreOps`.
#[derive(Debug, Clone)]
pub struct MemKvStore {
    inner: Arc<RwLock<MemInner>>,
}

impl MemKvStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MemInner::default())),
        }
    }

    pub fn get_chkpt_idx(&self) -> u64 {
        self.inner.read().unwrap().chkpt_idx
    }

    pub fn set_chkpt_idx(&self, idx: u64) {
        self.inner.write().unwrap().chkpt_idx = idx;
    }

    pub fn get_chkpt_hash(&self) -> Hash {
        self.inner.read().unwrap().chkpt_hash
    }

    pub fn set_chkpt_hash(&self, hash: Hash) {
        self.inner.write().unwrap().chkpt_hash = hash;
    }
}

impl Default for MemKvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStoreOps for MemKvStore {
    fn get(&self, idx: u64) -> anyhow::Result<Option<KvEntry>> {
        Ok(self.inner.read().unwrap().entries.get(&idx).cloned())
    }

    fn put(&self, idx: u64, entry: &KvEntry) -> anyhow::Result<()> {
        self.inner.write().unwrap().entries.insert(idx, entry.clone());
        Ok(())
    }

    fn size(&self) -> u64 {
        let inner = self.inner.read().unwrap();
        // Size = highest index + 1, or 0 if empty.
        inner
            .entries
            .keys()
            .next_back()
            .map(|&k| k + 1)
            .unwrap_or(0)
    }

    /// Returns the `prev_hash` of the entry at the highest index.
    /// If the store is empty, returns `[0u8; 32]`.
    fn top_hash(&self) -> Hash {
        let inner = self.inner.read().unwrap();
        inner
            .entries
            .values()
            .next_back()
            .map(|e| e.prev_hash)
            .unwrap_or([0u8; 32])
    }

    fn hash_at(&self, idx: u64) -> anyhow::Result<Hash> {
        let inner = self.inner.read().unwrap();
        let entry = inner
            .entries
            .get(&idx)
            .ok_or_else(|| anyhow::anyhow!("no entry at index {idx}"))?;
        entry_hash(entry)
    }

    fn rebuild_from_entries(&mut self, entries: Vec<(u64, KvEntry)>) -> anyhow::Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.entries.clear();
        for (idx, entry) in entries {
            inner.entries.insert(idx, entry);
        }
        Ok(())
    }
}

// ===========================================================================
// RocksKvStore — RocksDB backend (compiled only with `rocksdb-backend` feature)
// ===========================================================================

#[cfg(feature = "rocksdb-backend")]
pub mod rocks {
    //! RocksDB-backed `KvStore` implementation.
    //!
    //! Enabled with `--features raspen-storage/rocksdb-backend`.
    //! Requires `libclang` on the build machine (for `bindgen` in `librocksdb-sys`).
    //!
    //! RocksDB layout:
    //!   Default CF: key = u64 big-endian 8 bytes → value = bincode(KvEntry)
    //!   "meta" CF:  key = b"kv_size"    → value = bincode(u64)
    //!               key = b"chkpt_idx"  → value = bincode(u64)
    //!               key = b"chkpt_hash" → value = bincode([u8; 32])

    use std::path::Path;
    use std::sync::Arc;

    use anyhow::Context;
    use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, DB};

    use raspen_types::{Hash, KvEntry, KvStoreOps};

    use super::{decode, encode, entry_hash};

    fn entry_key(idx: u64) -> [u8; 8] {
        idx.to_be_bytes()
    }

    const META_KV_SIZE: &[u8] = b"kv_size";
    const META_CHKPT_IDX: &[u8] = b"chkpt_idx";
    const META_CHKPT_HASH: &[u8] = b"chkpt_hash";

    #[derive(Debug)]
    pub struct RocksKvStore {
        db: Arc<DB>,
    }

    impl RocksKvStore {
        pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            let cfs = vec![
                ColumnFamilyDescriptor::new("default", Options::default()),
                ColumnFamilyDescriptor::new("meta", Options::default()),
            ];
            let db = DB::open_cf_descriptors(&opts, path, cfs)
                .context("failed to open RocksDB")?;
            Ok(Self { db: Arc::new(db) })
        }

        fn meta_cf(&self) -> &rocksdb::ColumnFamily {
            self.db.cf_handle("meta").expect("meta CF always present")
        }

        fn read_meta_u64(&self, key: &[u8]) -> anyhow::Result<u64> {
            match self.db.get_cf(self.meta_cf(), key)? {
                Some(v) => decode::<u64>(&v),
                None => Ok(0),
            }
        }

        fn write_meta_u64(&self, key: &[u8], value: u64) -> anyhow::Result<()> {
            let encoded = encode(&value)?;
            self.db.put_cf(self.meta_cf(), key, encoded)?;
            Ok(())
        }

        pub fn get_chkpt_idx(&self) -> anyhow::Result<u64> {
            self.read_meta_u64(META_CHKPT_IDX)
        }

        pub fn set_chkpt_idx(&self, idx: u64) -> anyhow::Result<()> {
            self.write_meta_u64(META_CHKPT_IDX, idx)
        }

        pub fn get_chkpt_hash(&self) -> anyhow::Result<Hash> {
            match self.db.get_cf(self.meta_cf(), META_CHKPT_HASH)? {
                Some(v) => decode::<Hash>(&v),
                None => Ok([0u8; 32]),
            }
        }

        pub fn set_chkpt_hash(&self, hash: &Hash) -> anyhow::Result<()> {
            let encoded = encode(hash)?;
            self.db.put_cf(self.meta_cf(), META_CHKPT_HASH, encoded)?;
            Ok(())
        }
    }

    impl KvStoreOps for RocksKvStore {
        /// Spec: `kv_store[log_idx]`
        fn get(&self, idx: u64) -> anyhow::Result<Option<KvEntry>> {
            match self.db.get(entry_key(idx))? {
                Some(bytes) => Ok(Some(decode(&bytes)?)),
                None => Ok(None),
            }
        }

        /// Spec: `kv_store.put(log_idx, new_entry)` (spec line 300)
        fn put(&self, idx: u64, entry: &KvEntry) -> anyhow::Result<()> {
            let value = encode(entry)?;
            let mut batch = WriteBatch::default();
            batch.put(entry_key(idx), &value);

            let current_size = self.read_meta_u64(META_KV_SIZE)?;
            let new_size = current_size.max(idx + 1);
            if new_size > current_size {
                let size_bytes = encode(&new_size)?;
                batch.put_cf(self.meta_cf(), META_KV_SIZE, size_bytes);
            }
            self.db.write(batch)?;
            Ok(())
        }

        fn size(&self) -> u64 {
            self.read_meta_u64(META_KV_SIZE).unwrap_or(0)
        }

        fn top_hash(&self) -> Hash {
            let mut iter = self.db.iterator(IteratorMode::End);
            match iter.next() {
                Some(Ok((_key, value))) => match decode::<KvEntry>(&value) {
                    Ok(entry) => entry.prev_hash,
                    Err(_) => [0u8; 32],
                },
                _ => [0u8; 32],
            }
        }

        fn hash_at(&self, idx: u64) -> anyhow::Result<Hash> {
            let entry = self
                .get(idx)?
                .ok_or_else(|| anyhow::anyhow!("no entry at index {idx}"))?;
            entry_hash(&entry)
        }

        fn rebuild_from_entries(&mut self, entries: Vec<(u64, KvEntry)>) -> anyhow::Result<()> {
            let start = 0u64.to_be_bytes();
            let end = u64::MAX.to_be_bytes();
            self.db.delete_range_cf(
                &self.db.cf_handle("default").expect("default CF"),
                start,
                end,
            )?;

            let mut max_idx: u64 = 0;
            let mut batch = WriteBatch::default();
            for (idx, entry) in &entries {
                let value = encode(entry)?;
                batch.put(entry_key(*idx), value);
                if idx + 1 > max_idx {
                    max_idx = idx + 1;
                }
            }
            let size_bytes = encode(&max_idx)?;
            batch.put_cf(self.meta_cf(), META_KV_SIZE, size_bytes);
            self.db.write(batch)?;
            Ok(())
        }
    }
}

// ===========================================================================
// Public `KvStore` type: selects backend at compile time
// ===========================================================================

// When the `rocksdb-backend` feature is enabled, `KvStore` is `RocksKvStore`.
// Otherwise it is `MemKvStore`.

#[cfg(feature = "rocksdb-backend")]
pub use rocks::RocksKvStore as KvStore;

#[cfg(not(feature = "rocksdb-backend"))]
pub use MemKvStore as KvStore;

// ===========================================================================
// Constructor shim
// ===========================================================================

// Provide a unified `KvStore::open(path)` regardless of backend.
// For `MemKvStore` the path is ignored; for `RocksKvStore` it is used.

#[cfg(not(feature = "rocksdb-backend"))]
impl KvStore {
    /// Open (or create) a store at `path`.
    ///
    /// When using the in-memory backend the path is ignored.
    pub fn open(_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self::new())
    }
}

// ===========================================================================
// Unit tests (run against whichever backend is compiled in)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_temp() -> (TempDir, KvStore) {
        let dir = TempDir::new().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        (dir, store)
    }

    fn make_entry(op_hash: u64, result: u8, prev_hash: Hash) -> KvEntry {
        KvEntry {
            op_hash,
            result: vec![result],
            prev_hash,
        }
    }

    // -----------------------------------------------------------------------
    // put / get
    // -----------------------------------------------------------------------

    #[test]
    fn test_put_and_get() {
        let (_dir, store) = open_temp();
        let entry = make_entry(42, 1, [0u8; 32]);
        store.put(0, &entry).unwrap();

        let got = store.get(0).unwrap().expect("entry should exist");
        assert_eq!(got.op_hash, 42);
        assert_eq!(got.result, vec![1u8]);
    }

    #[test]
    fn test_get_missing() {
        let (_dir, store) = open_temp();
        assert!(store.get(99).unwrap().is_none());
    }

    #[test]
    fn test_put_multiple_and_retrieve() {
        let (_dir, store) = open_temp();
        for i in 0u64..5 {
            let entry = make_entry(i * 10, i as u8, [i as u8; 32]);
            store.put(i, &entry).unwrap();
        }
        for i in 0u64..5 {
            let got = store.get(i).unwrap().unwrap();
            assert_eq!(got.op_hash, i * 10);
        }
    }

    // -----------------------------------------------------------------------
    // size
    // -----------------------------------------------------------------------

    #[test]
    fn test_size_empty() {
        let (_dir, store) = open_temp();
        assert_eq!(store.size(), 0);
    }

    #[test]
    fn test_size_after_puts() {
        let (_dir, store) = open_temp();
        let entry = make_entry(1, 0, [0u8; 32]);
        store.put(0, &entry).unwrap();
        assert_eq!(store.size(), 1);
        store.put(4, &entry).unwrap();
        assert_eq!(store.size(), 5);
    }

    #[test]
    fn test_size_no_regression_on_overwrite() {
        let (_dir, store) = open_temp();
        let entry = make_entry(1, 0, [0u8; 32]);
        store.put(0, &entry).unwrap();
        store.put(1, &entry).unwrap();
        assert_eq!(store.size(), 2);
        // Overwriting idx=0 should not shrink size
        store.put(0, &make_entry(99, 0, [0u8; 32])).unwrap();
        assert_eq!(store.size(), 2);
    }

    // -----------------------------------------------------------------------
    // top_hash
    // -----------------------------------------------------------------------

    #[test]
    fn test_top_hash_empty() {
        let (_dir, store) = open_temp();
        assert_eq!(store.top_hash(), [0u8; 32]);
    }

    #[test]
    fn test_top_hash_returns_last_entry_prev_hash() {
        let (_dir, store) = open_temp();
        let prev = [0xABu8; 32];
        let entry0 = make_entry(1, 0, [0u8; 32]);
        let entry1 = make_entry(2, 0, prev);
        store.put(0, &entry0).unwrap();
        store.put(1, &entry1).unwrap();

        // top_hash = prev_hash of the entry at the highest index
        assert_eq!(store.top_hash(), prev);
    }

    // -----------------------------------------------------------------------
    // hash_at
    // -----------------------------------------------------------------------

    #[test]
    fn test_hash_at_deterministic() {
        let (_dir, store) = open_temp();
        let entry = make_entry(7, 3, [0u8; 32]);
        store.put(0, &entry).unwrap();

        let h1 = store.hash_at(0).unwrap();
        let h2 = store.hash_at(0).unwrap();
        assert_eq!(h1, h2);
        assert_ne!(h1, [0u8; 32]);
    }

    #[test]
    fn test_hash_at_missing_returns_error() {
        let (_dir, store) = open_temp();
        assert!(store.hash_at(42).is_err());
    }

    // -----------------------------------------------------------------------
    // rebuild_from_entries
    // -----------------------------------------------------------------------

    #[test]
    fn test_rebuild_clears_and_repopulates() {
        let (_dir, mut store) = open_temp();
        let e0 = make_entry(1, 0, [0u8; 32]);
        store.put(0, &e0).unwrap();
        store.put(1, &e0).unwrap();
        assert_eq!(store.size(), 2);

        let new_entries = vec![(0u64, make_entry(99, 5, [0xFFu8; 32]))];
        store.rebuild_from_entries(new_entries).unwrap();

        assert_eq!(store.size(), 1);
        let got = store.get(0).unwrap().unwrap();
        assert_eq!(got.op_hash, 99);
        assert!(store.get(1).unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // meta helpers (MemKvStore specific)
    // -----------------------------------------------------------------------

    #[cfg(not(feature = "rocksdb-backend"))]
    mod mem_meta {
        use super::*;

        #[test]
        fn test_chkpt_idx_default_zero() {
            let (_dir, store) = open_temp();
            assert_eq!(store.get_chkpt_idx(), 0);
        }

        #[test]
        fn test_chkpt_idx_roundtrip() {
            let (_dir, store) = open_temp();
            store.set_chkpt_idx(42);
            assert_eq!(store.get_chkpt_idx(), 42);
        }

        #[test]
        fn test_chkpt_hash_roundtrip() {
            let (_dir, store) = open_temp();
            let h = [0xDEu8; 32];
            store.set_chkpt_hash(h);
            assert_eq!(store.get_chkpt_hash(), h);
        }
    }
}
