// Per-replica data types: the log entry format and the full mutable replica state.

use std::collections::HashSet;

use crate::message::{Hash, Message, NodeId};
use crate::phase::Phase;

// ---------------------------------------------------------------------------
// KvEntry
// ---------------------------------------------------------------------------

/// One entry in the replicated log.
///
/// Each entry records the operation hash, the execution result, and the hash of the
/// previous entry so that the log forms a verifiable hash chain.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct KvEntry {
    pub op_hash: u64,
    pub result: Vec<u8>,
    /// SHA-256 of the previous entry (genesis entry uses [0u8; 32]).
    pub prev_hash: Hash,
}

// ---------------------------------------------------------------------------
// KvStoreOps trait  (abstraction layer so types crate does not depend on storage)
// ---------------------------------------------------------------------------

/// Trait abstracting the underlying key-value store.
///
/// `storage::KvStore` implements this trait. `ReplicaState.kv_store` holds a
/// `Box<dyn KvStoreOps>` so the `types` crate remains free of a dependency on
/// the `storage` crate (which itself depends on `types` for `KvEntry`).
pub trait KvStoreOps: Send + Sync + std::fmt::Debug {
    fn get(&self, idx: u64) -> anyhow::Result<Option<KvEntry>>;
    fn put(&self, idx: u64, entry: &KvEntry) -> anyhow::Result<()>;
    fn size(&self) -> u64;
    fn top_hash(&self) -> Hash;
    fn hash_at(&self, idx: u64) -> anyhow::Result<Hash>;
    fn rebuild_from_entries(&mut self, entries: Vec<(u64, KvEntry)>) -> anyhow::Result<()>;
}

// ---------------------------------------------------------------------------
// ReplicaState
// ---------------------------------------------------------------------------

/// All per-replica mutable state fields.
///
/// Holds the protocol phase, log metadata, checkpoint state, and one
/// `HashSet<Message>` per message category for deduplication tracking.
pub struct ReplicaState {
    /// Identity of this replica.
    pub node_id: NodeId,

    /// Current protocol phase.
    pub phase: Phase,

    /// Current Aspen round.
    pub round: u64,

    /// Internal REPAIR view; leader = nodes_ordered[view mod n].
    pub view: u64,

    /// First uncommitted log index this round.
    pub start_idx: u64,

    /// Number of log entries held by this replica.
    pub kv_size: u64,

    /// Replicated log store.
    pub kv_store: Box<dyn KvStoreOps>,

    /// Index of the latest committed checkpoint.
    pub chkpt_idx: u64,

    /// Hash of the latest committed checkpoint.
    pub chkpt_hash: Hash,

    /// Largest sequence number (η*) seen this round.
    pub eta_star: u64,

    /// True while the REPAIR sub-protocol is active.
    pub in_repair: bool,

    /// History hash being voted on during REPAIR.
    pub proposed_history_hash: u64,

    // ------------------------------------------------------------------
    // Message collection sets  (for deduplication, one per category)
    // ------------------------------------------------------------------

    /// Received SYNC messages. Used to form checkpoints.
    pub sync_msgs: HashSet<Message>,

    /// Received ACK / SPEC-REPLY messages.
    pub ack_msgs: HashSet<Message>,

    /// Received CHECKPOINT messages.
    pub chkpt_msgs: HashSet<Message>,

    /// Received TIMEOUT messages (used for TimeoutProof formation).
    pub timeout_msgs: HashSet<Message>,

    /// Received LOG messages (repair leader collects these).
    pub log_msgs: HashSet<Message>,

    /// Received REPAIR-HISTORY proposals.
    pub repair_history_msgs: HashSet<Message>,

    /// Received REPAIR-PREPARE votes.
    pub repair_prepare_msgs: HashSet<Message>,

    /// Received REPAIR-COMMIT votes.
    pub repair_commit_msgs: HashSet<Message>,

    /// Received REPAIR-DONE notifications.
    pub repair_done_msgs: HashSet<Message>,
}

impl std::fmt::Debug for ReplicaState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaState")
            .field("node_id", &self.node_id)
            .field("phase", &self.phase)
            .field("round", &self.round)
            .field("view", &self.view)
            .field("start_idx", &self.start_idx)
            .field("kv_size", &self.kv_size)
            .field("chkpt_idx", &self.chkpt_idx)
            .field("chkpt_hash", &self.chkpt_hash)
            .field("eta_star", &self.eta_star)
            .field("in_repair", &self.in_repair)
            .field("proposed_history_hash", &self.proposed_history_hash)
            .finish_non_exhaustive()
    }
}
