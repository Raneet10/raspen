// All Aspen protocol message types.
//
// There are 14 wire-level message variants covering the fast path, checkpoint,
// alignment, repair entry, and repair agreement sub-protocols. One extra variant
// — `Speculate` — is added for the sequencer-to-replica trigger that initiates
// speculative execution (the sequencer is modeled as an external oracle in the
// formal spec; in Rust it sends a first-class message).

/// SHA-256 digest: 32 bytes, used as a compact log-chain hash.
pub type Hash = [u8; 32];

/// Node identifier — corresponds to `Node = str` in the spec.
pub type NodeId = String;

/// Sequence number assigned by the sequencer (η in the spec).
pub type Eta = u64;

/// All Aspen protocol messages.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Message {

    /// Sequencer broadcasts this to trigger speculative execution on all replicas.
    /// Carries the submitting replica, the current round, the operation hash,
    /// and the assigned sequence number (η).
    Speculate {
        from_seq: NodeId,
        round: u64,
        op_hash: Hash,
        eta: Eta,
    },


    /// Replica → client: speculative reply after executing the entry at `log_idx`.
    SpecReply {
        from: NodeId,
        round: u64,
        log_idx: u64,
        log_hash: Hash,
    },


    /// Replica broadcasts its current log tip so peers can form a checkpoint.
    Sync {
        from: NodeId,
        round: u64,
        log_idx: u64,
        log_hash: Hash,
        eta_star: u64,
    },

    /// Formed and broadcast when a fast-quorum of matching SYNC messages have been collected.
    Checkpoint {
        from: NodeId,
        round: u64,
        log_idx: u64,
        log_hash: Hash,
    },


    /// Diverged replica requests state transfer from peers that hold a valid checkpoint.
    StateRequest {
        from: NodeId,
        chkpt_idx: u64,
    },

    /// Checkpoint holder replies with the hash of the requested checkpoint.
    StateReply {
        from: NodeId,
        chkpt_idx: u64,
        chkpt_hash: Hash,
    },


    /// Replica broadcasts a timeout vote for `log_idx` after its checkpoint timer fires.
    TimeoutMsg {
        from: NodeId,
        round: u64,
        log_idx: u64,
    },

    /// Aggregated proof that f+1 replicas timed out for `log_idx`.
    TimeoutProof {
        round: u64,
        log_idx: u64,
    },

    /// Proof that two replicas have conflicting logs at `log_idx`.
    ConflictProof {
        round: u64,
        log_idx: u64,
    },


    /// Replica sends its log summary to the repair leader so it can propose a merged history.
    LogMsg {
        from: NodeId,
        view: u64,
        round: u64,
        log_hash: Hash,
    },

    /// Repair leader proposes a history hash for agreement.
    RepairHistory {
        from: NodeId,
        view: u64,
        round: u64,
        history_hash: u64,
    },

    /// PBFT PREPARE vote for the proposed history hash.
    RepairPrepare {
        from: NodeId,
        view: u64,
        round: u64,
        history_hash: u64,
    },

    /// PBFT COMMIT vote after seeing a prepare quorum.
    RepairCommit {
        from: NodeId,
        view: u64,
        round: u64,
        history_hash: u64,
    },

    /// Repair complete: replica notifies peers that it has applied the agreed history.
    RepairDone {
        from: NodeId,
        view: u64,
        round: u64,
        history_hash: u64,
    },

    /// Reply to a client after a committed (non-speculative) execution.
    CommittedReply {
        from: NodeId,
        round: u64,
        log_idx: u64,
    },
}
