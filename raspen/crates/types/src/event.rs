// External timer events delivered to the replica.
//
// Three timer types drive sub-protocol transitions: a periodic sync timer,
// a per-entry checkpoint deadline, and a REPAIR view-change timer.

/// External timer events delivered to the replica.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Event {
    /// Periodic sync timer fired: replica should broadcast a SYNC for its current log tip.
    SyncTimeout,

    /// Checkpoint deadline for `log_idx` expired: replica should send a TIMEOUT vote.
    ChkptTimeout { log_idx: u64 },

    /// REPAIR view timer fired: the current repair leader has not proposed in time;
    /// trigger a view change.
    RepairViewTimeout,
}
