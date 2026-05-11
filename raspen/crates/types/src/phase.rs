// Protocol phase tracked per replica.
//
// A replica's phase determines which transitions are active. It starts in
// Speculative and moves to Aligning (on log divergence) or into the REPAIR
// state machine (on checkpoint failure).

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Phase {
    /// Normal operation: speculatively executing requests (fast path)
    Speculative,
    /// ALIGN sub-protocol: requesting state transfer from checkpoint holders
    Aligning,
    /// REPAIR sub-protocol: collecting LOG messages before proposing history
    CollectingLogs,
    /// REPAIR sub-protocol: PBFT prepare phase (broadcasting REPAIR-PREPARE)
    PreparePhase,
    /// REPAIR sub-protocol: PBFT commit phase (broadcasting REPAIR-COMMIT)
    CommitPhase,
}
