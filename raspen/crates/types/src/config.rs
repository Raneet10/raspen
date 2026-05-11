// Static protocol configuration: replica count, fault tolerance parameters,
// and derived quorum thresholds.

use crate::message::NodeId;

/// Static protocol configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// `n`: total number of replicas.
    pub n: usize,

    /// `f`: maximum number of Byzantine faults tolerated.
    pub f: usize,

    /// `p`: maximum number of diverged replicas tolerated on the fast path.
    pub p: usize,

    /// `n - p`: minimum matching replies needed for a client to commit speculatively.
    pub fast_quorum: usize,

    /// `n - f`: minimum agreeing replicas needed for a BFT decision.
    pub byz_quorum: usize,

    /// `f + 1`: minimum to rule out all-Byzantine explanations.
    pub f_plus_1: usize,

    /// Ordered replica list used for round-robin leader election during REPAIR.
    /// Leader for view `v` = `nodes_ordered[v mod n]`.
    pub nodes_ordered: Vec<NodeId>,
}

impl Config {
    /// Construct a `Config` from raw `(n, f, p)` parameters.
    ///
    /// `nodes_ordered` defaults to `["r0", "r1", ..., "r{n-1}"]`.
    pub fn for_test(n: usize, f: usize, p: usize) -> Self {
        let nodes_ordered = (0..n).map(|i| format!("r{}", i)).collect();
        Self {
            n,
            f,
            p,
            fast_quorum: n - p,
            byz_quorum: n - f,
            f_plus_1: f + 1,
            nodes_ordered,
        }
    }

    /// Canonical configuration: n=6, f=1, p=1.
    pub fn spec_default() -> Self {
        Self::for_test(6, 1, 1)
    }
}

impl Default for Config {
    /// Returns the canonical spec configuration: n=6, f=1, p=1.
    fn default() -> Self {
        Self::spec_default()
    }
}
