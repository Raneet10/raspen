// ETA (η) assigner — monotonically increasing sequence number counter.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Monotonically-increasing ETA counter.
///
#[derive(Debug, Clone)]
pub struct EtaAssigner(Arc<AtomicU64>);

impl EtaAssigner {
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    /// Atomically fetch the current value and increment it.
    ///
    /// Returns the value *before* the increment — i.e., the η assigned to the
    /// current operation. `Relaxed` is sufficient: `fetch_add` atomicity alone
    /// guarantees each caller receives a unique, monotonically-increasing value.
    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for EtaAssigner {
    fn default() -> Self {
        Self::new()
    }
}
