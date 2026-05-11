// raspen-types: all protocol-level type definitions
//
// Dependency order: this crate has NO dependency on other raspen-* crates.
// All other raspen crates depend on this one.

pub mod config;
pub mod effect;
pub mod event;
pub mod message;
pub mod phase;
pub mod state;

// Convenience re-exports of the most commonly used types
pub use config::Config;
pub use effect::Effect;
pub use event::Event;
pub use message::{Eta, Hash, Message, NodeId};
pub use phase::Phase;
pub use state::{KvEntry, KvStoreOps, ReplicaState};
