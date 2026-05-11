// raspen-consensus: core Aspen BFT consensus logic.
//
// The `Replica` struct in `replica.rs` owns all per-replica state and
// dispatches messages/events to handler modules below.

pub mod replica;
pub mod node;
pub mod state_machine;
pub mod transport;

// Handler modules implementing each sub-protocol transition.
mod alignment;
pub(crate) mod checkpoint;
mod fast_path;
mod repair;
mod repair_entry;

pub use replica::Replica;
pub use node::Node;
pub use state_machine::StateMachine;
pub use transport::Transport;
