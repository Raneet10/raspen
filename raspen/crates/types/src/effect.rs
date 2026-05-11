// Effect type — actions emitted by replica handlers.
//

use crate::message::{Message, NodeId};

/// An effect produced by a replica handler.
///
/// Handlers return `Vec<Effect>` instead of mutating shared state directly.
/// The caller (driver / network layer) is responsible for executing effects.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Broadcast `message` to all replicas.
    Broadcast(Message),

    /// Send `message` to a specific replica.
    Send { to: NodeId, message: Message },
}
