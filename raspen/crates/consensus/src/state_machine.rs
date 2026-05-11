// StateMachine trait — the synchronous protocol state machine interface.
//
// Node is generic over any StateMachine implementation, which lets test
// doubles be substituted without touching the actor/channel logic in node.rs.

use raspen_types::{Effect, Event, Message};

use crate::replica::Replica;

pub trait StateMachine: Send + 'static {
    #[must_use]
    fn process_message(&mut self, msg: Message) -> Vec<Effect>;
    #[must_use]
    fn handle_event(&mut self, event: Event) -> Vec<Effect>;
}

impl StateMachine for Replica {
    fn process_message(&mut self, msg: Message) -> Vec<Effect> {
        Replica::process_message(self, msg)
    }

    fn handle_event(&mut self, event: Event) -> Vec<Effect> {
        Replica::handle_event(self, event)
    }
}