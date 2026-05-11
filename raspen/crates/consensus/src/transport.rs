// Transport trait — abstracts effect execution for the state machine actor.

use raspen_types::{Effect, Message, NodeId};

pub trait Transport: Send + 'static {
    fn broadcast(&self, msg: Message);
    fn send(&self, to: NodeId, msg: Message);

    fn execute(&self, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Broadcast(msg) => self.broadcast(msg),
                Effect::Send { to, message } => self.send(to, message),
                _ => {}
            }
        }
    }
}
