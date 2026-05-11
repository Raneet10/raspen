// Async actor wrapper around a StateMachine.



use tokio::{sync::mpsc, task};

use raspen_types::{Event, Message};

use crate::state_machine::StateMachine;
use crate::transport::Transport;

enum ReplicaInput {
    Message(Message),
    Event(Event),
}

#[derive(Clone, Debug)]
pub struct Node {
    tx: mpsc::Sender<ReplicaInput>,
}

impl Node {
    pub fn start<S: StateMachine, T: Transport>(
        state_machine: S,
        transport: T,
        capacity: usize,
    ) -> anyhow::Result<Self> {
        let (input_tx, mut input_rx) = mpsc::channel::<ReplicaInput>(capacity);

        let join_handle = task::spawn_blocking(move || {
            let mut sm = state_machine;
            loop {
                let Some(input) = input_rx.blocking_recv() else {
                    tracing::debug!("replica input channel closed — shutting down");
                    break;
                };

                let effects = match input {
                    ReplicaInput::Message(msg) => sm.process_message(msg),
                    ReplicaInput::Event(event) => sm.handle_event(event),
                };

                if !effects.is_empty() {
                    transport.execute(effects);
                }
            }
        });

        tokio::spawn(async move {
            if let Err(e) = join_handle.await {
                tracing::error!("replica actor panicked: {e:?}");
            }
        });

        Ok(Self { tx: input_tx })
    }

    pub async fn submit_message(&self, msg: Message) -> anyhow::Result<()> {
        self.tx
            .send(ReplicaInput::Message(msg))
            .await
            .map_err(|_| anyhow::anyhow!("replica actor is no longer running"))
    }

    pub async fn submit_event(&self, event: Event) -> anyhow::Result<()> {
        self.tx
            .send(ReplicaInput::Event(event))
            .await
            .map_err(|_| anyhow::anyhow!("replica actor is no longer running"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    use raspen_types::{config::Config, effect::Effect, message::Message, NodeId};
    use tokio::sync::Notify;

    use crate::replica::Replica;
    use crate::transport::Transport;

    // Records whole effect batches atomically and signals a Notify after each
    // batch so tests can await without arbitrary sleeps.
    #[derive(Clone)]
    struct RecordingTransport {
        effects: Arc<Mutex<Vec<Effect>>>,
        notify: Arc<Notify>,
    }

    impl RecordingTransport {
        fn new() -> Self {
            Self { effects: Arc::default(), notify: Arc::new(Notify::new()) }
        }

        async fn recv(&self) -> Vec<Effect> {
            self.notify.notified().await;
            self.effects.lock().unwrap().drain(..).collect()
        }
    }

    impl Transport for RecordingTransport {
        fn broadcast(&self, msg: Message) {
            self.effects.lock().unwrap().push(Effect::Broadcast(msg));
        }
        fn send(&self, to: NodeId, message: Message) {
            self.effects.lock().unwrap().push(Effect::Send { to, message });
        }
        fn execute(&self, batch: Vec<Effect>) {
            self.effects.lock().unwrap().extend(batch);
            self.notify.notify_one();
        }
    }

    fn make_node(capacity: usize) -> (TempDir, Node, RecordingTransport) {
        let tmp = TempDir::new().unwrap();
        let config = Config::for_test(6, 1, 1);
        let replica = Replica::new(config, "n0".to_string(), tmp.path()).unwrap();
        let transport = RecordingTransport::new();
        let node = Node::start(replica, transport.clone(), capacity).unwrap();
        (tmp, node, transport)
    }

    #[tokio::test]
    async fn test_submit_message_speculate() {
        let (_tmp, node, transport) = make_node(16);

        node.submit_message(Message::Speculate {
            eta: 0,
            op_hash: [0u8; 32],
            from_seq: "seq".to_string(),
            round: 0,
        })
        .await
        .unwrap();

        let effects = transport.recv().await;

        let has_spec_reply = effects
            .iter()
            .any(|e| matches!(e, Effect::Broadcast(Message::SpecReply { log_idx, .. }) if *log_idx == 0));
        assert!(has_spec_reply, "expected SpecReply; got: {effects:?}");

        let has_sync = effects
            .iter()
            .any(|e| matches!(e, Effect::Broadcast(Message::Sync { .. })));
        assert!(has_sync, "expected Sync (threshold branch); got: {effects:?}");
    }

    #[tokio::test]
    async fn test_submit_event_sync_timeout() {
        let (_tmp, node, transport) = make_node(16);

        node.submit_message(Message::Speculate {
            eta: 0,
            op_hash: [0u8; 32],
            from_seq: "seq".to_string(),
            round: 0,
        })
        .await
        .unwrap();
        let _ = transport.recv().await;

        node.submit_event(Event::SyncTimeout).await.unwrap();

        let effects = transport.recv().await;
        let has_sync = effects
            .iter()
            .any(|e| matches!(e, Effect::Broadcast(Message::Sync { .. })));
        assert!(has_sync, "expected Sync broadcast from SyncTimeout; got: {effects:?}");
    }

    #[tokio::test]
    async fn test_drop_node_shuts_down() {
        let (_tmp, node, _transport) = make_node(16);
        drop(node);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}