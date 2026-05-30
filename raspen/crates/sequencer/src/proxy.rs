// Sequencer proxy — assigns ETAs and broadcasts `Speculate` messages.
//
// The sequencer sits outside the consensus protocol:
//   client op → Sequencer::submit → broadcast::Sender<Message::Speculate>
//                                            ↓ (all replicas subscribed)
//                                   Replica::process_message(Speculate { .. })

use tokio::sync::broadcast;
use raspen_types::{Hash, Message};

use crate::eta::EtaAssigner;

/// The sequencer proxy.
///
/// Replicas subscribe to the broadcast channel via `subscribe()`.
/// Clients (or drivers) call `submit(op_hash)` to sequence an operation.
pub struct Sequencer {
    eta: EtaAssigner,
    sender: broadcast::Sender<Message>,
}

impl Sequencer {
    /// Create a new sequencer with a broadcast channel of `capacity` slots.
    ///
    /// `capacity` is the number of buffered messages before slow subscribers
    /// start receiving `RecvError::Lagged`. Choose based on expected burst size.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            eta: EtaAssigner::new(),
            sender,
        }
    }

    /// Subscribe to the broadcast channel.
    ///
    /// Each replica calls this once at startup; the returned `Receiver` yields
    /// every `Message::Speculate` broadcast by `submit`.
    pub fn subscribe(&self) -> broadcast::Receiver<Message> {
        self.sender.subscribe()
    }

    /// Assign a monotonically-increasing η to `op_hash` and broadcast a
    /// `Message::Speculate` to all subscribed replicas.
    ///
    /// Returns the assigned η (ETA).
    ///
    /// Spec: the sequencer is the external entity that triggers fast-path
    /// execution by sending a SPECULATE message with a unique η.
    pub fn submit(&self, op_hash: Hash) -> u64 {
        let eta = self.eta.next();
        // Broadcast to all active subscribers; ignore SendError (no receivers).
        let _ = self.sender.send(Message::Speculate {
            from_seq: "sequencer".to_string(),
            round: 0,
            op_hash,
            eta,
        });
        eta
    }
}

impl std::fmt::Debug for Sequencer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sequencer")
            .field("eta", &self.eta)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that `submit` assigns strictly monotonically increasing ETAs.
    ///
    /// Integration note: `submit` is the real entry point — it both assigns the
    /// ETA *and* sends a real `Message::Speculate` through the broadcast channel.
    /// The subscriber `rx` receives actual messages, demonstrating end-to-end flow.
    #[tokio::test]
    async fn test_submit_increments_eta_monotonically() {
        let seq = Sequencer::new(16);
        let mut rx = seq.subscribe();

        let op1 = [0x01u8; 32];
        let op2 = [0x02u8; 32];

        let eta1 = seq.submit(op1);
        let eta2 = seq.submit(op2);

        // ETA must increment by exactly 1
        assert_eq!(eta2, eta1 + 1, "second ETA must equal first ETA + 1");

        // Verify the broadcast messages arrive with the correct ETAs
        let msg1 = rx.recv().await.expect("should receive first message");
        let msg2 = rx.recv().await.expect("should receive second message");

        match msg1 {
            Message::Speculate { eta, .. } => assert_eq!(eta, eta1),
            other => panic!("expected Speculate, got {other:?}"),
        }
        match msg2 {
            Message::Speculate { eta, .. } => assert_eq!(eta, eta2),
            other => panic!("expected Speculate, got {other:?}"),
        }
    }

    /// Verify that a subscriber created *before* submit receives the message.
    #[tokio::test]
    async fn test_subscribe_receives_speculate() {
        let seq = Sequencer::new(8);
        let mut rx = seq.subscribe();

        let eta = seq.submit([0xAAu8; 32]);

        let msg = rx.recv().await.unwrap();
        assert!(matches!(msg, Message::Speculate { .. }));
        if let Message::Speculate { eta: received_eta, op_hash, .. } = msg {
            assert_eq!(received_eta, eta);
            assert_eq!(op_hash, [0xAAu8; 32]);
        }
    }
}
