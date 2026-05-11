// Core replica object for the Aspen BFT protocol.
//
// `Replica` owns all per-replica mutable state and dispatches inbound messages
// and timer events to the handler modules: fast_path, checkpoint, alignment,
// repair_entry, and repair.
//
// `is_leader` implements the round-robin leader election used by the REPAIR
// sub-protocol: the leader for a given view is NODES_ORDERED[view mod n].

use std::collections::HashSet;
use std::path::Path;

use raspen_types::{Config, Effect, Event, Message, NodeId, Phase, ReplicaState};
use raspen_storage::KvStore;

/// The core replica object.
///
/// Owns the full `ReplicaState` (including the `KvStore` handle) and the
/// read-only `Config`. Handler functions in `fast_path.rs`, `checkpoint.rs`,
/// etc. receive `&mut Replica` and return `Vec<Effect>`.
pub struct Replica {
    pub state: ReplicaState,
    pub config: Config,
}

impl Replica {
    /// Construct a new replica in its initial state.
    ///
    /// A replica starts in Speculative phase with an empty log, checkpoint
    /// index 0, view 0, round 0, and `in_repair` false.
    pub fn new(
        config: Config,
        node_id: NodeId,
        db_path: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        let kv_store = KvStore::open(db_path)?;

        let state = ReplicaState {
            node_id,
            phase: Phase::Speculative,
            round: 0,
            view: 0,
            start_idx: 0,
            kv_size: 0,
            kv_store: Box::new(kv_store),
            chkpt_idx: 0,
            chkpt_hash: [0u8; 32],
            eta_star: 0,
            in_repair: false,
            proposed_history_hash: 0,
            // Message sets — all empty on init
            sync_msgs: HashSet::new(),
            ack_msgs: HashSet::new(),
            chkpt_msgs: HashSet::new(),
            timeout_msgs: HashSet::new(),
            log_msgs: HashSet::new(),
            repair_history_msgs: HashSet::new(),
            repair_prepare_msgs: HashSet::new(),
            repair_commit_msgs: HashSet::new(),
            repair_done_msgs: HashSet::new(),
        };

        Ok(Self { state, config })
    }

    /// Dispatch an inbound message to the appropriate handler.
    ///
    /// Tries each protocol guard in order; for every guard that fires, invokes
    /// the corresponding transition and collects its effects.
    ///
    /// The returned effects **must** be executed by the network layer; dropping
    /// them silently causes the protocol to stall.
    #[must_use]
    pub fn process_message(&mut self, msg: Message) -> Vec<Effect> {
        let mut effects = Vec::new();

        // Fast-path speculative execution: append entry and broadcast SPEC-REPLY.
        if let Some(log_idx) = crate::fast_path::can_speculate(self, &msg) {
            effects.extend(crate::fast_path::handle_speculate(self, log_idx));
        }

        // Checkpoint sync threshold: send SYNC when kv_size grows past chkpt_idx.
        if let Some(log_idx) = crate::checkpoint::can_send_sync_threshold(self) {
            effects.extend(crate::checkpoint::handle_send_sync(self, log_idx));
        }

        // Checkpoint formation: store incoming SYNC and check whether
        //    (n − p) consistent SYNCs have now been collected.
        effects.extend(crate::checkpoint::on_receive_sync(self, &msg));

        // Alignment entry: store incoming CHECKPOINT and check whether
        //    f+1 consistent CHECKPOINTs have now been seen.
        effects.extend(crate::alignment::on_receive_checkpoint(self, &msg));

        // Conflict proof: check whether a checkpoint is provably impossible
        //    after accumulating SYNC messages.
        if let Some(log_idx) = crate::repair_entry::can_conflict_proof(self) {
            effects.extend(crate::repair_entry::handle_conflict_proof(self, log_idx));
        }

        // chkpt_timeout is event-only — handled in handle_event.

        // Timeout proof: store incoming TIMEOUT and check whether f+1 have
        //    been collected.
        effects.extend(crate::repair_entry::on_receive_timeout(self, &msg));

        // Enter repair: fires when a TIMEOUT-PROOF or CONFLICT-PROOF arrives
        //    and the replica is not yet in REPAIR.
        if let Some(round) = crate::repair_entry::can_enter_repair(self, &msg) {
            effects.extend(crate::repair_entry::handle_enter_repair(self, round));
        }

        // State request: reply with our checkpoint state to any STATE-REQUEST.
        if let Some(requester) = crate::alignment::can_state_request(&msg) {
            effects.extend(crate::alignment::handle_state_request(self, requester));
        }

        // State reply: rebuild log from recovered checkpoint when Aligning
        //     and a STATE-REPLY arrives.
        if let Some(chkpt_idx) = crate::alignment::can_state_reply(self, &msg) {
            effects.extend(crate::alignment::handle_state_reply(self, chkpt_idx));
        }

        // Broadcast history: leader collects (n − f) LOGs and proposes a
        //     REPAIR-HISTORY.
        effects.extend(crate::repair::on_receive_log_msg(self, &msg));

        // Repair prepare: vote for the proposed history when a REPAIR-HISTORY
        //     arrives in CollectingLogs or PreparePhase.
        effects.extend(crate::repair::on_receive_repair_history(self, &msg));

        // Repair commit: broadcast REPAIR-COMMIT when (n − f) REPAIR-PREPARE
        //     votes have been collected in PreparePhase.
        effects.extend(crate::repair::on_receive_repair_prepare(self, &msg));

        // Apply repair: apply the merged history when (n − f) REPAIR-COMMIT
        //     votes have been collected in CommitPhase.
        effects.extend(crate::repair::on_receive_repair_commit(self, &msg));

        // Repair done catchup: a lagging replica exits REPAIR when it sees
        //     f+1 REPAIR-DONE messages and already holds the REPAIR-HISTORY.
        effects.extend(crate::repair::on_receive_repair_done(self, &msg));

        // view_change is event-only — handled in handle_event.

        effects
    }

    /// Dispatch a timer event to the appropriate handler.
    ///
    /// Handles the three timer events used by the protocol:
    /// - `SyncTimeout` → broadcast SYNC for the current log top.
    /// - `ChkptTimeout { log_idx }` → broadcast TIMEOUT to signal checkpoint failure.
    /// - `RepairViewTimeout` → increment view and re-broadcast LOG to the new leader.
    ///
    /// The returned effects **must** be executed by the network layer; dropping
    /// them silently causes the protocol to stall.
    #[must_use]
    pub fn handle_event(&mut self, event: Event) -> Vec<Effect> {
        let mut effects = Vec::new();

        // SyncTimeout → broadcast SYNC for the current log top.
        if let Some(log_idx) = crate::checkpoint::can_send_sync_on_event(self, &event) {
            effects.extend(crate::checkpoint::handle_send_sync(self, log_idx));
        }

        // ChkptTimeout → broadcast TIMEOUT to signal that checkpoint formation has stalled.
        if let Some(log_idx) = crate::repair_entry::can_chkpt_timeout(self, &event) {
            effects.extend(crate::repair_entry::handle_chkpt_timeout(self, log_idx));
        }

        // RepairViewTimeout → increment view and re-broadcast LOG to the new leader.
        if let Some(new_view) = crate::repair::can_view_change(self, &event) {
            effects.extend(crate::repair::handle_view_change(self, new_view));
        }

        effects
    }

    /// Return true if this replica is the current repair-view leader.
    ///
    /// The leader is selected by round-robin from an ordered list of replicas:
    /// `NODES_ORDERED[view mod n]`.  Leadership is internal to the REPAIR
    /// sub-protocol and changes each time the view increments.
    pub fn is_leader(&self) -> bool {
        let n = self.config.nodes_ordered.len();
        if n == 0 {
            return false;
        }
        self.config.nodes_ordered[self.state.view as usize % n] == self.state.node_id
    }
}

impl std::fmt::Debug for Replica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replica")
            .field("state", &self.state)
            .field("config", &self.config)
            .finish()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use raspen_types::{config::Config, effect::Effect, message::Message, phase::Phase};
    use tempfile::TempDir;

    fn make_replica() -> (TempDir, Replica) {
        let tmp = TempDir::new().unwrap();
        let config = Config::for_test(6, 1, 1);
        let replica = Replica::new(config, "n0".to_string(), tmp.path()).unwrap();
        (tmp, replica)
    }

    /// Replica starts in Speculative phase and the first Speculate message
    /// produces a SpecReply broadcast with log_idx = 0, plus a Sync broadcast
    /// (sync threshold fires: kv_size=1 > chkpt_idx=0).
    ///
    /// Integration verified: replica.rs::process_message calls
    /// fast_path::can_speculate + fast_path::handle_speculate when a
    /// Message::Speculate arrives and phase == Speculative.
    /// After speculate, checkpoint::can_send_sync_threshold fires.
    #[test]
    fn test_speculate_produces_spec_reply() {
        let (_tmp, mut replica) = make_replica();

        let msg = Message::Speculate {
            eta: 0,
            op_hash: [0u8; 32],
            from_seq: "seq".to_string(),
            round: 0,
        };

        let effects = replica.process_message(msg);

        // After speculate: SpecReply + Sync (threshold branch)
        let has_spec_reply = effects.iter().any(|e| {
            matches!(e, Effect::Broadcast(Message::SpecReply { log_idx, from, .. })
                if *log_idx == 0 && from == "n0")
        });
        assert!(has_spec_reply, "expected SpecReply in effects; got: {effects:?}");

        let has_sync = effects.iter().any(|e| {
            matches!(e, Effect::Broadcast(Message::Sync { log_idx, .. }) if *log_idx == 0)
        });
        assert!(has_sync, "expected Sync in effects (threshold branch); got: {effects:?}");

        assert_eq!(replica.state.kv_size, 1);
        assert_eq!(replica.state.kv_store.size(), 1);
    }

    /// A second Speculate uses log_idx = 1 (monotonically increasing).
    /// After each speculate the sync threshold also fires (kv_size > chkpt_idx=0).
    #[test]
    fn test_second_speculate_uses_log_idx_1() {
        let (_tmp, mut replica) = make_replica();

        let msg1 = Message::Speculate {
            eta: 0,
            op_hash: [0u8; 32],
            from_seq: "seq".to_string(),
            round: 0,
        };
        let _ = replica.process_message(msg1);

        let msg2 = Message::Speculate {
            eta: 1,
            op_hash: [1u8; 32],
            from_seq: "seq".to_string(),
            round: 0,
        };
        let effects2 = replica.process_message(msg2);

        // Must contain SpecReply with log_idx = 1
        let has_spec_reply = effects2.iter().any(|e| {
            matches!(e, Effect::Broadcast(Message::SpecReply { log_idx, .. }) if *log_idx == 1)
        });
        assert!(has_spec_reply, "expected SpecReply(log_idx=1) in effects; got: {effects2:?}");

        assert_eq!(replica.state.kv_size, 2);
    }

    /// When phase is not Speculative, process_message returns no effects.
    #[test]
    fn test_speculate_ignored_when_not_speculative() {
        let (_tmp, mut replica) = make_replica();
        replica.state.phase = Phase::Aligning;

        let msg = Message::Speculate {
            eta: 0,
            op_hash: [0u8; 32],
            from_seq: "seq".to_string(),
            round: 0,
        };

        let effects = replica.process_message(msg);
        assert!(effects.is_empty(), "expected no effects when not Speculative");
        assert_eq!(replica.state.kv_size, 0);
    }
}