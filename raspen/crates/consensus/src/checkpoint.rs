// Checkpoint sub-protocol.
//
// send_sync: Periodically (on a timer or when the log grows past the last
//   checkpoint) the replica broadcasts a SYNC message carrying its current log
//   index and top hash.
//
// form_checkpoint: Once (n − p) SYNC messages with matching (round, log_idx,
//   log_hash) have been collected, the log is provably consistent up to that
//   index.  The replica records the checkpoint and broadcasts a CHECKPOINT
//   message so peers can detect divergence.
//
// Entry points:
//   - Replica::handle_event (replica.rs) calls can_send_sync_on_event + handle_send_sync
//     when Event::SyncTimeout fires.
//   - Replica::process_message (replica.rs) calls can_send_sync_threshold + handle_send_sync
//     after any message that may grow kv_size above chkpt_idx.
//   - Replica::process_message (replica.rs) calls on_receive_sync which stores Sync messages
//     and checks can_form_checkpoint after each one.

use raspen_types::{Effect, Event, Message, Phase};

use crate::replica::Replica;


/// Returns `Some(log_idx)` when a SyncTimeout fires and the log is non-empty.
///
/// When the sync timer expires the replica broadcasts a SYNC for its current
/// highest log index (`kv_size − 1`).  Only fires in Speculative phase because
/// SYNC messages are part of the fast-path checkpoint protocol.
///
pub(crate) fn can_send_sync_on_event(replica: &Replica, event: &Event) -> Option<u64> {
    if !matches!(event, Event::SyncTimeout) {
        return None;
    }
    if replica.state.phase != Phase::Speculative {
        return None;
    }
    if replica.state.kv_size == 0 {
        return None;
    }
    Some(replica.state.kv_size - 1)
}

/// Returns `Some(log_idx)` when the log has grown past the last checkpoint index.
///
/// After each new log entry the replica checks whether `kv_size` now exceeds
/// `chkpt_idx`.  If so it broadcasts a SYNC for the new log top, allowing peers
/// to quickly form checkpoints without waiting for the timer.
///
/// Integration: called from Replica::process_message in replica.rs after
/// handle_speculate may have grown kv_size.
pub(crate) fn can_send_sync_threshold(replica: &Replica) -> Option<u64> {
    if replica.state.phase != Phase::Speculative {
        return None;
    }
    if replica.state.kv_size == 0 {
        return None;
    }
    if replica.state.kv_size <= replica.state.chkpt_idx {
        return None;
    }
    Some(replica.state.kv_size - 1)
}


/// Broadcast a SYNC message for the given log index. No state change.
///
/// The SYNC carries the log index, the current top-of-log hash, and
/// the largest ETA this replica has seen so far.  State is not modified;
/// only the network effect is returned.
pub(crate) fn handle_send_sync(replica: &Replica, log_idx: u64) -> Vec<Effect> {
    let log_hash = replica.state.kv_store.top_hash();

    vec![Effect::Broadcast(Message::Sync {
        from: replica.state.node_id.clone(),
        round: replica.state.round,
        log_idx,
        log_hash,
        eta_star: replica.state.eta_star,
    })]
}


/// Returns `Some(log_idx)` when a fast-path quorum of consistent SYNCs has been seen.
///
/// Counts SYNC messages where `round`, `log_idx`, and `log_hash` all match the
/// replica's own current values.  Once (n − p) such messages have been collected
/// the log is considered committed up to that index.
pub(crate) fn can_form_checkpoint(replica: &Replica) -> Option<u64> {
    if replica.state.phase != Phase::Speculative {
        return None;
    }
    if replica.state.kv_size == 0 {
        return None;
    }
    let log_idx = replica.state.kv_size - 1;
    let expected_hash = replica.state.kv_store.top_hash();

    let quorum_count = replica.state.sync_msgs.iter().filter(|m| {
        matches!(m,
            Message::Sync { round, log_idx: li, log_hash: lh, .. }
            if *round == replica.state.round
                && *li == log_idx
                && *lh == expected_hash
        )
    }).count();

    if quorum_count >= replica.config.fast_quorum {
        Some(log_idx)
    } else {
        None
    }
}


/// Record the checkpoint and broadcast a CHECKPOINT message.
///
/// Updates `chkpt_idx` and `chkpt_hash` to the new checkpoint values, then
/// broadcasts a CHECKPOINT so other replicas can detect whether they have
/// diverged (which would trigger the alignment sub-protocol).
pub(crate) fn handle_form_checkpoint(replica: &mut Replica, log_idx: u64) -> Vec<Effect> {
    let log_hash = replica.state.kv_store.top_hash();

    replica.state.chkpt_idx = log_idx;
    replica.state.chkpt_hash = log_hash;

    vec![Effect::Broadcast(Message::Checkpoint {
        from: replica.state.node_id.clone(),
        round: replica.state.round,
        log_idx,
        log_hash,
    })]
}


/// Process an incoming SYNC: store it, then check whether the checkpoint quorum is met.
///
/// Each SYNC is stored in `sync_msgs` (HashSet deduplicates by sender).  After
/// storing, the checkpoint guard is re-evaluated; if (n − p) consistent SYNCs
/// are now available the checkpoint transition fires.
///
/// Integration: called from Replica::process_message in replica.rs.
pub(crate) fn on_receive_sync(replica: &mut Replica, msg: &Message) -> Vec<Effect> {
    if matches!(msg, Message::Sync { .. }) {
        replica.state.sync_msgs.insert(msg.clone());
    }

    if let Some(log_idx) = can_form_checkpoint(replica) {
        handle_form_checkpoint(replica, log_idx)
    } else {
        vec![]
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use raspen_types::{config::Config, effect::Effect, message::Message};
    use tempfile::TempDir;

    use crate::replica::Replica;

    fn make_replica() -> (TempDir, Replica) {
        let tmp = TempDir::new().unwrap();
        // 6 nodes, fast_quorum=5, slow_quorum=4
        let config = Config::for_test(6, 1, 1);
        let replica = Replica::new(config, "n0".to_string(), tmp.path()).unwrap();
        (tmp, replica)
    }

    /// Helper: speculate once so kv_size becomes 1
    fn do_speculate(replica: &mut Replica) {
        crate::fast_path::handle_speculate(replica, 0);
    }


    #[test]
    fn test_sync_on_event() {
        let (_tmp, mut replica) = make_replica();
        // First speculate so kv_size > 0
        do_speculate(&mut replica);
        assert_eq!(replica.state.kv_size, 1);

        // Deliver SyncTimeout via the full handle_event call path
        let effects = replica.handle_event(raspen_types::Event::SyncTimeout);

        assert_eq!(effects.len(), 1, "expected exactly one effect");
        match &effects[0] {
            Effect::Broadcast(Message::Sync {
                from,
                round,
                log_idx,
                eta_star,
                ..
            }) => {
                assert_eq!(from, "n0");
                assert_eq!(*round, 0);
                assert_eq!(*log_idx, 0); // kv_size - 1 = 0
                assert_eq!(*eta_star, 0);
            }
            other => panic!("unexpected effect: {other:?}"),
        }
    }

    /// No sync when log is empty (guard: kv_size > 0).
    #[test]
    fn test_no_sync_when_empty() {
        let (_tmp, mut replica) = make_replica();

        let effects = replica.handle_event(raspen_types::Event::SyncTimeout);
        assert!(effects.is_empty(), "expected no effects when kv_size == 0");
    }

    /// No state change after broadcasting SYNC.
    #[test]
    fn test_send_sync_no_state_change() {
        let (_tmp, mut replica) = make_replica();
        do_speculate(&mut replica);

        let kv_size_before = replica.state.kv_size;
        let chkpt_idx_before = replica.state.chkpt_idx;

        handle_send_sync(&mut replica, 0);

        assert_eq!(replica.state.kv_size, kv_size_before, "kv_size must not change");
        assert_eq!(replica.state.chkpt_idx, chkpt_idx_before, "chkpt_idx must not change");
    }

    /// Threshold branch: kv_size > chkpt_idx fires after speculate even without SyncTimeout.
    #[test]
    fn test_sync_threshold_via_process_message() {
        let (_tmp, mut replica) = make_replica();
        // chkpt_idx defaults to 0; after speculate kv_size = 1 > 0 = chkpt_idx

        let speculate_msg = Message::Speculate {
            eta: 0,
            op_hash: [0u8; 32],
            from_seq: "seq".to_string(),
            round: 0,
        };
        let effects = replica.process_message(speculate_msg);

        // Should have SpecReply + Sync (threshold triggered)
        let has_sync = effects.iter().any(|e| matches!(e, Effect::Broadcast(Message::Sync { .. })));
        assert!(has_sync, "expected a Sync broadcast after kv_size > chkpt_idx");
    }

    #[test]
    fn test_checkpoint_forms_after_quorum() {
        let (_tmp, mut replica) = make_replica();
        // Speculate once: kv_size = 1
        do_speculate(&mut replica);
        let expected_hash = replica.state.kv_store.top_hash();
        let fast_quorum = replica.config.fast_quorum as usize;

        // Deliver fast_quorum Sync messages (all consistent: same round/log_idx/log_hash)
        let mut final_effects = vec![];
        for i in 0..fast_quorum {
            let sync_msg = Message::Sync {
                from: format!("n{i}"),
                round: 0,
                log_idx: 0, // kv_size - 1 = 0
                log_hash: expected_hash,
                eta_star: 0,
            };
            // Deliver via process_message full call path
            let effects = replica.process_message(sync_msg);
            final_effects = effects;
        }

        // The last message should have triggered checkpoint formation
        let has_checkpoint = final_effects.iter().any(|e| {
            matches!(e, Effect::Broadcast(Message::Checkpoint { log_idx, log_hash, .. })
                if *log_idx == 0 && *log_hash == expected_hash)
        });
        assert!(has_checkpoint, "expected Checkpoint broadcast after fast_quorum Sync messages");

        // State: chkpt_idx and chkpt_hash updated
        assert_eq!(replica.state.chkpt_idx, 0, "chkpt_idx must be updated to log_idx");
        assert_eq!(replica.state.chkpt_hash, expected_hash, "chkpt_hash must be updated");
    }

    /// No checkpoint below the (n − p) quorum threshold.
    #[test]
    fn test_no_checkpoint_below_quorum() {
        let (_tmp, mut replica) = make_replica();
        do_speculate(&mut replica);
        let expected_hash = replica.state.kv_store.top_hash();
        let fast_quorum = replica.config.fast_quorum as usize;

        // Deliver one fewer than fast_quorum Sync messages
        let mut all_effects = vec![];
        for i in 0..(fast_quorum - 1) {
            let sync_msg = Message::Sync {
                from: format!("n{i}"),
                round: 0,
                log_idx: 0,
                log_hash: expected_hash,
                eta_star: 0,
            };
            let effects = replica.process_message(sync_msg);
            all_effects.extend(effects);
        }

        let has_checkpoint = all_effects.iter().any(|e| {
            matches!(e, Effect::Broadcast(Message::Checkpoint { .. }))
        });
        assert!(!has_checkpoint, "must not form checkpoint below fast_quorum");

        // chkpt_idx must remain 0 (unchanged)
        assert_eq!(replica.state.chkpt_idx, 0);
    }

    /// Mismatched Sync messages (wrong log_hash) must not count toward quorum.
    #[test]
    fn test_mismatched_sync_does_not_count() {
        let (_tmp, mut replica) = make_replica();
        do_speculate(&mut replica);
        let fast_quorum = replica.config.fast_quorum as usize;

        // Deliver fast_quorum Sync messages with wrong log_hash
        let mut all_effects = vec![];
        for i in 0..fast_quorum {
            let sync_msg = Message::Sync {
                from: format!("n{i}"),
                round: 0,
                log_idx: 0,
                log_hash: [0xFFu8; 32], // wrong hash
                eta_star: 0,
            };
            let effects = replica.process_message(sync_msg);
            all_effects.extend(effects);
        }

        let has_checkpoint = all_effects.iter().any(|e| {
            matches!(e, Effect::Broadcast(Message::Checkpoint { .. }))
        });
        assert!(!has_checkpoint, "mismatched log_hash must not form checkpoint");
    }
}