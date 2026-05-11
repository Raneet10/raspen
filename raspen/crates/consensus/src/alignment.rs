// Alignment sub-protocol.
//
// A replica detects that it has diverged when it sees f+1 CHECKPOINT messages
// that are inconsistent with its own log.  It then broadcasts a STATE-REQUEST
// to its peers, and upon receiving a STATE-REPLY resets its log to the
// recovered checkpoint before re-entering Speculative phase.

use raspen_types::{Effect, KvEntry, Message, NodeId, Phase};

use crate::replica::Replica;

/// Returns `Some(chkpt_idx)` when f+1 CHECKPOINT messages for the current
/// (round, chkpt_idx) have been observed and the replica is still in Speculative phase.
///
/// Seeing f+1 matching CHECKPOINTs guarantees that at least one correct replica
/// formed that checkpoint, which means this replica's log has diverged and
/// needs to be corrected.
pub(crate) fn can_enter_align(replica: &Replica) -> Option<u64> {
    if replica.state.phase != Phase::Speculative {
        return None;
    }
    let chkpt_idx = replica.state.chkpt_idx;
    let round = replica.state.round;
    // Count distinct senders of CHECKPOINT messages that match our (round, chkpt_idx).
    let sender_count = replica
        .state
        .chkpt_msgs
        .iter()
        .filter(|m| {
            matches!(m, Message::Checkpoint { round: r, log_idx: li, .. }
                if *r == round && *li == chkpt_idx)
        })
        .count();
    if sender_count >= replica.config.f_plus_1 {
        Some(chkpt_idx)
    } else {
        None
    }
}



/// Switch to Aligning phase and broadcast a STATE-REQUEST.
///
/// While in Aligning phase the replica stops processing new requests.
/// It broadcasts a STATE-REQUEST so that peers with a valid checkpoint
/// will reply with the state needed to recover.
pub(crate) fn handle_enter_align(replica: &mut Replica, chkpt_idx: u64) -> Vec<Effect> {
    replica.state.phase = Phase::Aligning;
    vec![Effect::Broadcast(Message::StateRequest {
        from: replica.state.node_id.clone(),
        chkpt_idx,
    })]
}


/// Process an incoming CHECKPOINT: store it and check whether f+1 have been seen.
///
/// Each CHECKPOINT is stored in `chkpt_msgs` (HashSet deduplicates by sender).
/// After storing, the alignment guard is re-evaluated; if f+1 matching
/// CHECKPOINTs are now available the alignment transition fires.
///
/// Integration: called from Replica::process_message in replica.rs.
pub(crate) fn on_receive_checkpoint(replica: &mut Replica, msg: &Message) -> Vec<Effect> {
    if matches!(msg, Message::Checkpoint { .. }) {
        replica.state.chkpt_msgs.insert(msg.clone());
    }
    if let Some(chkpt_idx) = can_enter_align(replica) {
        handle_enter_align(replica, chkpt_idx)
    } else {
        vec![]
    }
}


/// Returns `Some(requester)` for every STATE-REQUEST message (unconditional).
///
/// Any replica that receives a STATE-REQUEST replies with its current checkpoint
/// state, regardless of its own phase.
pub(crate) fn can_state_request(msg: &Message) -> Option<NodeId> {
    if let Message::StateRequest { from, .. } = msg {
        Some(from.clone())
    } else {
        None
    }
}


/// Send a STATE-REPLY to the requester with our current checkpoint index and hash.
///
/// No state change occurs; only the unicast network effect is returned.
pub(crate) fn handle_state_request(replica: &Replica, requester: NodeId) -> Vec<Effect> {
    vec![Effect::Send {
        to: requester,
        message: Message::StateReply {
            from: replica.state.node_id.clone(),
            chkpt_idx: replica.state.chkpt_idx,
            chkpt_hash: replica.state.chkpt_hash,
        },
    }]
}


/// Returns `Some(chkpt_idx)` when a STATE-REPLY arrives while in Aligning phase.
///
/// The replica only accepts STATE-REPLY messages while it is actively aligning;
/// replies that arrive in other phases are ignored.
pub(crate) fn can_state_reply(replica: &Replica, msg: &Message) -> Option<u64> {
    if replica.state.phase != Phase::Aligning {
        return None;
    }
    if let Message::StateReply { chkpt_idx, .. } = msg {
        Some(*chkpt_idx)
    } else {
        None
    }
}


/// Rebuild the log from the recovered checkpoint and return to Speculative phase.
///
/// Synthetic log entries are written for all indices 0..=chkpt_idx, then the
/// checkpoint state (`chkpt_idx`, `chkpt_hash`, `kv_size`, `start_idx`) is
/// updated to match the recovered checkpoint.  No network effects are produced.
pub(crate) fn handle_state_reply(replica: &mut Replica, chkpt_idx: u64) -> Vec<Effect> {
    // Rebuild synthetic entries for all indices 0..=chkpt_idx.
    let entries: Vec<(u64, KvEntry)> = (0..=chkpt_idx)
        .map(|k| {
            (
                k,
                KvEntry {
                    op_hash: k,
                    result: vec![],
                    prev_hash: [k as u8; 32],
                },
            )
        })
        .collect();
    replica
        .state
        .kv_store
        .rebuild_from_entries(entries)
        .expect("rebuild failed");

    replica.state.phase = Phase::Speculative;
    replica.state.chkpt_idx = chkpt_idx;
    replica.state.chkpt_hash = replica
        .state
        .kv_store
        .hash_at(chkpt_idx)
        .unwrap_or([0u8; 32]);
    // kv_size covers all entries including the checkpoint index.
    replica.state.kv_size = chkpt_idx + 1;
    // New round starts at the checkpoint boundary.
    replica.state.start_idx = chkpt_idx;

    vec![]
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use raspen_types::{config::Config, effect::Effect, message::Message, phase::Phase};
    use tempfile::TempDir;

    use crate::replica::Replica;

    fn make_replica() -> (TempDir, Replica) {
        let tmp = TempDir::new().unwrap();
        // 6 nodes, f=1, p=1 → f_plus_1=2, fast_quorum=5, byz_quorum=5
        let config = Config::for_test(6, 1, 1);
        let replica = Replica::new(config, "n0".to_string(), tmp.path()).unwrap();
        (tmp, replica)
    }

    #[test]
    fn test_enter_align_after_f_plus_1_checkpoints() {
        let (_tmp, mut replica) = make_replica();
        assert_eq!(replica.state.phase, Phase::Speculative);
        let f_plus_1 = replica.config.f_plus_1;

        // Deliver f_plus_1 Checkpoint messages (all matching round=0, log_idx=0)
        let mut final_effects = vec![];
        for i in 0..f_plus_1 {
            let chkpt_msg = Message::Checkpoint {
                from: format!("n{i}"),
                round: 0,
                log_idx: 0,
                log_hash: [0u8; 32],
            };
            let effects = replica.process_message(chkpt_msg);
            final_effects = effects;
        }

        // The last message should have triggered alignment entry
        let has_state_request = final_effects.iter().any(|e| {
            matches!(e, Effect::Broadcast(Message::StateRequest { from, chkpt_idx })
                if from == "n0" && *chkpt_idx == 0)
        });
        assert!(
            has_state_request,
            "expected StateRequest broadcast after f+1 Checkpoints; got: {final_effects:?}"
        );
        assert_eq!(
            replica.state.phase,
            Phase::Aligning,
            "phase must be Aligning after enter_align"
        );
    }

    /// Guard rejects when below f+1 threshold.
    #[test]
    fn test_enter_align_below_threshold_no_effect() {
        let (_tmp, mut replica) = make_replica();
        let f_plus_1 = replica.config.f_plus_1;

        // Deliver one fewer than f_plus_1
        let mut all_effects = vec![];
        for i in 0..(f_plus_1 - 1) {
            let chkpt_msg = Message::Checkpoint {
                from: format!("n{i}"),
                round: 0,
                log_idx: 0,
                log_hash: [0u8; 32],
            };
            all_effects.extend(replica.process_message(chkpt_msg));
        }

        let has_state_request = all_effects
            .iter()
            .any(|e| matches!(e, Effect::Broadcast(Message::StateRequest { .. })));
        assert!(
            !has_state_request,
            "must not enter align below f+1 threshold"
        );
        assert_eq!(replica.state.phase, Phase::Speculative);
    }

    /// Guard rejects when not in Speculative phase.
    #[test]
    fn test_enter_align_guard_fails_wrong_phase() {
        let (_tmp, mut replica) = make_replica();
        replica.state.phase = Phase::Aligning;

        let result = can_enter_align(&replica);
        assert_eq!(result, None, "must not trigger when already Aligning");
    }


    /// Integration verified: process_message calls can_state_request → handle_state_request
    /// for every StateRequest message unconditionally.
    #[test]
    fn test_state_request_triggers_reply() {
        let (_tmp, replica) = make_replica();
        let msg = Message::StateRequest {
            from: "n1".to_string(),
            chkpt_idx: 0,
        };

        let effects = handle_state_request(&replica, "n1".to_string());
        assert_eq!(effects.len(), 1);
        match &effects[0] {
            Effect::Send { to, message } => {
                assert_eq!(to, "n1");
                assert!(matches!(message, Message::StateReply { from, .. } if from == "n0"));
            }
            other => panic!("unexpected effect: {other:?}"),
        }

        // can_state_request extracts requester from the message
        let requester = can_state_request(&msg);
        assert_eq!(requester, Some("n1".to_string()));
    }

    /// Non-StateRequest messages return None from guard.
    #[test]
    fn test_state_request_guard_fails_for_other_messages() {
        let msg = Message::Speculate {
            eta: 0,
            op_hash: [0u8; 32],
            from_seq: "s".to_string(),
            round: 0,
        };
        assert_eq!(can_state_request(&msg), None);
    }


    /// Integration verified: process_message calls can_state_reply → handle_state_reply
    /// when phase is Aligning and a StateReply arrives.
    #[test]
    fn test_state_reply_rebuilds_store_and_returns_speculative() {
        let (_tmp, mut replica) = make_replica();
        replica.state.phase = Phase::Aligning;

        let msg = Message::StateReply {
            from: "n1".to_string(),
            chkpt_idx: 3,
            chkpt_hash: [0u8; 32],
        };

        let chkpt_idx = can_state_reply(&replica, &msg).expect("guard must fire");
        assert_eq!(chkpt_idx, 3);

        let effects = handle_state_reply(&mut replica, chkpt_idx);
        assert!(effects.is_empty(), "state_reply must produce no effects");

        assert_eq!(replica.state.phase, Phase::Speculative);
        assert_eq!(replica.state.chkpt_idx, 3);
        assert_eq!(replica.state.kv_size, 4); // chkpt_idx + 1
        assert_eq!(replica.state.start_idx, 3);
        assert_eq!(replica.state.kv_store.size(), 4);
    }

    /// Guard rejects when not in Aligning phase.
    #[test]
    fn test_state_reply_guard_fails_when_not_aligning() {
        let (_tmp, replica) = make_replica();
        let msg = Message::StateReply {
            from: "n1".to_string(),
            chkpt_idx: 0,
            chkpt_hash: [0u8; 32],
        };
        assert_eq!(
            can_state_reply(&replica, &msg),
            None,
            "must not fire when not Aligning"
        );
    }
}