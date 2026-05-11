// Repair entry sub-protocol.
//
// Replicas enter REPAIR via one of two paths:
//
// Conflict proof: If the replica has received enough SYNC messages to prove
//   that a checkpoint is impossible (byz_quorum total SYNCs but fewer than
//   fast_quorum consistent ones), it broadcasts a CONFLICT-PROOF and immediately
//   enters REPAIR.
//
// Timeout proof: If the replica's chkptTimeout expires before a checkpoint or
//   f+1 CHECKPOINTs are seen, it broadcasts a TIMEOUT message and waits.  Once
//   f+1 TIMEOUT messages are collected they are bundled into a TIMEOUT-PROOF,
//   broadcast, and the replica enters REPAIR.  Any replica that receives a
//   TIMEOUT-PROOF or CONFLICT-PROOF also enters REPAIR after re-broadcasting
//   the proof, ensuring all correct replicas enter REPAIR together.


use raspen_types::{Effect, Event, Message, Phase};

use crate::replica::Replica;


/// Returns `Some(log_idx)` when a checkpoint is provably impossible at the
/// current log top.
///
/// A checkpoint is impossible when the number of remaining SYNCs we could still
/// receive cannot bridge the gap to the fast-path quorum.  The condition is:
/// `total >= byz_quorum && consistent < fast_quorum`, where `total` is all
/// SYNCs seen for this (round, log_idx) and `consistent` is those that match
/// our own log hash.
pub(crate) fn can_conflict_proof(replica: &Replica) -> Option<u64> {
    if replica.state.phase != Phase::Speculative {
        return None;
    }
    if replica.state.kv_size == 0 {
        return None;
    }
    if replica.state.in_repair {
        return None;
    }
    let log_idx = replica.state.kv_size - 1;
    let round = replica.state.round;
    let expected_hash = replica.state.kv_store.top_hash();

    // All SYNC messages seen for this (round, log_idx), regardless of hash.
    let total = replica
        .state
        .sync_msgs
        .iter()
        .filter(|m| {
            matches!(m, Message::Sync { round: r, log_idx: li, .. }
                if *r == round && *li == log_idx)
        })
        .count();

    // SYNCs that agree with our own log hash (consistent SYNCs).
    let consist = replica
        .state
        .sync_msgs
        .iter()
        .filter(|m| {
            matches!(m, Message::Sync { round: r, log_idx: li, log_hash: lh, .. }
                if *r == round && *li == log_idx && *lh == expected_hash)
        })
        .count();

    // Checkpoint is impossible: we have byz_quorum SYNCs but not enough consistent ones.
    if total >= replica.config.byz_quorum && consist < replica.config.fast_quorum {
        Some(log_idx)
    } else {
        None
    }
}


/// Broadcast a CONFLICT-PROOF and LOG message, then enter CollectingLogs/in_repair.
///
/// The CONFLICT-PROOF is broadcast so other replicas also enter REPAIR.
/// The LOG message carries this replica's log summary to the current view leader
/// so it can propose a merged history.
pub(crate) fn handle_conflict_proof(replica: &mut Replica, log_idx: u64) -> Vec<Effect> {
    replica.state.phase = Phase::CollectingLogs;
    replica.state.in_repair = true;
    let log_hash = replica.state.kv_store.top_hash();
    vec![
        Effect::Broadcast(Message::ConflictProof {
            round: replica.state.round,
            log_idx,
        }),
        Effect::Broadcast(Message::LogMsg {
            from: replica.state.node_id.clone(),
            view: replica.state.view,
            round: replica.state.round,
            log_hash,
        }),
    ]
}


/// Returns `Some(log_idx)` when a ChkptTimeout fires in Speculative phase.
///
/// The checkpoint timer expires when the replica has not seen a checkpoint or
/// f+1 CHECKPOINTs within the allowed window.  The replica broadcasts a TIMEOUT
/// message as the first step toward entering REPAIR; it does not enter REPAIR
/// immediately but waits to see whether other replicas also time out.
///
pub(crate) fn can_chkpt_timeout(replica: &Replica, event: &Event) -> Option<u64> {
    if replica.state.phase != Phase::Speculative {
        return None;
    }
    if replica.state.in_repair {
        return None;
    }
    match event {
        Event::ChkptTimeout { log_idx } => Some(*log_idx),
        _ => None,
    }
}


/// Broadcast a TIMEOUT message for the given log index. No state change.
///
/// The TIMEOUT signals to other replicas that this replica has given up waiting
/// for a checkpoint at this log index.  State is not modified; the replica waits
/// for f+1 TIMEOUT messages before forming a TIMEOUT-PROOF and entering REPAIR.
///
/// Integration: called from Replica::handle_event in replica.rs.
pub(crate) fn handle_chkpt_timeout(replica: &Replica, log_idx: u64) -> Vec<Effect> {
    vec![Effect::Broadcast(Message::TimeoutMsg {
        from: replica.state.node_id.clone(),
        round: replica.state.round,
        log_idx,
    })]
}


/// Returns `Some(log_idx)` when f+1 TIMEOUT messages for the current (round, log_idx)
/// have been collected.
///
/// Once f+1 replicas have timed out, the set of TIMEOUT messages is bundled into
/// a TIMEOUT-PROOF and broadcast.  At least one correct replica must have
/// genuinely timed out for this threshold to be reached.
pub(crate) fn can_timeout_proof(replica: &Replica) -> Option<u64> {
    if replica.state.phase != Phase::Speculative {
        return None;
    }
    if replica.state.in_repair {
        return None;
    }
    if replica.state.kv_size == 0 {
        return None;
    }
    let log_idx = replica.state.kv_size - 1;
    let round = replica.state.round;
    let count = replica
        .state
        .timeout_msgs
        .iter()
        .filter(|m| {
            matches!(m, Message::TimeoutMsg { round: r, log_idx: li, .. }
                if *r == round && *li == log_idx)
        })
        .count();
    if count >= replica.config.f_plus_1 {
        Some(log_idx)
    } else {
        None
    }
}


/// Broadcast a TIMEOUT-PROOF and LOG message, then enter CollectingLogs/in_repair.
///
/// Mirrors handle_conflict_proof: broadcasts the proof so other replicas enter
/// REPAIR, and sends the LOG summary to the current view leader.
pub(crate) fn handle_timeout_proof(replica: &mut Replica, log_idx: u64) -> Vec<Effect> {
    replica.state.phase = Phase::CollectingLogs;
    replica.state.in_repair = true;
    let log_hash = replica.state.kv_store.top_hash();
    vec![
        Effect::Broadcast(Message::TimeoutProof {
            round: replica.state.round,
            log_idx,
        }),
        Effect::Broadcast(Message::LogMsg {
            from: replica.state.node_id.clone(),
            view: replica.state.view,
            round: replica.state.round,
            log_hash,
        }),
    ]
}


/// Process an incoming TIMEOUT: store it and check whether f+1 have been seen.
///
/// Integration: called from Replica::process_message in replica.rs.
pub(crate) fn on_receive_timeout(replica: &mut Replica, msg: &Message) -> Vec<Effect> {
    if matches!(msg, Message::TimeoutMsg { .. }) {
        replica.state.timeout_msgs.insert(msg.clone());
    }
    if let Some(log_idx) = can_timeout_proof(replica) {
        handle_timeout_proof(replica, log_idx)
    } else {
        vec![]
    }
}


/// Returns `Some(round)` when a TIMEOUT-PROOF or CONFLICT-PROOF is received
/// and the replica is not yet in REPAIR.
///
/// Any replica that receives a proof message enters REPAIR and re-broadcasts
/// the proof, ensuring that once one correct replica enters REPAIR all others
/// do so shortly after.
pub(crate) fn can_enter_repair(replica: &Replica, msg: &Message) -> Option<u64> {
    if replica.state.in_repair {
        return None;
    }
    if matches!(msg, Message::TimeoutProof { .. } | Message::ConflictProof { .. }) {
        Some(replica.state.round)
    } else {
        None
    }
}


/// Broadcast a LOG message and transition to CollectingLogs/in_repair.
///
/// Upon entering REPAIR this replica sends its LOG summary to the current view
/// leader so the leader can collect (n − f) logs and propose a merged history.
pub(crate) fn handle_enter_repair(replica: &mut Replica, _round: u64) -> Vec<Effect> {
    replica.state.phase = Phase::CollectingLogs;
    replica.state.in_repair = true;
    let log_hash = replica.state.kv_store.top_hash();
    vec![Effect::Broadcast(Message::LogMsg {
        from: replica.state.node_id.clone(),
        view: replica.state.view,
        round: replica.state.round,
        log_hash,
    })]
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
        // 6 nodes, f=1, p=1 → f_plus_1=2, byz_quorum=5, fast_quorum=5
        let config = Config::for_test(6, 1, 1);
        let replica = Replica::new(config, "n0".to_string(), tmp.path()).unwrap();
        (tmp, replica)
    }

    fn do_speculate(replica: &mut Replica) {
        crate::fast_path::handle_speculate(replica, 0);
    }


    #[test]
    fn test_chkpt_timeout_broadcasts_timeout_msg() {
        let (_tmp, mut replica) = make_replica();
        assert_eq!(replica.state.phase, Phase::Speculative);

        let effects = replica.handle_event(Event::ChkptTimeout { log_idx: 5 });

        assert_eq!(effects.len(), 1, "expected exactly one effect");
        match &effects[0] {
            Effect::Broadcast(Message::TimeoutMsg { from, round, log_idx }) => {
                assert_eq!(from, "n0");
                assert_eq!(*round, 0);
                assert_eq!(*log_idx, 5);
            }
            other => panic!("unexpected effect: {other:?}"),
        }
        // No state change after broadcasting TIMEOUT.
        assert_eq!(replica.state.phase, Phase::Speculative);
    }

    /// Guard rejects when phase is not Speculative.
    #[test]
    fn test_chkpt_timeout_guard_fails_wrong_phase() {
        let (_tmp, mut replica) = make_replica();
        replica.state.phase = Phase::Aligning;

        let effects = replica.handle_event(Event::ChkptTimeout { log_idx: 0 });
        assert!(
            effects.is_empty(),
            "must not fire when not Speculative"
        );
    }

    /// Guard rejects when in_repair is true.
    #[test]
    fn test_chkpt_timeout_guard_fails_when_in_repair() {
        let (_tmp, mut replica) = make_replica();
        replica.state.in_repair = true;

        let effects = replica.handle_event(Event::ChkptTimeout { log_idx: 0 });
        assert!(
            effects.is_empty(),
            "must not fire when in_repair"
        );
    }


    #[test]
    fn test_timeout_proof_after_f_plus_1_timeouts() {
        let (_tmp, mut replica) = make_replica();
        do_speculate(&mut replica);
        let f_plus_1 = replica.config.f_plus_1;

        let mut final_effects = vec![];
        for i in 0..f_plus_1 {
            let msg = Message::TimeoutMsg {
                from: format!("n{i}"),
                round: 0,
                log_idx: 0, // kv_size - 1 = 0
            };
            let effects = replica.process_message(msg);
            final_effects = effects;
        }

        let has_timeout_proof = final_effects.iter().any(|e| {
            matches!(e, Effect::Broadcast(Message::TimeoutProof { round: 0, log_idx: 0 }))
        });
        assert!(has_timeout_proof, "expected TimeoutProof; got: {final_effects:?}");

        let has_log_msg = final_effects
            .iter()
            .any(|e| matches!(e, Effect::Broadcast(Message::LogMsg { .. })));
        assert!(has_log_msg, "expected LogMsg broadcast with TimeoutProof");

        assert_eq!(replica.state.phase, Phase::CollectingLogs);
        assert!(replica.state.in_repair);
    }

    /// Guard rejects when below f+1 threshold.
    #[test]
    fn test_timeout_proof_below_threshold() {
        let (_tmp, mut replica) = make_replica();
        do_speculate(&mut replica);
        let f_plus_1 = replica.config.f_plus_1;

        let mut all_effects = vec![];
        for i in 0..(f_plus_1 - 1) {
            let msg = Message::TimeoutMsg {
                from: format!("n{i}"),
                round: 0,
                log_idx: 0,
            };
            all_effects.extend(replica.process_message(msg));
        }

        let has_timeout_proof = all_effects
            .iter()
            .any(|e| matches!(e, Effect::Broadcast(Message::TimeoutProof { .. })));
        assert!(!has_timeout_proof, "must not form timeout proof below f+1");
        assert_eq!(replica.state.phase, Phase::Speculative);
    }


    /// Integration verified: process_message calls can_enter_repair → handle_enter_repair
    /// when a TimeoutProof or ConflictProof arrives and not in_repair.
    #[test]
    fn test_enter_repair_on_timeout_proof() {
        let (_tmp, mut replica) = make_replica();
        assert!(!replica.state.in_repair);

        let msg = Message::TimeoutProof {
            round: 0,
            log_idx: 0,
        };
        let effects = replica.process_message(msg);

        let has_log_msg = effects
            .iter()
            .any(|e| matches!(e, Effect::Broadcast(Message::LogMsg { .. })));
        assert!(has_log_msg, "expected LogMsg from enter_repair; got: {effects:?}");
        assert_eq!(replica.state.phase, Phase::CollectingLogs);
        assert!(replica.state.in_repair);
    }

    /// Guard rejects when already in_repair.
    #[test]
    fn test_enter_repair_guard_fails_when_in_repair() {
        let (_tmp, mut replica) = make_replica();
        replica.state.in_repair = true;
        replica.state.phase = Phase::CollectingLogs;

        let msg = Message::TimeoutProof {
            round: 0,
            log_idx: 0,
        };
        let result = can_enter_repair(&replica, &msg);
        assert_eq!(result, None, "must not fire when already in_repair");
    }

    /// ConflictProof also triggers enter_repair.
    #[test]
    fn test_enter_repair_on_conflict_proof() {
        let (_tmp, mut replica) = make_replica();

        let msg = Message::ConflictProof {
            round: 0,
            log_idx: 0,
        };
        let effects = replica.process_message(msg);

        let has_log_msg = effects
            .iter()
            .any(|e| matches!(e, Effect::Broadcast(Message::LogMsg { .. })));
        assert!(has_log_msg, "ConflictProof must trigger enter_repair");
    }
}