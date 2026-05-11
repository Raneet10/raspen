// REPAIR agreement sub-protocol.
//
// Once replicas have entered REPAIR (CollectingLogs phase), they run a
// PBFT-style two-phase agreement to commit a merged log history:
//
// broadcast_history: The current view leader collects (n − f) LOG messages and
//   proposes a REPAIR-HISTORY containing the set of logs.
//
// repair_prepare / repair_commit: All replicas run two rounds of voting.
//   (n − f) matching REPAIR-PREPARE votes form a prepare certificate; (n − f)
//   matching REPAIR-COMMIT votes commit the history.
//
// apply_repair: After committing, the replica rebuilds its log from the agreed
//   history, increments the round number, and broadcasts REPAIR-DONE so lagging
//   replicas can catch up.  It also broadcasts COMMITTED-REPLY to notify clients.
//
// repair_done_catchup: A replica that falls behind can skip the prepare/commit
//   cycle if it sees f+1 REPAIR-DONE messages and already holds the REPAIR-HISTORY.
//
// view_change: If the leader fails to make progress, replicas time out and
//   elect the next leader via round-robin by incrementing their internal view
//   number and re-broadcasting their LOG to the new leader.


use std::collections::HashSet;

use raspen_types::{Effect, Event, KvEntry, Message, NodeId, Phase};

use crate::replica::Replica;


/// Returns `Some(history_hash)` when the leader has collected (n − f) LOG messages.
///
/// Only the current view leader broadcasts a REPAIR-HISTORY.  The history hash
/// abstractly represents the number of distinct log senders, which the leader
/// will use to identify the agreed-upon log set.
pub(crate) fn can_broadcast_history(replica: &Replica) -> Option<u64> {
    if replica.state.phase != Phase::CollectingLogs {
        return None;
    }
    if !replica.is_leader() {
        return None;
    }
    let view = replica.state.view;
    let round = replica.state.round;
    // Count distinct senders of LOG messages for the current (view, round).
    let senders: HashSet<&NodeId> = replica
        .state
        .log_msgs
        .iter()
        .filter_map(|m| match m {
            Message::LogMsg { from, view: v, round: r, .. } if *v == view && *r == round => {
                Some(from)
            }
            _ => None,
        })
        .collect();
    if senders.len() >= replica.config.byz_quorum {
        Some(senders.len() as u64)
    } else {
        None
    }
}


/// Propose a REPAIR-HISTORY and advance to PreparePhase.
///
/// The leader broadcasts the REPAIR-HISTORY to all replicas and records the
/// proposed history hash so it can validate subsequent REPAIR-PREPARE votes.
pub(crate) fn handle_broadcast_history(replica: &mut Replica, history_hash: u64) -> Vec<Effect> {
    replica.state.phase = Phase::PreparePhase;
    replica.state.proposed_history_hash = history_hash;
    vec![Effect::Broadcast(Message::RepairHistory {
        from: replica.state.node_id.clone(),
        view: replica.state.view,
        round: replica.state.round,
        history_hash,
    })]
}


/// Process an incoming LOG: store it and check whether the leader can broadcast history.
///
/// Integration: called from Replica::process_message in replica.rs.
pub(crate) fn on_receive_log_msg(replica: &mut Replica, msg: &Message) -> Vec<Effect> {
    if matches!(msg, Message::LogMsg { .. }) {
        replica.state.log_msgs.insert(msg.clone());
    }
    if let Some(history_hash) = can_broadcast_history(replica) {
        handle_broadcast_history(replica, history_hash)
    } else {
        vec![]
    }
}


/// Returns `Some(history_hash)` when a REPAIR-HISTORY for the current view has been received.
///
/// Accepted in either CollectingLogs or PreparePhase to handle the case where
/// the REPAIR-HISTORY arrives slightly before or after the replica enters PreparePhase.
pub(crate) fn can_repair_prepare(replica: &Replica) -> Option<u64> {
    let phase = replica.state.phase;
    if phase != Phase::CollectingLogs && phase != Phase::PreparePhase {
        return None;
    }
    let view = replica.state.view;
    let round = replica.state.round;
    // Look for a REPAIR-HISTORY from the current view leader.
    replica.state.repair_history_msgs.iter().find_map(|m| {
        if let Message::RepairHistory {
            view: v,
            round: r,
            history_hash,
            ..
        } = m
        {
            if *v == view && *r == round {
                Some(*history_hash)
            } else {
                None
            }
        } else {
            None
        }
    })
}


/// Vote for the proposed history hash and advance to PreparePhase.
///
/// The replica broadcasts a REPAIR-PREPARE and records the history hash locally
/// so it can validate subsequent REPAIR-COMMIT votes.
pub(crate) fn handle_repair_prepare(replica: &mut Replica, history_hash: u64) -> Vec<Effect> {
    replica.state.phase = Phase::PreparePhase;
    replica.state.proposed_history_hash = history_hash;
    vec![Effect::Broadcast(Message::RepairPrepare {
        from: replica.state.node_id.clone(),
        view: replica.state.view,
        round: replica.state.round,
        history_hash,
    })]
}


/// Process an incoming REPAIR-HISTORY: store it and check if the prepare vote can fire.
///
/// Integration: called from Replica::process_message in replica.rs.
pub(crate) fn on_receive_repair_history(replica: &mut Replica, msg: &Message) -> Vec<Effect> {
    if matches!(msg, Message::RepairHistory { .. }) {
        replica.state.repair_history_msgs.insert(msg.clone());
    }
    if let Some(history_hash) = can_repair_prepare(replica) {
        handle_repair_prepare(replica, history_hash)
    } else {
        vec![]
    }
}


/// Returns `Some(history_hash)` when (n − f) REPAIR-PREPARE votes for the
/// proposed history have been collected in PreparePhase.
///
/// These votes form a prepare certificate.  The replica then locks in its
/// choice by broadcasting a REPAIR-COMMIT.
pub(crate) fn can_repair_commit(replica: &Replica) -> Option<u64> {
    if replica.state.phase != Phase::PreparePhase {
        return None;
    }
    let view = replica.state.view;
    let round = replica.state.round;
    let hist = replica.state.proposed_history_hash;
    let count = replica
        .state
        .repair_prepare_msgs
        .iter()
        .filter(|m| {
            matches!(m, Message::RepairPrepare { view: v, round: r, history_hash: h, .. }
                if *v == view && *r == round && *h == hist)
        })
        .count();
    if count >= replica.config.byz_quorum {
        Some(hist)
    } else {
        None
    }
}


/// Broadcast a REPAIR-COMMIT vote and advance to CommitPhase.
///
/// After collecting (n − f) REPAIR-PREPAREs the replica locks in its vote and
/// waits for (n − f) REPAIR-COMMITs to complete the agreement.
pub(crate) fn handle_repair_commit(replica: &mut Replica, history_hash: u64) -> Vec<Effect> {
    replica.state.phase = Phase::CommitPhase;
    vec![Effect::Broadcast(Message::RepairCommit {
        from: replica.state.node_id.clone(),
        view: replica.state.view,
        round: replica.state.round,
        history_hash,
    })]
}


/// Process an incoming REPAIR-PREPARE: store it and check if the commit vote can fire.
///
/// Integration: called from Replica::process_message in replica.rs.
pub(crate) fn on_receive_repair_prepare(replica: &mut Replica, msg: &Message) -> Vec<Effect> {
    if matches!(msg, Message::RepairPrepare { .. }) {
        replica.state.repair_prepare_msgs.insert(msg.clone());
    }
    if let Some(history_hash) = can_repair_commit(replica) {
        handle_repair_commit(replica, history_hash)
    } else {
        vec![]
    }
}


/// Returns `Some(history_hash)` when (n − f) REPAIR-COMMIT votes for the
/// proposed history have been collected in CommitPhase.
///
/// Once this threshold is met the replica may apply the merged history, rebuild
/// its log, and enter the next round.
pub(crate) fn can_apply_repair(replica: &Replica) -> Option<u64> {
    if replica.state.phase != Phase::CommitPhase {
        return None;
    }
    let view = replica.state.view;
    let round = replica.state.round;
    let hist = replica.state.proposed_history_hash;
    let count = replica
        .state
        .repair_commit_msgs
        .iter()
        .filter(|m| {
            matches!(m, Message::RepairCommit { view: v, round: r, history_hash: h, .. }
                if *v == view && *r == round && *h == hist)
        })
        .count();
    if count >= replica.config.byz_quorum {
        Some(hist)
    } else {
        None
    }
}


/// Apply the merged history, advance the round, and broadcast REPAIR-DONE + COMMITTED-REPLY.
///
/// The replica rebuilds its log from the agreed history (here represented
/// abstractly by `history_hash` as the merged log size), increments the round,
/// clears all repair state, and returns to Speculative phase.  REPAIR-DONE is
/// broadcast so lagging replicas can catch up; COMMITTED-REPLY notifies clients
/// that their requests are committed.
pub(crate) fn handle_apply_repair(replica: &mut Replica, history_hash: u64) -> Vec<Effect> {
    let merged_size = history_hash;
    let old_round = replica.state.round;
    let old_kv_size = replica.state.kv_size;

    if merged_size > 0 {
        let entries: Vec<(u64, KvEntry)> = (0..merged_size)
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
    }

    replica.state.phase = Phase::Speculative;
    replica.state.round = old_round + 1;
    replica.state.start_idx = merged_size;
    replica.state.kv_size = merged_size;
    replica.state.in_repair = false;
    replica.state.proposed_history_hash = 0;

    vec![
        // Broadcast REPAIR-DONE so lagging replicas can catch up.
        Effect::Broadcast(Message::RepairDone {
            from: replica.state.node_id.clone(),
            view: replica.state.view,
            round: old_round,
            history_hash,
        }),
        // Notify clients that their requests are committed.
        Effect::Broadcast(Message::CommittedReply {
            from: replica.state.node_id.clone(),
            round: old_round,
            log_idx: old_kv_size,
        }),
    ]
}


/// Process an incoming REPAIR-COMMIT: store it and check if apply_repair can fire.
///
pub(crate) fn on_receive_repair_commit(replica: &mut Replica, msg: &Message) -> Vec<Effect> {
    if matches!(msg, Message::RepairCommit { .. }) {
        replica.state.repair_commit_msgs.insert(msg.clone());
    }
    if let Some(history_hash) = can_apply_repair(replica) {
        handle_apply_repair(replica, history_hash)
    } else {
        vec![]
    }
}


/// Returns `Some(round)` when f+1 REPAIR-DONE messages and a REPAIR-HISTORY are available.
///
/// A lagging replica that has not yet committed can catch up when it sees f+1
/// REPAIR-DONE messages, because at least one correct replica has successfully
/// exited REPAIR.  The replica also needs the REPAIR-HISTORY to know what the
/// agreed-upon log looks like.
pub(crate) fn can_repair_done_catchup(replica: &Replica) -> Option<u64> {
    // Only catch up while still inside a repair phase.
    let in_repair_phase = matches!(
        replica.state.phase,
        Phase::CollectingLogs | Phase::PreparePhase | Phase::CommitPhase
    );
    if !in_repair_phase {
        return None;
    }
    let view = replica.state.view;
    let round = replica.state.round;
    let done_count = replica
        .state
        .repair_done_msgs
        .iter()
        .filter(|m| {
            matches!(m, Message::RepairDone { view: v, round: r, .. }
                if *v == view && *r == round)
        })
        .count();
    // Require the REPAIR-HISTORY so the replica knows the agreed-upon log.
    let has_hist = replica.state.repair_history_msgs.iter().any(|m| {
        matches!(m, Message::RepairHistory { view: v, round: r, .. }
            if *v == view && *r == round)
    });
    if done_count >= replica.config.f_plus_1 && has_hist {
        Some(round)
    } else {
        None
    }
}


/// Exit REPAIR and advance the round without replaying log changes.
///
/// The lagging replica increments the round and resets all repair state to
/// rejoin the fast path.  No network effects are produced because the log
/// changes were already agreed upon by the replicas that fully committed.
pub(crate) fn handle_repair_done_catchup(replica: &mut Replica, _round: u64) -> Vec<Effect> {
    replica.state.phase = Phase::Speculative;
    replica.state.round += 1;
    // New round starts at the current log size.
    replica.state.start_idx = replica.state.kv_size;
    replica.state.in_repair = false;
    replica.state.proposed_history_hash = 0;
    vec![]
}


/// Process an incoming REPAIR-DONE: store it and check if catchup can fire.
///
/// Integration: called from Replica::process_message in replica.rs.
pub(crate) fn on_receive_repair_done(replica: &mut Replica, msg: &Message) -> Vec<Effect> {
    if matches!(msg, Message::RepairDone { .. }) {
        replica.state.repair_done_msgs.insert(msg.clone());
    }
    if let Some(round) = can_repair_done_catchup(replica) {
        handle_repair_done_catchup(replica, round)
    } else {
        vec![]
    }
}


/// Returns `Some(new_view)` when a RepairViewTimeout fires in CollectingLogs phase.
///
/// If the current leader has not broadcast a REPAIR-HISTORY within the timeout
/// window, replicas increment their view to elect the next round-robin leader
/// and re-send their LOG to it.
///
/// Integration: called from Replica::handle_event in replica.rs.
pub(crate) fn can_view_change(replica: &Replica, event: &Event) -> Option<u64> {
    // View change only occurs while waiting for LOG collection to complete.
    if replica.state.phase != Phase::CollectingLogs {
        return None;
    }
    if matches!(event, Event::RepairViewTimeout) {
        Some(replica.state.view + 1)
    } else {
        None
    }
}


/// Increment the view and re-broadcast a LOG message to the new leader.
///
/// After updating the view number the replica sends its LOG to the newly elected
/// leader.  The new leader can then propose a REPAIR-HISTORY, using any existing
/// prepare certificates from the previous view to preserve already-committed work.
///
/// Integration: called from Replica::handle_event in replica.rs.
pub(crate) fn handle_view_change(replica: &mut Replica, new_view: u64) -> Vec<Effect> {
    replica.state.view = new_view;
    let log_hash = replica.state.kv_store.top_hash();
    vec![Effect::Broadcast(Message::LogMsg {
        from: replica.state.node_id.clone(),
        view: new_view,
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
        // Use "r0" as node_id so it is the leader for view=0 (NODES_ORDERED[0 mod 6] = "r0")
        let config = Config::for_test(6, 1, 1);
        let replica = Replica::new(config, "r0".to_string(), tmp.path()).unwrap();
        (tmp, replica)
    }

    fn make_non_leader_replica() -> (TempDir, Replica) {
        let tmp = TempDir::new().unwrap();
        let config = Config::for_test(6, 1, 1);
        // "r1" is not the leader for view=0
        let replica = Replica::new(config, "r1".to_string(), tmp.path()).unwrap();
        (tmp, replica)
    }

 

    #[test]
    fn test_view_change_on_repair_timeout() {
        let (_tmp, mut replica) = make_replica();
        replica.state.phase = Phase::CollectingLogs;
        assert_eq!(replica.state.view, 0);

        let effects = replica.handle_event(Event::RepairViewTimeout);

        assert_eq!(effects.len(), 1, "expected exactly one effect");
        match &effects[0] {
            Effect::Broadcast(Message::LogMsg { from, view, .. }) => {
                assert_eq!(from, "r0");
                assert_eq!(*view, 1); // new_view = old_view + 1
            }
            other => panic!("unexpected effect: {other:?}"),
        }
        assert_eq!(replica.state.view, 1, "view must increment");
    }

    /// Guard rejects when not in CollectingLogs phase.
    #[test]
    fn test_view_change_guard_fails_wrong_phase() {
        let (_tmp, mut replica) = make_replica();
        // phase is Speculative by default

        let effects = replica.handle_event(Event::RepairViewTimeout);
        assert!(effects.is_empty(), "must not fire when not CollectingLogs");
    }

    // -----------------------------------------------------------------------
    // Full REPAIR protocol integration test
    // -----------------------------------------------------------------------

    /// Multi-step test simulating the full REPAIR protocol.

    #[test]
    fn test_full_repair_protocol() {
        let (_tmp, mut leader) = make_replica();
        // Set leader into CollectingLogs+in_repair (as if entered via enter_repair)
        leader.state.phase = Phase::CollectingLogs;
        leader.state.in_repair = true;
        leader.state.kv_size = 3;

        let byz_quorum = leader.config.byz_quorum;
        let view = leader.state.view;
        let round = leader.state.round;

        // Step 1: Deliver byz_quorum LogMsgs to the leader replica
        let mut step1_effects = vec![];
        for i in 0..byz_quorum {
            let msg = Message::LogMsg {
                from: format!("r{i}"),
                view,
                round,
                log_hash: [0u8; 32],
            };
            let effects = leader.process_message(msg);
            step1_effects = effects;
        }

        // Assert RepairHistory broadcast from leader
        let repair_history_effect = step1_effects.iter().find(|e| {
            matches!(e, Effect::Broadcast(Message::RepairHistory { .. }))
        });
        assert!(
            repair_history_effect.is_some(),
            "leader must broadcast RepairHistory after byz_quorum LogMsgs; got: {step1_effects:?}"
        );
        assert_eq!(
            leader.state.phase,
            Phase::PreparePhase,
            "leader must move to PreparePhase"
        );
        let history_hash = leader.state.proposed_history_hash;

        // Step 2: Leader receives its own RepairHistory → broadcasts RepairPrepare
        // (on_receive_repair_history fires for CollectingLogs | PreparePhase)
        // The leader already moved to PreparePhase; simulate receiving RepairHistory from itself
        let hist_msg = Message::RepairHistory {
            from: "r0".to_string(),
            view,
            round,
            history_hash,
        };
        let step2_effects = leader.process_message(hist_msg);
        let has_prepare = step2_effects.iter().any(|e| {
            matches!(e, Effect::Broadcast(Message::RepairPrepare { history_hash: h, .. })
                if *h == history_hash)
        });
        assert!(
            has_prepare,
            "leader must broadcast RepairPrepare after receiving RepairHistory; got: {step2_effects:?}"
        );

        // Step 3: Deliver byz_quorum RepairPrepare messages
        let mut step3_effects = vec![];
        for i in 0..byz_quorum {
            let msg = Message::RepairPrepare {
                from: format!("r{i}"),
                view,
                round,
                history_hash,
            };
            let effects = leader.process_message(msg);
            step3_effects = effects;
        }

        let has_commit = step3_effects.iter().any(|e| {
            matches!(e, Effect::Broadcast(Message::RepairCommit { history_hash: h, .. })
                if *h == history_hash)
        });
        assert!(
            has_commit,
            "must broadcast RepairCommit after byz_quorum RepairPrepares; got: {step3_effects:?}"
        );
        assert_eq!(leader.state.phase, Phase::CommitPhase);

        // Step 4: Deliver byz_quorum RepairCommit messages
        let mut step4_effects = vec![];
        for i in 0..byz_quorum {
            let msg = Message::RepairCommit {
                from: format!("r{i}"),
                view,
                round,
                history_hash,
            };
            let effects = leader.process_message(msg);
            step4_effects = effects;
        }

        let has_repair_done = step4_effects.iter().any(|e| {
            matches!(e, Effect::Broadcast(Message::RepairDone { round: r, history_hash: h, .. })
                if *r == round && *h == history_hash)
        });
        assert!(
            has_repair_done,
            "must broadcast RepairDone after byz_quorum RepairCommits; got: {step4_effects:?}"
        );

        let has_committed_reply = step4_effects
            .iter()
            .any(|e| matches!(e, Effect::Broadcast(Message::CommittedReply { .. })));
        assert!(has_committed_reply, "must broadcast CommittedReply after apply_repair");

        assert_eq!(
            leader.state.phase,
            Phase::Speculative,
            "phase must return to Speculative"
        );
        assert_eq!(leader.state.round, 1, "round must increment after repair");
        assert!(!leader.state.in_repair, "in_repair must be false after repair");
        assert_eq!(leader.state.proposed_history_hash, 0);
    }


    /// Non-leader does not broadcast history even with enough LogMsgs.
    #[test]
    fn test_broadcast_history_only_for_leader() {
        let (_tmp, mut non_leader) = make_non_leader_replica();
        non_leader.state.phase = Phase::CollectingLogs;
        non_leader.state.in_repair = true;

        let byz_quorum = non_leader.config.byz_quorum;
        let view = non_leader.state.view;
        let round = non_leader.state.round;

        let mut all_effects = vec![];
        for i in 0..byz_quorum {
            let msg = Message::LogMsg {
                from: format!("r{i}"),
                view,
                round,
                log_hash: [0u8; 32],
            };
            all_effects.extend(non_leader.process_message(msg));
        }

        let has_repair_history = all_effects
            .iter()
            .any(|e| matches!(e, Effect::Broadcast(Message::RepairHistory { .. })));
        assert!(
            !has_repair_history,
            "non-leader must not broadcast RepairHistory"
        );
    }


    /// Non-committing replica catches up via f+1 RepairDone + RepairHistory.
    #[test]
    fn test_repair_done_catchup() {
        let (_tmp, mut replica) = make_non_leader_replica();
        let view = replica.state.view;
        let round = replica.state.round;
        let f_plus_1 = replica.config.f_plus_1;

        // Set up replica in PreparePhase (past CollectingLogs) with a proposed_history_hash.
        // At this point it has already processed a RepairHistory and moved to PreparePhase.
        replica.state.phase = Phase::PreparePhase;
        replica.state.in_repair = true;
        replica.state.proposed_history_hash = 5;

        // Inject the RepairHistory into the message set so has_hist is true.
        // (The replica is already in PreparePhase so can_repair_prepare won't fire again
        // because on_receive_repair_history's can_repair_prepare accepts CollectingLogs OR PreparePhase.)
        // Instead, set CommitPhase to skip the prepare guard entirely.
        replica.state.phase = Phase::CommitPhase;

        let hist_msg = Message::RepairHistory {
            from: "r0".to_string(),
            view,
            round,
            history_hash: 5,
        };
        // Insert directly so it satisfies has_hist without triggering repair_prepare
        replica.state.repair_history_msgs.insert(hist_msg);

        // Deliver f_plus_1 RepairDone messages.
        // In CommitPhase, can_repair_commit fires if byz_quorum RepairCommits are present —
        // but we have none, so only can_repair_done_catchup should fire once threshold is met.
        let mut final_effects = vec![];
        for i in 0..f_plus_1 {
            let msg = Message::RepairDone {
                from: format!("r{i}"),
                view,
                round,
                history_hash: 5,
            };
            let effects = replica.process_message(msg);
            final_effects = effects;
        }

        // Should catch up with no network effects
        assert!(
            final_effects.is_empty(),
            "catchup must produce no effects; got: {final_effects:?}"
        );
        assert_eq!(replica.state.phase, Phase::Speculative);
        assert_eq!(replica.state.round, 1);
        assert!(!replica.state.in_repair);
    }
}