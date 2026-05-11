// Fast-path speculative execution.
//
// When a sequenced request arrives, the replica appends it to its log at the
// next available index, chains the new entry's hash to the previous entry's
// hash, and broadcasts a SPEC-REPLY to the client.  The client commits once
// it has gathered (n − p) consistent SPEC-REPLYs.
//
// Entry point: Replica::process_message (replica.rs) dispatches here whenever a
// Message::Speculate arrives and the guard passes.

use raspen_types::{Effect, KvEntry, Message, Phase};

use crate::replica::Replica;


/// Returns the log index to write the next entry at, or `None` if the guard fails.
///
/// The replica only processes new requests while in Speculative phase.
/// The next entry is always placed at the current `kv_size` (the first unused index).
pub(crate) fn can_speculate(replica: &Replica, msg: &Message) -> Option<u64> {
    if !matches!(msg, Message::Speculate { .. }) {
        return None;
    }
    if replica.state.phase != Phase::Speculative {
        return None;
    }
    Some(replica.state.kv_size)
}


/// Append a new log entry and broadcast a SPEC-REPLY.
///
/// Computes H(k) by hashing the new entry together with H(k−1) (zero for the
/// genesis entry), writes the entry to the log, increments `kv_size`, and
/// broadcasts a SPEC-REPLY carrying the log index and hash.
pub(crate) fn handle_speculate(replica: &mut Replica, log_idx: u64) -> Vec<Effect> {
    // Hash of the previous entry, or zero for the genesis entry.
    let prev_hash: [u8; 32] = if log_idx > 0 {
        replica
            .state
            .kv_store
            .hash_at(log_idx - 1)
            .unwrap_or([0u8; 32])
    } else {
        [0u8; 32]
    };

    // op_hash stands in for the request identifier in the abstract spec.
    // In a real implementation the state machine would populate `result`.
    let new_entry = KvEntry {
        op_hash: log_idx,
        result: Vec::new(),
        prev_hash,
    };

    replica
        .state
        .kv_store
        .put(log_idx, &new_entry)
        .expect("kv_store put failed");

    // hash_at must succeed immediately after a successful put.
    let new_hash: [u8; 32] = replica
        .state
        .kv_store
        .hash_at(log_idx)
        .expect("hash_at immediately after put must succeed");

    replica.state.kv_size += 1;

    vec![Effect::Broadcast(Message::SpecReply {
        from: replica.state.node_id.clone(),
        round: replica.state.round,
        log_idx,
        log_hash: new_hash,
    })]
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use raspen_types::{config::Config, phase::Phase};
    use tempfile::TempDir;

    use crate::replica::Replica;

    fn make_replica(phase: Phase) -> (TempDir, Replica) {
        let tmp = TempDir::new().unwrap();
        let config = Config::for_test(6, 1, 1);
        let mut replica = Replica::new(config, "n0".to_string(), tmp.path()).unwrap();
        replica.state.phase = phase;
        (tmp, replica)
    }

    fn speculate_msg() -> Message {
        Message::Speculate {
            eta: 0,
            op_hash: [0u8; 32],
            from_seq: "seq".to_string(),
            round: 0,
        }
    }

    // -----------------------------------------------------------------------
    // can_speculate guard tests
    // -----------------------------------------------------------------------

    #[test]
    fn guard_passes_when_speculative() {
        let (_tmp, replica) = make_replica(Phase::Speculative);
        let msg = speculate_msg();
        assert_eq!(can_speculate(&replica, &msg), Some(0));
    }

    #[test]
    fn guard_fails_when_aligning() {
        let (_tmp, replica) = make_replica(Phase::Aligning);
        let msg = speculate_msg();
        assert_eq!(can_speculate(&replica, &msg), None);
    }

    #[test]
    fn guard_fails_for_non_speculate_message() {
        let (_tmp, replica) = make_replica(Phase::Speculative);
        let msg = Message::SpecReply {
            from: "x".to_string(),
            round: 0,
            log_idx: 0,
            log_hash: [0u8; 32],
        };
        assert_eq!(can_speculate(&replica, &msg), None);
    }

    // -----------------------------------------------------------------------
    // handle_speculate transition tests
    // -----------------------------------------------------------------------

    #[test]
    fn first_speculate_writes_genesis_entry() {
        let (_tmp, mut replica) = make_replica(Phase::Speculative);
        let effects = handle_speculate(&mut replica, 0);

        assert_eq!(replica.state.kv_size, 1);
        assert_eq!(replica.state.kv_store.size(), 1);

        assert_eq!(effects.len(), 1);
        match &effects[0] {
            Effect::Broadcast(Message::SpecReply {
                log_idx,
                from,
                log_hash,
                ..
            }) => {
                assert_eq!(*log_idx, 0);
                assert_eq!(from, "n0");
                assert_ne!(*log_hash, [0u8; 32], "hash must be non-zero for a real entry");
            }
            other => panic!("unexpected effect: {other:?}"),
        }

        // Genesis entry has no predecessor, so prev_hash is all zeros.
        let entry = replica.state.kv_store.get(0).unwrap().unwrap();
        assert_eq!(entry.prev_hash, [0u8; 32]);
        assert_eq!(entry.op_hash, 0);
    }

    #[test]
    fn second_speculate_chains_prev_hash() {
        let (_tmp, mut replica) = make_replica(Phase::Speculative);

        handle_speculate(&mut replica, 0);
        let hash_0 = replica.state.kv_store.hash_at(0).unwrap();

        handle_speculate(&mut replica, 1);
        let entry_1 = replica.state.kv_store.get(1).unwrap().unwrap();

        // Entry 1's prev_hash must be the hash of entry 0 (chained hashing).
        assert_eq!(entry_1.prev_hash, hash_0);
        assert_eq!(replica.state.kv_size, 2);
    }
}