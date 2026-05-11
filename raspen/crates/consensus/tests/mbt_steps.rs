// MBT validation for Part 7: can_take_steps_test
//
// Corresponds to Quint spec lines 941-945:
//
//   run can_take_steps_test = init
//     .then(step)
//     .then(step)
//     .then(step)
//     .expect(true)
//
// The Quint test witnesses that 3 sequential `step` transitions can be taken
// from the initial state without any invariant violation. `expect(true)` means
// this is a liveness/reachability test: the system must always be able to make
// progress — no deadlock, no panic, no corrupted state.
//
// In the Rust model the three steps correspond to three rounds of message
// delivery through a 6-replica network:
//
//   Step 1  — Sequencer delivers `Speculate` to all replicas; each replica
//              speculatively executes and broadcasts `SpecReply` and `Sync`.
//
//   Step 2  — Each broadcast from Step 1 is delivered to every replica.
//              When a replica receives enough matching `Sync` messages it
//              forms a `Checkpoint` and broadcasts it.
//
//   Step 3  — Each broadcast from Step 2 is delivered to every replica.
//              This models the spec's third `step`, which may process
//              Checkpoint messages and any further effects.

use raspen_consensus::replica::Replica;
use raspen_types::config::Config;
use raspen_types::effect::Effect;
use raspen_types::message::Message;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// Create one replica using the canonical spec config (n=6, f=1, p=1).
///
/// Returns both the `TempDir` and the `Replica` so the caller keeps the
/// directory alive for the duration of the test (dropping `TempDir` early
/// would close the underlying RocksDB handle).
fn make_replica(config: Config, node_id: &str) -> (TempDir, Replica) {
    let tmp = TempDir::new().expect("tempdir");
    let replica = Replica::new(config, node_id.to_string(), tmp.path())
        .expect("Replica::new");
    (tmp, replica)
}

/// Collect every `Effect::Broadcast` message from a slice of effects.
fn collect_broadcasts(effects: &[Effect]) -> Vec<Message> {
    effects
        .iter()
        .filter_map(|e| {
            if let Effect::Broadcast(m) = e {
                Some(m.clone())
            } else {
                None
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// MBT test
// ---------------------------------------------------------------------------

/// Validate the Quint `can_take_steps_test` witness (spec lines 941-945).
///
/// Spec assertion: `expect(true)` — three steps must complete without failure.
///
/// We use a 6-replica network (matching the spec's `NODES` set) and simulate
/// three rounds of message delivery.  The test asserts:
///
/// 1. No panics or assertion failures during any step.
/// 2. After all steps, every replica holds a non-negative `kv_size` and
///    `round` (trivially true by type but mirrors the spec's type constraints).
/// 3. After all steps, at least one replica has `kv_size > 0`, confirming
///    that the speculate transition fired — i.e., progress was made.
#[test]
fn mbt_can_take_steps() {
    // Config matching the spec's canonical parameters: n=6, f=1, p=1.
    // `Config::for_test` assigns node IDs "r0" … "r5" to match NODES_ORDERED.
    // We call it once per replica because each replica needs ownership.
    let node_ids = ["r0", "r1", "r2", "r3", "r4", "r5"];

    // Keep the TempDirs alive alongside the replicas.
    let mut replicas_and_dirs: Vec<(TempDir, Replica)> = node_ids
        .iter()
        .map(|id| make_replica(Config::for_test(6, 1, 1), id))
        .collect();

    // -----------------------------------------------------------------------
    // Step 1 — Deliver Speculate to all replicas (spec: first `step`)
    //
    // In the spec `step` non-deterministically picks one enabled transition
    // for one replica.  Delivering Speculate is the only transition enabled
    // in the initial Speculative phase, so every replica fires it when the
    // message arrives.  We deliver to all replicas in one round, which models
    // the spec choosing `step` for each replica in turn.
    // -----------------------------------------------------------------------
    let speculate = Message::Speculate {
        eta: 0,
        op_hash: [0u8; 32],
        from_seq: "seq".to_string(),
        round: 0,
    };

    let mut step1_effects: Vec<Effect> = Vec::new();
    for (_, replica) in replicas_and_dirs.iter_mut() {
        let effects = replica.process_message(speculate.clone());
        step1_effects.extend(effects);
    }

    // Each replica in Speculative phase must have produced at least one effect
    // (SpecReply broadcast + Sync broadcast from the threshold guard).
    assert!(
        !step1_effects.is_empty(),
        "Step 1 must produce effects; got none"
    );

    // -----------------------------------------------------------------------
    // Step 2 — Route all Step-1 broadcasts to every replica (spec: second `step`)
    //
    // This models the network delivering the messages produced in Step 1.
    // When each replica receives the Sync messages from every other replica,
    // the fast-quorum guard (can_form_checkpoint) may fire once a quorum
    // of matching Sync messages has accumulated.
    // -----------------------------------------------------------------------
    let step1_msgs = collect_broadcasts(&step1_effects);

    let mut step2_effects: Vec<Effect> = Vec::new();
    for msg in &step1_msgs {
        for (_, replica) in replicas_and_dirs.iter_mut() {
            let effects = replica.process_message(msg.clone());
            step2_effects.extend(effects);
        }
    }

    // -----------------------------------------------------------------------
    // Step 3 — Route all Step-2 broadcasts to every replica (spec: third `step`)
    //
    // This delivers any Checkpoint messages formed in Step 2, along with any
    // other broadcasts, completing the third `step` of the Quint test.
    // -----------------------------------------------------------------------
    let step2_msgs = collect_broadcasts(&step2_effects);

    for msg in &step2_msgs {
        for (_, replica) in replicas_and_dirs.iter_mut() {
            let _ = replica.process_message(msg.clone());
        }
    }

    // -----------------------------------------------------------------------
    // Spec assertion: expect(true)
    //
    // The Quint assertion is trivially satisfied by reaching this point without
    // a panic.  We additionally check the type-level invariants from the spec:
    //   - kv_size: int  (must be non-negative; i64 cannot go negative here)
    //   - round: int    (same)
    // and a stronger liveness property: at least one replica made progress.
    // -----------------------------------------------------------------------
    for (_, replica) in &replicas_and_dirs {
        // Spec type constraint: kv_size is a non-negative integer.
        // In Rust kv_size is u64 so this assert can never fail, but it
        // documents correspondence with the spec's type system.
        assert!(
            replica.state.kv_size as i64 >= 0,
            "kv_size must be non-negative (spec type constraint)"
        );

        // Spec type constraint: round is a non-negative integer.
        assert!(
            replica.state.round as i64 >= 0,
            "round must be non-negative (spec type constraint)"
        );
    }

    // Stronger: at least one replica speculated (kv_size > 0).
    // This confirms the first `step` fired the speculate transition for at
    // least one replica — i.e., the system made observable progress.
    assert!(
        replicas_and_dirs
            .iter()
            .any(|(_, r)| r.state.kv_size > 0),
        "At least one replica must have kv_size > 0 after 3 steps"
    );
}
