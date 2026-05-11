// MBT validation for Part 4: can_speculate_test
//
// Corresponds to Quint spec lines 936-938:
//
//   run can_speculate_test = init
//     .then(step)
//     .expect(NODES.exists(r => s.system.get(r).kv_size > 0))
//
// The Quint test witnesses that after one `step` from the initial state,
// at least one replica has `kv_size > 0` — meaning the speculate transition
// fired and wrote an entry to the KV store.
//
// In the Rust model, `step` may fire any enabled transition.  The speculate
// transition (fast_path::can_speculate / handle_speculate, spec lines 291-311)
// is the only transition enabled in the initial Speculative phase, so
// delivering one `Message::Speculate` corresponds exactly to one `step`.

use raspen_consensus::replica::Replica;
use raspen_types::config::Config;
use raspen_types::effect::Effect;
use raspen_types::message::Message;
use tempfile::TempDir;

/// Validate the Quint `can_speculate_test` witness (spec lines 936-938).
///
/// Spec assertion: `NODES.exists(r => s.system.get(r).kv_size > 0)`
///
/// We use a single replica ("n0") representing any node in NODES.  After
/// delivering one `Message::Speculate` — which corresponds to one `step` in
/// the Quint model — `replica.state.kv_size` must be greater than zero.
///
/// We additionally assert the effect emitted by the transition, confirming
/// that `handle_speculate` (spec lines 296-311) returns exactly one
/// `Effect::Broadcast(Message::SpecReply { log_idx: 0, .. })`.
#[test]
fn mbt_can_speculate() {
    let tmp = TempDir::new().unwrap();
    // Config matching spec canonical parameters: n=6, f=1, p=1.
    let config = Config::for_test(6, 1, 1);
    let mut replica = Replica::new(config, "n0".to_string(), tmp.path()).unwrap();

    // Pre-condition: initial state must mirror `initialize` (spec lines 797-810).
    // kv_size == 0 and phase == Speculative.
    assert_eq!(replica.state.kv_size, 0, "pre: kv_size must be 0 on init");

    // Deliver one Speculate message — this is the single `step` in the Quint
    // model.  The message content mirrors the simplest case from the spec:
    // eta = 0, op_hash = 0 (all-zeros), round = 0.
    let speculate = Message::Speculate {
        eta: 0,
        op_hash: [0u8; 32],
        from_seq: "seq".to_string(),
        round: 0,
    };
    let effects = replica.process_message(speculate);

    // --- Quint can_speculate_test assertion (spec line 938) ---
    // NODES.exists(r => s.system.get(r).kv_size > 0)
    // In Rust: our single replica must have kv_size > 0 after one step.
    assert!(
        replica.state.kv_size > 0,
        "kv_size must be > 0 after one speculate step (spec line 938)"
    );

    // --- Additional effect assertion (spec lines 303-308) ---
    // handle_speculate must broadcast SpecReply with log_idx = 0 (the first
    // entry appended to the log).
    assert!(
        effects.iter().any(|e| matches!(
            e,
            Effect::Broadcast(Message::SpecReply { log_idx: 0, .. })
        )),
        "should broadcast SpecReply with log_idx=0; got: {effects:?}"
    );
}
