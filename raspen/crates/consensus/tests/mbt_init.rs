// MBT validation for Part 2: init_test
//
// Corresponds to Quint spec lines 929-933:
//
//   run init_test = init
//     .expect(NODES.forall(r => {
//       val st = s.system.get(r)
//       st.phase == Speculative and st.kv_size == 0 and st.round == 0
//     }))
//
// And the `initialize` function (spec lines 797-810) which defines the full
// initial replica state.

use raspen_consensus::replica::Replica;
use raspen_types::config::Config;
use raspen_types::phase::Phase;
use tempfile::TempDir;

/// Validate that `Replica::new` produces exactly the initial state described
/// by the Quint `initialize` function (spec lines 797-810) and checked by
/// `init_test` (spec lines 929-933).
///
/// The Quint test checks:
///   st.phase == Speculative and st.kv_size == 0 and st.round == 0
///
/// We additionally assert all remaining fields from `initialize` to ensure
/// full correspondence with the spec.
#[test]
fn mbt_init_state() {
    let tmp = TempDir::new().unwrap();
    // Config matching the spec's canonical parameters: n=6, f=1, p=1.
    // nodes_ordered = ["r0", "r1", ..., "r5"] (spec: NODES_ORDERED).
    let config = Config::for_test(6, 1, 1);

    // Initialize one replica — the spec's init_test checks all nodes;
    // the initial state is identical for every node so checking "r0" suffices.
    let replica = Replica::new(config, "r0".to_string(), tmp.path()).unwrap();

    // --- Quint init_test assertions (spec lines 929-933) ---

    // st.phase == Speculative  (spec line 797)
    assert_eq!(replica.state.phase, Phase::Speculative);

    // st.kv_size == 0  (spec line 801)
    assert_eq!(replica.state.kv_size, 0);

    // st.round == 0  (spec line 798)
    assert_eq!(replica.state.round, 0);

    // --- Remaining fields from `initialize` (spec lines 799-810) ---

    // view == 0  (spec line 799)
    assert_eq!(replica.state.view, 0);

    // start_idx == 0  (spec line 800)
    assert_eq!(replica.state.start_idx, 0);

    // kv_store == Map()  (spec line 804) — store must be empty
    assert_eq!(replica.state.kv_store.size(), 0);

    // chkpt_idx == 0  (spec line 805)
    assert_eq!(replica.state.chkpt_idx, 0);

    // chkpt_hash == 0  (spec line 806) — represented as [0u8; 32] in Rust
    assert_eq!(replica.state.chkpt_hash, [0u8; 32]);

    // eta_star == 0  (spec line 807)
    assert_eq!(replica.state.eta_star, 0);

    // in_repair == false  (spec line 808)
    assert!(!replica.state.in_repair);

    // proposed_history_hash == 0  (spec line 809)
    assert_eq!(replica.state.proposed_history_hash, 0);
}
