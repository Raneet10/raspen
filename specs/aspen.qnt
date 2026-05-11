// -*- mode: Bluespec; -*-
/**
 * Aspen Byzantine Fault-Tolerant Replicated State Machine
 *
 * Based on: https://arxiv.org/pdf/2601.03390
 *
 * Aspen is a leaderless BFT RSM using 3f+2p+1 replicas tolerating up to f Byzantine
 * faults and p diverged replicas. It features four execution paths:
 *
 *   Fast path   - Clients submit requests via a sequencer; replicas speculatively
 *                 execute and reply. Clients commit after (n-p) consistent SPEC-REPLYs.
 *
 *   Checkpoint  - Replicas periodically broadcast SYNC. On (n-p) matching SYNCs for
 *                 an index k, a checkpoint is formed and the log prefix committed.
 *                 Outcomes: CHECKPOINT (success) | ALIGN (diverged) | REPAIR (no quorum).
 *
 *   Alignment   - A diverged replica rd requests a state transfer (STATE-REQUEST /
 *                 STATE-REPLY) then reapplies requests with ETA > η*.
 *
 *   Repair      - PBFT-style agreement: collect LOG messages, leader proposes
 *                 REPAIR-HISTORY, then PREPARE → COMMIT → DONE phases merge logs
 *                 and advance to round i+1.
 *
 * This spec models all four paths using the Choreo framework.
 * All types are abstract (e.g. log hashes = int, snapshots = int).
 */
module aspen {
  import basicSpells.* from "choreo/spells/basicSpells"
  import choreo(processes = NODES) as choreo from "choreo/choreo"

  // ===========================================================================
  // PROTOCOL-SPECIFIC TYPES
  // ===========================================================================

  // Protocol phase tracked per replica
  type Phase =
    | Speculative       // Normal: speculatively executing requests (fast path)
    | Aligning          // ALIGN: requesting state transfer from checkpoint holders
    | CollectingLogs    // REPAIR: collecting LOG messages before proposing history
    | PreparePhase      // REPAIR: PBFT prepare phase (broadcasting REPAIR-PREPARE)
    | CommitPhase       // REPAIR: PBFT commit phase (broadcasting REPAIR-COMMIT)

  // Key-value store replacing the abstract log_size/log_hash integers.
  // Each entry at index k stores: the hash of the operation at k (op_hash),
  // the execution result (result), and the chaining hash H(k-1) (prev_hash).
  type KVEntry = {
    op_hash: int,    // abstract hash of the client operation
    result: int,     // abstract execution result
    prev_hash: int   // H(k-1): hash of the previous entry, enabling chain verification
  }

  // KVStore maps log index (int) to KVEntry.
  // Map type syntax in Quint: KeyType -> ValueType
  type KVStore = int -> KVEntry

  // Sentinel empty entry used for map pre-population and as the genesis prev_hash.
  pure val EMPTY_KV_ENTRY: KVEntry = { op_hash: 0, result: 0, prev_hash: 0 }

  // ===========================================================================
  // MANDATORY CHOREO TYPE DEFINITIONS
  // ===========================================================================

  type Node = str

  // All Aspen message types (§5.2 – §5.6)
  type Message =
    // Fast path
    | SpecReply({ from: Node, round: int, log_idx: int, log_hash: int })
    // Checkpoint sub-protocol (§5.4)
    | Sync({ from: Node, round: int, log_idx: int, log_hash: int, eta_star: int })
    | Checkpoint({ from: Node, round: int, log_idx: int, log_hash: int })
    // Alignment sub-protocol (§5.5)
    | StateRequest({ from: Node, chkpt_idx: int })
    | StateReply({ from: Node, chkpt_idx: int, chkpt_hash: int })
    // Repair entry (§5.6.1)
    | TimeoutMsg({ from: Node, round: int, log_idx: int })
    | TimeoutProof({ round: int, log_idx: int })
    | ConflictProof({ round: int, log_idx: int })
    // Repair agreement (§5.6.2)
    | LogMsg({ from: Node, view: int, round: int, log_hash: int })
    | RepairHistory({ from: Node, view: int, round: int, history_hash: int })
    | RepairPrepare({ from: Node, view: int, round: int, history_hash: int })
    | RepairCommit({ from: Node, view: int, round: int, history_hash: int })
    | RepairDone({ from: Node, view: int, round: int, history_hash: int })
    | CommittedReply({ from: Node, round: int, log_idx: int })

  // Local state fields for each replica (§5.1)
  type StateFields = {
    phase: Phase,
    round: int,                 // i: current Aspen round
    view: int,                  // v: internal REPAIR view (leader = v mod n)
    start_idx: int,             // startIdx: first uncommitted log index this round
    kv_size: int,               // |log|: number of log entries at this replica
    kv_store: KVStore,          // key-value store replacing abstract log_hash
    chkpt_idx: int,             // chkpt.idx: latest committed checkpoint index
    chkpt_hash: int,            // chkpt hash (abstract: = chkpt_idx)
    eta_star: int,              // η*: largest ETA seen this round
    in_repair: bool,            // true while executing REPAIR sub-protocol
    proposed_history_hash: int  // h(H): history hash being voted on in REPAIR
  }

  // External events (timeouts) that trigger sub-protocol entry
  type Event =
    | SyncTimeout               // syncTimeout expired (§5.4): send SYNC for current log top
    | ChkptTimeout({ log_idx: int }) // chkptTimeout[k] expired (§5.6.1): send TIMEOUT for k
    | RepairViewTimeout         // fired when REPAIR leader fails to propose in time

  // No custom effects beyond standard Choreo broadcast / send
  type CustomEffects = ()

  // No global extensions needed
  type Extensions = ()

  // ===========================================================================
  // CHOREO SETUP
  // ===========================================================================
  type LocalState   = choreo::LocalState[Node, StateFields]
  type LocalContext = choreo::LocalContext[Node, StateFields, Message, Event, Extensions]
  type Transition   = choreo::Transition[Node, StateFields, Message, Event, CustomEffects]
  type GlobalContext = choreo::GlobalContext[Node, StateFields, Message, Event, Extensions]

  // ===========================================================================
  // PROTOCOL CONFIGURATION
  // ===========================================================================

  pure val MAX_BYZ: int = 1     // f: max Byzantine faults
  pure val MAX_DIV: int = 1     // p: max diverged replicas tolerated on fast path
  // n = 3f + 2p + 1 = 3(1) + 2(1) + 1 = 6 replicas
  pure val NODES: Set[Node] = Set("r0", "r1", "r2", "r3", "r4", "r5")

  // R1: Fixed ordered list for round-robin leader election
  pure val NODES_ORDERED: List[Node] = List("r0", "r1", "r2", "r3", "r4", "r5")

  // R2: Byzantine fault injection constants
  // FAULTY_NODES must be a subset of NODES with |FAULTY_NODES| <= MAX_BYZ
  pure val FAULTY_NODES: Set[Node] = Set("r5")
  pure val CORRECT_NODES: Set[Node] = NODES.filter(n => not(FAULTY_NODES.contains(n)))

  // Quorum thresholds (§5.1, §5.4, §5.6)
  pure val n_replicas: int  = NODES.size()
  pure val fast_quorum: int = n_replicas - MAX_DIV   // n-p
  pure val byz_quorum: int  = n_replicas - MAX_BYZ   // n-f
  pure val f_plus_1: int    = MAX_BYZ + 1            // f+1

  // ===========================================================================
  // HELPER FUNCTIONS
  // ===========================================================================

  // Round-robin leader election: leader for view v is NODES_ORDERED[v mod n_replicas]
  pure def is_leader(node: Node, view: int): bool =
    NODES_ORDERED.nth(view % n_replicas) == node

  // Hash of the entry at index k in the store
  pure def log_hash_at(store: KVStore, k: int): int =
    store.get(k).op_hash + store.get(k).prev_hash

  // Hash of the top entry (latest) in the store
  pure def log_top_hash(store: KVStore, kv_size: int): int =
    if (kv_size > 0) log_hash_at(store, kv_size - 1) else 0

  // Collect SYNC senders that agree on (round, log_idx, log_hash) — "consistent" SYNCs
  pure def consistent_syncs(
    messages: Set[Message], round: int, log_idx: int, log_hash: int
  ): Set[Node] =
    messages.filterMap(m => match m {
      | Sync(s) =>
        if (s.round == round and s.log_idx == log_idx and s.log_hash == log_hash)
          Some(s.from)
        else None
      | _ => None
    })

  // Collect all SYNC senders for (round, log_idx) regardless of hash
  pure def all_sync_senders(messages: Set[Message], round: int, log_idx: int): Set[Node] =
    messages.filterMap(m => match m {
      | Sync(s) =>
        if (s.round == round and s.log_idx == log_idx) Some(s.from) else None
      | _ => None
    })

  // Collect CHECKPOINT senders for (round, log_idx)
  pure def checkpoint_senders(messages: Set[Message], round: int, log_idx: int): Set[Node] =
    messages.filterMap(m => match m {
      | Checkpoint(c) =>
        if (c.round == round and c.log_idx == log_idx) Some(c.from) else None
      | _ => None
    })

  // Collect TIMEOUT senders for (round, log_idx)
  pure def timeout_senders(messages: Set[Message], round: int, log_idx: int): Set[Node] =
    messages.filterMap(m => match m {
      | TimeoutMsg(t) =>
        if (t.round == round and t.log_idx == log_idx) Some(t.from) else None
      | _ => None
    })

  // Collect LOG message senders for (view, round)
  pure def log_msg_senders(messages: Set[Message], view: int, round: int): Set[Node] =
    messages.filterMap(m => match m {
      | LogMsg(l) =>
        if (l.view == view and l.round == round) Some(l.from) else None
      | _ => None
    })

  // Collect REPAIR-PREPARE senders for (view, round, history_hash)
  pure def repair_prepare_senders(
    messages: Set[Message], view: int, round: int, history_hash: int
  ): Set[Node] =
    messages.filterMap(m => match m {
      | RepairPrepare(p) =>
        if (p.view == view and p.round == round and p.history_hash == history_hash)
          Some(p.from)
        else None
      | _ => None
    })

  // Collect REPAIR-COMMIT senders for (view, round, history_hash)
  pure def repair_commit_senders(
    messages: Set[Message], view: int, round: int, history_hash: int
  ): Set[Node] =
    messages.filterMap(m => match m {
      | RepairCommit(c) =>
        if (c.view == view and c.round == round and c.history_hash == history_hash)
          Some(c.from)
        else None
      | _ => None
    })

  // Collect REPAIR-DONE senders for (view, round)
  pure def repair_done_senders(messages: Set[Message], view: int, round: int): Set[Node] =
    messages.filterMap(m => match m {
      | RepairDone(d) =>
        if (d.view == view and d.round == round) Some(d.from) else None
      | _ => None
    })

  // Check whether a REPAIR-HISTORY message exists for (view, round)
  pure def has_repair_history(messages: Set[Message], view: int, round: int): bool =
    messages.exists(m => match m {
      | RepairHistory(h) => h.view == view and h.round == round
      | _ => false
    })

  // Extract the history_hash from REPAIR-HISTORY for (view, round). Returns 0 if not found.
  // Guard with has_repair_history before using.
  pure def get_history_hash(messages: Set[Message], view: int, round: int): int =
    messages.filterMap(m => match m {
      | RepairHistory(h) =>
        if (h.view == view and h.round == round) Some(h.history_hash) else None
      | _ => None
    }).fold(0, (acc, h) => if (h > acc) h else acc)

  // Conflict-proof condition (§5.6.1): n - m < (n-p) - c
  // total_syncs = m (all received), consistent_count = c (largest consistent group)
  pure def conflict_proof_possible(total_syncs: int, consistent_count: int): bool =
    (n_replicas - total_syncs) < (fast_quorum - consistent_count)

  // Detect equivocation: two distinct LogMsg messages from the same sender
  // in the same (view, round) with differing log_hash values.
  pure def log_equivocation_in(node: Node, messages: Set[Message]): bool =
    messages.exists(m1 => messages.exists(m2 => match m1 {
      | LogMsg(a) => match m2 {
        | LogMsg(b) =>
          a.from == node and b.from == node and
          a.view == b.view and a.round == b.round and
          a.log_hash != b.log_hash
        | _ => false
      }
      | _ => false
    }))

  // Detect equivocation: two distinct RepairPrepare messages from the same sender
  // in the same (view, round) with differing history_hash values.
  pure def prepare_equivocation_in(node: Node, messages: Set[Message]): bool =
    messages.exists(m1 => messages.exists(m2 => match m1 {
      | RepairPrepare(a) => match m2 {
        | RepairPrepare(b) =>
          a.from == node and b.from == node and
          a.view == b.view and a.round == b.round and
          a.history_hash != b.history_hash
        | _ => false
      }
      | _ => false
    }))

  // ===========================================================================
  // FAST PATH TRANSITIONS (§5.2)
  // ===========================================================================

  // Speculatively execute the next request: insert into kv_store and broadcast SPEC-REPLY
  pure def listen_speculate(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    if (st.phase == Speculative) Set(st.kv_size) else Set()
  }

  pure def handle_speculate(ctx: LocalContext, log_idx: int): Transition = {
    val st = ctx.state
    val prev_h = if (log_idx > 0) log_hash_at(st.kv_store, log_idx - 1) else 0
    val new_entry: KVEntry = { op_hash: log_idx, result: log_idx, prev_hash: prev_h }
    val new_store = st.kv_store.put(log_idx, new_entry)
    val new_hash = log_hash_at(new_store, log_idx)
    {
      effects: Set(choreo::Broadcast(SpecReply({
        from: st.process_id,
        round: st.round,
        log_idx: log_idx,
        log_hash: new_hash
      }))),
      post_state: { ...st, kv_size: st.kv_size + 1, kv_store: new_store }
    }
  }

  // ===========================================================================
  // CHECKPOINT SUB-PROTOCOL (§5.4)
  // ===========================================================================

  // Broadcast SYNC when log reaches sync threshold or SyncTimeout fires
  pure def listen_send_sync(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    val has_sync_timeout = ctx.events.exists(e => match e {
      | SyncTimeout => true
      | _ => false
    })
    if (st.phase == Speculative and st.kv_size > 0 and
        (has_sync_timeout or st.kv_size > st.chkpt_idx)) {
      Set(st.kv_size - 1)
    } else {
      Set()
    }
  }

  pure def handle_send_sync(ctx: LocalContext, log_idx: int): Transition = {
    val st = ctx.state
    {
      effects: Set(choreo::Broadcast(Sync({
        from: st.process_id,
        round: st.round,
        log_idx: log_idx,
        log_hash: log_top_hash(st.kv_store, st.kv_size),
        eta_star: st.eta_star
      }))),
      post_state: st
    }
  }

  // Form CHECKPOINT upon receiving fast_quorum consistent SYNCs for current log top
  pure def listen_form_checkpoint(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    if (st.phase == Speculative and st.kv_size > 0) {
      val log_idx = st.kv_size - 1
      val quorum = consistent_syncs(ctx.messages, st.round, log_idx, log_top_hash(st.kv_store, st.kv_size))
      if (quorum.size() >= fast_quorum) Set(log_idx) else Set()
    } else {
      Set()
    }
  }

  pure def handle_form_checkpoint(ctx: LocalContext, log_idx: int): Transition = {
    val st = ctx.state
    {
      effects: Set(choreo::Broadcast(Checkpoint({
        from: st.process_id,
        round: st.round,
        log_idx: log_idx,
        log_hash: log_top_hash(st.kv_store, st.kv_size)
      }))),
      post_state: { ...st, chkpt_idx: log_idx, chkpt_hash: log_top_hash(st.kv_store, st.kv_size) }
    }
  }

  // ===========================================================================
  // ALIGN ENTRY: see f+1 conflicting CHECKPOINTs (§5.4 "Align" outcome)
  // ===========================================================================

  // A replica detects divergence when it sees f+1 CHECKPOINT messages for chkpt_idx.
  // In the real protocol, these would have a hash inconsistent with the replica's own;
  // here we abstract and trigger ALIGN whenever f+1 CHECKPOINTs are seen.
  pure def listen_enter_align(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    if (st.phase == Speculative) {
      val chkpts = checkpoint_senders(ctx.messages, st.round, st.chkpt_idx)
      if (chkpts.size() >= f_plus_1) Set(st.chkpt_idx) else Set()
    } else {
      Set()
    }
  }

  pure def handle_enter_align(ctx: LocalContext, chkpt_idx: int): Transition = {
    val st = ctx.state
    {
      effects: Set(choreo::Broadcast(StateRequest({ from: st.process_id, chkpt_idx: chkpt_idx }))),
      post_state: { ...st, phase: Aligning }
    }
  }

  // ===========================================================================
  // CONFLICT PROOF: checkpoint impossible → enter REPAIR (§5.6.1)
  // ===========================================================================

  pure def listen_conflict_proof(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    if (st.phase == Speculative and st.kv_size > 0 and not(st.in_repair)) {
      val log_idx  = st.kv_size - 1
      val total    = all_sync_senders(ctx.messages, st.round, log_idx).size()
      val consist  = consistent_syncs(ctx.messages, st.round, log_idx, log_top_hash(st.kv_store, st.kv_size)).size()
      if (conflict_proof_possible(total, consist)) Set(log_idx) else Set()
    } else {
      Set()
    }
  }

  pure def handle_conflict_proof(ctx: LocalContext, log_idx: int): Transition = {
    val st = ctx.state
    {
      effects: Set(
        choreo::Broadcast(ConflictProof({ round: st.round, log_idx: log_idx })),
        choreo::Broadcast(LogMsg({
          from: st.process_id,
          view: st.view,
          round: st.round,
          log_hash: log_top_hash(st.kv_store, st.kv_size)
        }))
      ),
      post_state: { ...st, phase: CollectingLogs, in_repair: true }
    }
  }

  // ===========================================================================
  // TIMEOUT PATH: entering REPAIR via chkptTimeout (§5.6.1)
  // ===========================================================================

  // ChkptTimeout fires → broadcast TIMEOUT
  pure def listen_chkpt_timeout(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    if (st.phase == Speculative and not(st.in_repair)) {
      ctx.events.filterMap(e => match e {
        | ChkptTimeout(t) => Some(t.log_idx)
        | _ => None
      })
    } else {
      Set()
    }
  }

  pure def handle_chkpt_timeout(ctx: LocalContext, log_idx: int): Transition = {
    val st = ctx.state
    {
      effects: Set(choreo::Broadcast(TimeoutMsg({
        from: st.process_id, round: st.round, log_idx: log_idx
      }))),
      post_state: st
    }
  }

  // Collect f+1 TIMEOUT messages → broadcast TIMEOUT-PROOF + enter REPAIR
  pure def listen_timeout_proof(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    if (st.phase == Speculative and not(st.in_repair) and st.kv_size > 0) {
      val log_idx  = st.kv_size - 1
      val timeouts = timeout_senders(ctx.messages, st.round, log_idx)
      if (timeouts.size() >= f_plus_1) Set(log_idx) else Set()
    } else {
      Set()
    }
  }

  pure def handle_timeout_proof(ctx: LocalContext, log_idx: int): Transition = {
    val st = ctx.state
    {
      effects: Set(
        choreo::Broadcast(TimeoutProof({ round: st.round, log_idx: log_idx })),
        choreo::Broadcast(LogMsg({
          from: st.process_id,
          view: st.view,
          round: st.round,
          log_hash: log_top_hash(st.kv_store, st.kv_size)
        }))
      ),
      post_state: { ...st, phase: CollectingLogs, in_repair: true }
    }
  }

  // Upon receiving TIMEOUT-PROOF or CONFLICT-PROOF from others, enter REPAIR
  // (ensures all correct replicas follow once one enters)
  pure def listen_enter_repair(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    if (not(st.in_repair)) {
      val has_proof = ctx.messages.exists(m => match m {
        | TimeoutProof(_) => true
        | ConflictProof(_) => true
        | _ => false
      })
      if (has_proof) Set(st.round) else Set()
    } else {
      Set()
    }
  }

  pure def handle_enter_repair(ctx: LocalContext, round: int): Transition = {
    val st = ctx.state
    {
      effects: Set(choreo::Broadcast(LogMsg({
        from: st.process_id,
        view: st.view,
        round: st.round,
        log_hash: log_top_hash(st.kv_store, st.kv_size)
      }))),
      post_state: { ...st, phase: CollectingLogs, in_repair: true }
    }
  }

  // ===========================================================================
  // ALIGNMENT SUB-PROTOCOL (§5.5)
  // ===========================================================================

  // Respond to STATE-REQUEST with current checkpoint state
  pure def listen_state_request(ctx: LocalContext): Set[Node] =
    ctx.messages.filterMap(m => match m {
      | StateRequest(req) => Some(req.from)
      | _ => None
    })

  pure def handle_state_request(ctx: LocalContext, requester: Node): Transition = {
    val st = ctx.state
    {
      effects: Set(choreo::Send({
        to: requester,
        message: StateReply({
          from: st.process_id,
          chkpt_idx: st.chkpt_idx,
          chkpt_hash: st.chkpt_hash
        })
      })),
      post_state: st
    }
  }

  // Receive STATE-REPLY → reset to checkpoint, reapply uncommitted requests, resume Speculative
  pure def listen_state_reply(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    if (st.phase == Aligning) {
      ctx.messages.filterMap(m => match m {
        | StateReply(r) => Some(r.chkpt_idx)
        | _ => None
      })
    } else {
      Set()
    }
  }

  pure def handle_state_reply(ctx: LocalContext, chkpt_idx: int): Transition = {
    val st = ctx.state
    val recovery_store = 0.to(chkpt_idx).mapBy(k => { op_hash: k, result: k, prev_hash: k })
    {
      effects: Set(),
      post_state: {
        ...st,
        phase: Speculative,
        chkpt_idx: chkpt_idx,
        chkpt_hash: if (chkpt_idx >= 0) log_hash_at(recovery_store, chkpt_idx) else 0,
        kv_size: chkpt_idx + 1,
        kv_store: recovery_store,
        start_idx: chkpt_idx
      }
    }
  }

  // ===========================================================================
  // REPAIR SUB-PROTOCOL (§5.6.2 – §5.6.3)
  // ===========================================================================

  // Collect byz_quorum LOG messages → propose REPAIR-HISTORY (leader only)
  pure def listen_broadcast_history(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    if (st.phase == CollectingLogs and is_leader(st.process_id, st.view)) {
      val log_senders = log_msg_senders(ctx.messages, st.view, st.round)
      if (log_senders.size() >= byz_quorum) {
        // Abstract history_hash = number of contributing replicas
        Set(log_senders.size())
      } else {
        Set()
      }
    } else {
      Set()
    }
  }

  pure def handle_broadcast_history(ctx: LocalContext, history_hash: int): Transition = {
    val st = ctx.state
    {
      effects: Set(choreo::Broadcast(RepairHistory({
        from: st.process_id,
        view: st.view,
        round: st.round,
        history_hash: history_hash
      }))),
      post_state: { ...st, phase: PreparePhase, proposed_history_hash: history_hash }
    }
  }

  // Receive REPAIR-HISTORY → broadcast REPAIR-PREPARE
  pure def listen_repair_prepare(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    val in_relevant_phase = st.phase == CollectingLogs or st.phase == PreparePhase
    if (in_relevant_phase and has_repair_history(ctx.messages, st.view, st.round)) {
      Set(get_history_hash(ctx.messages, st.view, st.round))
    } else {
      Set()
    }
  }

  pure def handle_repair_prepare(ctx: LocalContext, history_hash: int): Transition = {
    val st = ctx.state
    {
      effects: Set(choreo::Broadcast(RepairPrepare({
        from: st.process_id,
        view: st.view,
        round: st.round,
        history_hash: history_hash
      }))),
      post_state: { ...st, phase: PreparePhase, proposed_history_hash: history_hash }
    }
  }

  // Collect byz_quorum matching REPAIR-PREPAREs → broadcast REPAIR-COMMIT
  pure def listen_repair_commit(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    if (st.phase == PreparePhase) {
      val prepares = repair_prepare_senders(
        ctx.messages, st.view, st.round, st.proposed_history_hash
      )
      if (prepares.size() >= byz_quorum) Set(st.proposed_history_hash) else Set()
    } else {
      Set()
    }
  }

  pure def handle_repair_commit(ctx: LocalContext, history_hash: int): Transition = {
    val st = ctx.state
    {
      effects: Set(choreo::Broadcast(RepairCommit({
        from: st.process_id,
        view: st.view,
        round: st.round,
        history_hash: history_hash
      }))),
      post_state: { ...st, phase: CommitPhase }
    }
  }

  // Collect byz_quorum matching REPAIR-COMMITs → apply merged log, broadcast REPAIR-DONE
  // and COMMITTED-REPLY, then advance to round i+1 (§5.6.3)
  pure def listen_apply_repair(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    if (st.phase == CommitPhase) {
      val commits = repair_commit_senders(
        ctx.messages, st.view, st.round, st.proposed_history_hash
      )
      if (commits.size() >= byz_quorum) Set(st.proposed_history_hash) else Set()
    } else {
      Set()
    }
  }

  pure def handle_apply_repair(ctx: LocalContext, history_hash: int): Transition = {
    val st = ctx.state
    val new_round    = st.round + 1
    val merged_size  = history_hash
    val merged_store = 0.to(merged_size - 1).mapBy(k => { op_hash: k, result: k, prev_hash: k })
    {
      effects: Set(
        choreo::Broadcast(RepairDone({
          from: st.process_id,
          view: st.view,
          round: st.round,
          history_hash: history_hash
        })),
        choreo::Broadcast(CommittedReply({
          from: st.process_id,
          round: st.round,
          log_idx: st.kv_size
        }))
      ),
      post_state: {
        ...st,
        phase: Speculative,
        round: new_round,
        start_idx: merged_size,
        kv_size: merged_size,
        kv_store: merged_store,
        in_repair: false,
        proposed_history_hash: 0
      }
    }
  }

  // Catchup: replica sees f+1 REPAIR-DONE messages and has REPAIR-HISTORY → exit REPAIR
  // This ensures a slow correct replica can catch up (§5.6.2 last paragraph)
  pure def listen_repair_done_catchup(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    val in_repair_phase =
      st.phase == CollectingLogs or
      st.phase == PreparePhase   or
      st.phase == CommitPhase
    if (in_repair_phase) {
      val dones    = repair_done_senders(ctx.messages, st.view, st.round)
      val has_hist = has_repair_history(ctx.messages, st.view, st.round)
      if (dones.size() >= f_plus_1 and has_hist) Set(st.round) else Set()
    } else {
      Set()
    }
  }

  pure def handle_repair_done_catchup(ctx: LocalContext, round: int): Transition = {
    val st = ctx.state
    {
      effects: Set(),
      post_state: {
        ...st,
        phase: Speculative,
        round: st.round + 1,
        start_idx: st.kv_size,
        in_repair: false,
        proposed_history_hash: 0
      }
    }
  }

  // Internal view-change for REPAIR — if leader is faulty, replicas can increment view
  pure def listen_view_change(ctx: LocalContext): Set[int] = {
    val st = ctx.state
    val has_timeout = ctx.events.exists(e => match e {
      | RepairViewTimeout => true
      | _ => false
    })
    if (st.phase == CollectingLogs and has_timeout) Set(st.view + 1) else Set()
  }

  pure def handle_view_change(ctx: LocalContext, new_view: int): Transition = {
    val st = ctx.state
    {
      effects: Set(choreo::Broadcast(LogMsg({
        from: st.process_id,
        view: new_view,
        round: st.round,
        log_hash: log_top_hash(st.kv_store, st.kv_size)
      }))),
      post_state: { ...st, view: new_view }
    }
  }

  // ===========================================================================
  // MAIN LISTENER
  // ===========================================================================

  pure def main_listener(ctx: LocalContext): Set[Transition] =
    Set(
      // Fast path
      choreo::cue(ctx, listen_speculate, handle_speculate),
      choreo::cue(ctx, listen_send_sync, handle_send_sync),
      choreo::cue(ctx, listen_form_checkpoint, handle_form_checkpoint),

      // Checkpoint outcomes
      choreo::cue(ctx, listen_enter_align, handle_enter_align),
      choreo::cue(ctx, listen_conflict_proof, handle_conflict_proof),

      // Timeout → REPAIR pathway
      choreo::cue(ctx, listen_chkpt_timeout, handle_chkpt_timeout),
      choreo::cue(ctx, listen_timeout_proof, handle_timeout_proof),
      choreo::cue(ctx, listen_enter_repair, handle_enter_repair),

      // Alignment
      choreo::cue(ctx, listen_state_request, handle_state_request),
      choreo::cue(ctx, listen_state_reply, handle_state_reply),

      // REPAIR phases
      choreo::cue(ctx, listen_broadcast_history, handle_broadcast_history),
      choreo::cue(ctx, listen_repair_prepare, handle_repair_prepare),
      choreo::cue(ctx, listen_repair_commit, handle_repair_commit),
      choreo::cue(ctx, listen_apply_repair, handle_apply_repair),
      choreo::cue(ctx, listen_repair_done_catchup, handle_repair_done_catchup),

      // REPAIR view-change
      choreo::cue(ctx, listen_view_change, handle_view_change)
    ).flatten()

  // ===========================================================================
  // CUSTOM EFFECT PROCESSOR (no custom effects in this spec)
  // ===========================================================================

  pure def apply_custom_effect(ctx: GlobalContext, effect: CustomEffects): GlobalContext = ctx

  // ===========================================================================
  // INITIALIZATION
  // ===========================================================================

  pure def initialize(node: Node): LocalState = {
    process_id: node,
    phase: Speculative,
    round: 0,
    view: 0,
    start_idx: 0,
    kv_size: 0,
    kv_store: Map(),
    chkpt_idx: 0,
    chkpt_hash: 0,
    eta_star: 0,
    in_repair: false,
    proposed_history_hash: 0
  }

  // Pre-computed equivocating messages from faulty nodes.
  // Each faulty node sends two LogMsg variants with the same (view=0, round=0)
  // but different log_hash values (0 and 999) to model equivocation.
  pure val byzantine_log_msgs: Set[Message] =
    FAULTY_NODES.map(f => Set(
      LogMsg({ from: f, view: 0, round: 0, log_hash: 0 }),
      LogMsg({ from: f, view: 0, round: 0, log_hash: 999 })
    )).flatten()

  pure val byzantine_prepare_msgs: Set[Message] =
    FAULTY_NODES.map(f => Set(
      RepairPrepare({ from: f, view: 0, round: 0, history_hash: 0 }),
      RepairPrepare({ from: f, view: 0, round: 0, history_hash: 888 })
    )).flatten()

  pure val all_byzantine_messages: Set[Message] =
    byzantine_log_msgs.union(byzantine_prepare_msgs)

  action init = choreo::init({
    system: NODES.mapBy(n => initialize(n)),
    messages: NODES.mapBy(n => all_byzantine_messages),
    events: NODES.mapBy(n => Set()),
    extensions: ()
  })

  action step = choreo::step(main_listener, apply_custom_effect)

  // ===========================================================================
  // WIRING
  // ===========================================================================

  val s = choreo::s

  action step_with(v: Node, listener: LocalContext => Set[Transition]): bool =
    choreo::step_with(v, listener, apply_custom_effect)

  action step_deterministic(v: Node, listener: LocalContext => Transition): bool =
    choreo::step_deterministic(v, listener, apply_custom_effect)

  def with_cue(process, listen_fn, params) =
    choreo::with_cue(process, listen_fn, params)

  action perform(cue_ctx, upon_fn) =
    choreo::perform(cue_ctx, upon_fn, apply_custom_effect)

  // ===========================================================================
  // INVARIANTS
  // ===========================================================================

  // Safety: Checkpoint agreement (§5.4)
  // Any two correct replicas that have checkpointed the same index k agree on H(k).
  // This reflects that (n-p) consistent SYNCs guarantee prefix consistency.
  val checkpoint_agreement =
    CORRECT_NODES.forall(r1 => CORRECT_NODES.forall(r2 => {
      val st1 = s.system.get(r1)
      val st2 = s.system.get(r2)
      (st1.chkpt_idx == st2.chkpt_idx and st1.chkpt_idx > 0) implies
        (st1.chkpt_hash == st2.chkpt_hash)
    }))

  // Safety: REPAIR history agreement (§5.6.2)
  // Any two correct replicas in CommitPhase for the same (view, round) must vote on the same h(H).
  // Ensured because a single prepared value can gather (n-f) prepares.
  val repair_history_agreement =
    CORRECT_NODES.forall(r1 => CORRECT_NODES.forall(r2 => {
      val st1 = s.system.get(r1)
      val st2 = s.system.get(r2)
      (st1.phase == CommitPhase and st2.phase == CommitPhase and
       st1.view == st2.view and st1.round == st2.round) implies
        (st1.proposed_history_hash == st2.proposed_history_hash)
    }))

  // Safety: Phase consistency
  // A correct replica in REPAIR (in_repair = true) must not be in Speculative or Aligning phase.
  val phase_consistency =
    CORRECT_NODES.forall(r => {
      val st = s.system.get(r)
      st.in_repair implies (st.phase != Speculative and st.phase != Aligning)
    })

  // Safety: Round monotonicity helper
  // start_idx is non-negative (structural sanity check)
  val start_idx_non_negative =
    CORRECT_NODES.forall(r => s.system.get(r).start_idx >= 0)

  // Safety: Only the elected leader may have broadcast REPAIR-HISTORY for the current (view, round).
  // A replica in PreparePhase must have seen a RepairHistory message from the leader.
  val only_leader_proposes_history =
    NODES.forall(r => {
      val st = s.system.get(r)
      val msgs = s.messages.get(r)
      (st.phase == PreparePhase) implies
        msgs.exists(m => match m {
          | RepairHistory(h) =>
            h.view == st.view and h.round == st.round and
            is_leader(h.from, st.view)
          | _ => false
        })
    })

  // Safety: Honest replicas never equivocate.
  // A correct node never sends two LogMsg or RepairPrepare messages with
  // the same (view, round) but different content fields.
  val no_honest_equivocation =
    CORRECT_NODES.forall(r =>
      NODES.forall(receiver => {
        val msgs = s.messages.get(receiver)
        not(log_equivocation_in(r, msgs)) and
        not(prepare_equivocation_in(r, msgs))
      })
    )

  // ===========================================================================
  // WITNESS SCENARIOS (liveness / reachability)
  // ===========================================================================

  // Witness: All replicas initialized in Speculative phase with empty kv_store
  run init_test = init
    .expect(NODES.forall(r => {
      val st = s.system.get(r)
      st.phase == Speculative and st.kv_size == 0 and st.round == 0
    }))

  // Witness: A replica can speculatively execute (kv_store grows)
  run can_speculate_test = init
    .then(step)
    .expect(NODES.exists(r => s.system.get(r).kv_size > 0))

  // Witness: Multiple steps are possible (system makes progress)
  run can_take_steps_test = init
    .then(step)
    .then(step)
    .then(step)
    .expect(true)

  // Liveness witness: can_checkpoint — a checkpoint is ever formed
  // Run: quint run specs/aspen.qnt --main=aspen --invariant=no_checkpoint --max-steps=30
  val no_checkpoint = NODES.forall(r => s.system.get(r).chkpt_idx == 0)

  // Liveness witness: can_enter_repair — REPAIR is ever triggered
  // Run: quint run specs/aspen.qnt --main=aspen --invariant=never_in_repair --max-steps=30
  val never_in_repair = NODES.forall(r => not(s.system.get(r).in_repair))

  // Liveness witness: can_exit_repair — a replica exits REPAIR (round advances)
  // Run: quint run specs/aspen.qnt --main=aspen --invariant=never_exits_repair --max-steps=50
  val never_exits_repair = NODES.forall(r => s.system.get(r).round == 0)

  //  Witness: Byzantine equivocation is detectable in the network
  // Run: quint run specs/aspen.qnt --main=aspen --invariant=no_equivocation_visible --max-steps=5
  // This witness should be violated immediately after init (step 0),
  // confirming Byzantine messages are injected.
  val no_equivocation_visible =
    NODES.forall(receiver =>
      FAULTY_NODES.forall(f =>
        not(log_equivocation_in(f, s.messages.get(receiver)))
      )
    )
}
