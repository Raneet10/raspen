# raspen

A Rust implementation of the [Aspen BFT](https://arxiv.org/pdf/2601.03390) replicated state machine protocol

Aspen is a (almost) leaderless Byzantine fault-tolerant consensus protocol that combines a speculative fast path with a REPAIR sub-protocol for recovering from checkpoint failures. 

## Crates

| Crate | Purpose |
|-------|---------|
| `raspen-types` | All protocol types: `Message`, `Phase`, `ReplicaState`, `Config`, `Effect`, `Event` |
| `raspen-storage` | Replicated log store — in-memory (`MemKvStore`) by default, RocksDB via feature flag |
| `raspen-consensus` | Core `Replica` state machine and all sub-protocol handlers |
| `raspen-sequencer` | Broadcast proxy that assigns sequence numbers to client requests |

## Protocol overview

1. **Fast path** — the sequencer assigns a sequence number η to each request and broadcasts it to all replicas. Each replica appends the entry to its log and replies speculatively to the client. The client commits once it sees `n−p` matching replies.

2. **Checkpoint** — replicas periodically broadcast SYNC messages. When `n−p` consistent SYNCs are collected a CHECKPOINT is formed, advancing the committed prefix.

3. **Alignment** — a replica that detects log divergence (via `f+1` conflicting CHECKPOINTs) requests a state transfer, rebuilds its log from the recovered checkpoint, and resumes normal operation.

4. **REPAIR** — when a checkpoint stalls (timeout proof or conflict proof), all replicas enter a PBFT-style two-phase agreement to commit a merged log history, then return to the fast path.

## Building and testing

```bash
# Build
cargo build --workspace

# Run all tests
cargo test --workspace

# Enable RocksDB backend (requires libclang)
cargo build --workspace --features raspen-storage/rocksdb-backend

# Lint
cargo clippy --workspace -- -D warnings
```

## Formal specification

The protocol is derived from a Quint formal specification in `../specs/aspen.qnt`.
