# Architecture and queue selection

Status: **PgQue selected; pending-flag implementation removed**. The
[requirements](requirements.md) are the constraints. Pinned PgQue snapshot
delivery is runnable in the [local lab](../local/README.md).
Source-level Kafka transaction fencing is implemented and fault-tested;
consumer freshness remains future work. See [ownership.md](ownership.md).

## Boundaries

```text
PostgreSQL business transaction
  business row mutation
  AFTER trigger → logged event {identity, source tx, table, op, OLD, NEW}
  COMMIT or ROLLBACK together

RowRelay
  acquire ownership → select a bounded batch → encode → publish to Kafka
  → wait for every required ACK → acknowledge source batch
  → publish/complete a valid progress barrier under the chosen protocol

Application consumer
  decode → validate → apply idempotently → persist consumed progress
  → expose freshness only for successfully applied barriers
```

Capture performs no network I/O. Kafka and Registry outages must not be converted
into source event loss. Queue growth during an outage is real disk usage; a full
database may reject source writes. Retention limits must never silently discard
pending records to avoid that consequence.

Registry metadata is cached outside the per-record hot path. Database fetching,
encoding, producer buffering, and acknowledgement have bounded capacities and
explicit cancellation. Do not hold a SQL row-lock transaction open while waiting
for Kafka merely to avoid designing the delivery acknowledgement boundary.

## Selected: PgQ-style MVCC snapshot batches

The initial pending-flag implementation was evaluated and removed. Under the
corrected held-snapshot workload its p99 was 36.6 seconds versus PgQue's 206 ms.
The owner selected PgQue; future benchmarks compare it with the external pg-boss
baseline, without maintaining or rerunning the retired implementation.

Events are append-only and carry a **top-level** source transaction identity.
A ticker records PostgreSQL snapshots. A batch contains events whose source
transactions were not visible in the previous snapshot but are visible in the
next one. Acknowledgement updates consumer/batch position instead of every event.

This is a difference between **transaction visibility snapshots**, not repeated
full snapshots of business tables. A real implementation uses indexed transaction
ranges and in-progress set differences; it must not scan the whole event history
for each tick. Snapshot membership alone does not define row-event ordering.

Potential advantages:

- No per-event published-flag update on append-only event tables.
- Explicit visibility boundaries, including transactions that commit late.
- Rotation of obsolete event segments instead of per-event DELETE cleanup.

Remaining acceptance areas beyond the local selection:

- Top-level xid semantics, savepoints, long transactions, wraparound, and restore epochs.
- Reconstructible batch membership and deterministic order on retry.
- Safe segment rotation with in-flight transactions and lagging consumers.
- Source barriers that cannot overtake missing publication or consumer application.
- Tick/metadata write overhead and tail latency under sustained load.
- Installation under the actual allowed SQL privileges.

Audit existing PgQ/PgQue SQL and licensing before implementing a new engine.
PgQ's mature algorithm does not automatically establish the maturity of a newer
port. Do not require `pg_cron` when an external ticker/maintenance loop suffices.

## Ownership, ordering, and barriers

The implemented protocol combines a dedicated PostgreSQL session lock with a
stable Kafka transactional ID per source epoch. A successor initializes a new
producer epoch before reading work. Every Kafka chunk commits before source ACK;
ACK runs on the original source connection and verifies that its lock remains
held. Failed producers are terminal. Consumers require `read_committed`.

[Ownership and failure tests](ownership.md) describe this protocol, source-level
standby replicas, ambiguous commits and its limits. Earlier local performance
cohorts predate transactions; their results do not measure this implementation.
Closed-boundary barriers and their consumer-side application protocol are still
required before any cache-freshness guarantee.

One Kafka partition orders accepted records, not PostgreSQL commits. Specify the
source order separately, preserve dependent same-row mutations, and test retries
that repeat an older batch after part of it has already arrived. Do not promise
an atomic cross-table consumer view simply because source writes were atomic.

Checkpoint records travel on the same ordered stream. Their meaning is a closed
source boundary, not “the relay loop ran at this wall-clock time.” The source
queue's checkpoint and a consumer's successfully applied checkpoint are distinct.
An old-row timestamp is neither a reliable commit timestamp nor a safe cursor.

## Package and deployment shape

Start with one binary under `cmd/row-relay`. Add concrete implementation packages
under `internal/` when the first slice needs them. SQL migrations and the protocol
schema should live beside the implementation that owns them, with runnable
integration fixtures. Avoid pre-creating driver interfaces or plugin registries.

The local lab will pin PostgreSQL, Kafka, and Registry image versions/digests.
Production packaging follows correctness and shutdown tests, not the bootstrap.
Installation/migration is an explicit command or deployment step; startup must
not silently alter business tables using an overprivileged runtime credential.

See [research](research.md) for prior art and [benchmarks](benchmarks.md) for the
remaining resource, compatibility and operational acceptance program.
