# Implementation plan

The [requirements](../docs/requirements.md) are the contract. This document owns
milestone outcomes and dependencies; [todo.md](todo.md) owns execution status.
Do not treat a completed document as a completed relay feature.

## P0 — Repository foundation

**Outcome:** a buildable Go bootstrap with local/CI checks, scoped contracts,
prior-art notes, and a falsifiable correctness/benchmark plan.

**Verify:** `make check`, CLI help/version, explicit unsupported-start failure,
local documentation links, no secrets or generated results tracked.

## P1 — Durable JSON delivery slice and oracle

**Depends on:** P0.

**Deliver:** a disposable pinned PostgreSQL/Kafka lab; explicit capture SQL for
ordinary PK tables; a Go publisher using PgQue (the original pending-flag slice
was measured and retired); full Eventuate-compatible JSON images; bounded batches; all required
Kafka ACKs before batched source ACK. Include deterministic synthetic mutations
and an independent expected-event ledger.

The implemented slice uses pinned pgx and franz-go clients. Keep migrations
explicit and runtime privileges restricted.

**Verify:** C01–C06, basic C09/C10/C19/C20; committed insert/update/delete and
rollback; held low-ID late commit; crash after Kafka ACK. Publish the first
runnable integration command with the slice. This milestone has no HA or safe
cache-freshness claim yet.

## P2 — Progress, ownership, and consumer safety

**Depends on:** P1.

**Deliver:** a documented closed-boundary/barrier protocol, ownership and fencing
design, conditional source ACK, bounded shutdown/retry behavior, and a reference
idempotent consumer. Define replay gaps, restore epochs, and poison recovery.
An ORM/cache adapter can be a separate integration fixture.

**Verify:** C07–C13 and C17/C18. Exercise an old publisher resuming after takeover,
an ACK response lost after success, a poison event before a barrier, a stale
checkpoint, and a consumer apply failure. Record exactly what the barrier proves.

**Gate:** no HA/freshness advertising before these tests pass. Document any
baseline barrier deficiency rather than weakening the contract to match it.

## P3 — Reproducible A/B baseline

**Depends on:** P1; progress/failover measurements additionally require P2.

**Deliver:** pinned Eventuate A and equivalent Go B runners, common oracle and
consumer, saved configurations, raw artifact collection, and a reviewed report.
Measure source-write overhead, idle cost, fixed-load tails, capacity/backlog,
RSS/CPU, database churn, and recovery before attempting an optimization.

**Verify:** B01–B05/B07/B08/B12 screening, at least five steady-run repetitions,
same durability/compression, oracle reconciliation. Preregister candidate
acceptance limits and keep failed runs. Establish a representative deployment
budget; do not infer it from image size or configured heap limits.

## P4 — Snapshot queue evaluation and decision

**Depends on:** P2 and P3.

**Deliver:** review existing PgQ/PgQue SQL, installation rights, licensing,
transaction-ID handling, batching, maintenance, and recovery. Build the smallest
candidate C against the same capture/consumer workload. Avoid a home-grown
ID watermark or a full-history visibility scan.

**Verify:** candidate-specific C02/C03/C07/C16/C17, B08/B09/B11, indexed query plans,
multiple rotation cycles, and tick sweeps with an external Go ticker. Include
metadata CPU/WAL and retained disk, not just zero dead event tuples.

**Decision:** the owner selected PgQue after the held-snapshot measurements.
Pending is removed from runtime, installation, cleanup and future experiments.
Keep the historical evidence; finish PgQue's remaining operational acceptance.

## P5 — Typed Protobuf and Apicurio interoperability

**Depends on:** P2; wire-format work can proceed independently of P4, while the
final performance comparison uses the chosen queue and P3 harness.

**Deliver:** stable semantic envelope, selected row representation, versioned
Protobuf schema, exact Registry framing/reference handling, bounded schema cache,
and real JVM interoperability fixtures. Add a controlled JSON-to-Protobuf
consumer migration/replay plan.

**Verify:** C14/C15, numeric precision and NULL/presence, subtype/old-association
fixtures, cached/uncached Registry outage, old-schema replay. Run D against the
same queue/codec/settings as its JSON counterpart, including compressed batches.

## P6 — Operational hardening and release candidate

**Depends on:** P2–P5, including an explicit P4 decision even if it rejects C.

**Deliver:** validated configuration, source installer/upgrade procedure,
essential health/lag/backlog diagnostics, safe retention, recovery and restore
runbooks, graceful shutdown, and a small reproducible container/release build.
Document supported PostgreSQL/Kafka/Registry/JVM versions and table/type limits.

**Verify:** all applicable C01–C20; outage/burst/backlog-beyond-RAM tests; at least
two hours with multiple retention/rotation cycles; restricted-role installation;
clean upgrade/restart/replay; final controlled benchmark report. Verify real
consumer compatibility with released dependencies, not only local patches.

**Gate:** zero lost/phantom events and no barrier/fencing violations. Permitted
duplicates are measured and safe. Publish performance with reproducible evidence
and limitations. Releases and deployments require an explicit request.

## O1 — Business-outbox delivery in the same process (active)

**Depends on:** the implemented P1 transport. Generic CDC Protobuf (P5) is not a
dependency: business applications already supply complete wire bytes.

**Deliver:** an explicit transactional enqueue API, registered streams with fixed
topics, byte-preserving Kafka keys/values/headers and stable delivery IDs. Reuse
the pinned PgQue delivery loop rather than porting pg-boss internals.
Support several bounded CDC/outbox loops in one binary/process, with explicit
per-stream state and isolation of failures. Preserve existing immutable business
facts and application-level applied acknowledgements.

**Verify:** commit/rollback, low-ID late commits, binary/header fidelity, partial
Kafka success, replay after ACK failures, poison and resource limits, and CDC
plus multiple outbox streams in one process. Existing consumers must continue
receiving the original framing and IDs. A transport oracle precedes JVM/native
compatibility builds; those heavy builds run through CI.

## O2 — Ordered multi-replica delivery

**Depends on:** O1 and the ownership portion of P2.

**Deliver:** an explicit ownership unit (stream/shard), source ACK ownership
checks and broker-side stale-publisher fencing. Establish per-key enqueue order
and recovery semantics; a sequence, lease, advisory lock, or Kafka key alone is
not a complete ordering protocol. Independent keys must continue progressing
under the declared poison policy. Do not advertise horizontal throughput scaling
until multiple replicas actually share work safely.

**Verify:** competing replicas, takeover, delayed old sends, return of the stale
publisher, source ACK response loss, dependent revisions, shutdown and outages.
Keep the one-owner implementation explicitly limited until these pass.

## O3 — Outbox algorithm/resource decision

**Depends on:** O1; multi-replica comparisons additionally require O2.

**Compare:** the selected PgQue snapshot backend and a pinned official
pg-boss/Node transport baseline. The earlier pending comparison is complete;
do not maintain or schedule the retired backend.
Treat key-level claims/SKIP LOCKED as a candidate only when the required ordering
and ownership protocol are equivalent. No algorithm is selected by reputation.

**Measure:** prepared backlog of tens/hundreds of thousands of messages, steady
arrival/capacity ramp, held snapshots, large retained history and repeated
cleanup/rotation, mixed streams and one/multiple replicas. Record source write
cost, backlog age, CPU/RSS/cgroup memory, connections and tails. Preserve failed
runs and distinguish offered, committed, decoded and business-applied rates.
Account for the existing baseline's KafkaJS empty-queue timer defect separately.
The prior full business-chain rate is not the relay capacity baseline.

**Gate:** identical bytes, ACK/durability/compression and applicable ordering
guarantees; zero missing/corrupt/phantom events. Preregister limits per experiment.
Retain the simpler candidate when a measured advantage does not justify the
installation and operational costs. Current CDC measurements do not substitute
for business-outbox measurements.

## O4 — Migration and operational acceptance

**Depends on:** O1–O3 and applicable P6 gates.

**Deliver:** producer enqueue migrations, explicit backlog transfer/replay with
stable IDs, one deployment for registered flows, readiness/recovery/retention
runbooks, and a rollback path preserving facts and consumer offsets. Stop the
old owner before enabling the replacement; never reconstruct pending delivery
from all retained facts merely because delivery metadata was cleaned up.

**Gate:** managed-provider rights, real consumer compatibility and failure tests.
Transport changes must not enable new business revisions, backfills or unrelated
realtime outboxes. Shared cutover, commit/push/release/deploy still need explicit
authorization. The detailed migration context is in
[the supplied implementation request](unified-outbox-prompt.md).
