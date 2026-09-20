# Requirements

Status: **target contract**. The experimental JSON implementation covers a subset;
read [task status](../tasks/todo.md) and the [lab boundary](../local/README.md) for
actual coverage. Requirement IDs are stable references for tests and PRs.

## Scope

Build a small Go service that delivers committed PostgreSQL row changes to
Kafka on installations without logical replication privileges. Primary use
cases are cache invalidation and downstream change processing. It must observe
ordinary SQL writes regardless of which application issued them.

The first production scope is one configured source database and capture set,
one output topic with exactly one partition, and one active publisher. Multiple
independent deployments can serve different sources. HA ownership must be
verified before claiming automatic failover support.

## Capture

| ID | Requirement | Acceptance |
| --- | --- | --- |
| R01 | Capture INSERT, UPDATE, DELETE in the business transaction | Committed mutations arrive; aborted mutations and rolled-back savepoint work do not |
| R02 | Preserve full physical OLD/NEW values, including keys | DELETE, PK changes, FK moves, NULL, large values, and subtype/discriminator fixtures round-trip |
| R03 | Require no logical decoding, replication slot, superuser, or mandatory C extension | Installation and runtime suite pass with explicitly restricted roles |
| R04 | Make the capture start boundary and table coverage explicit | Writes after capture activation have no installation gap; unsupported tables/operations are rejected |
| R05 | Durable capture | Logged tables and normal PostgreSQL durability survive restart; trigger failure aborts the source write |

The installer role needs appropriate schema/table/function/trigger privileges;
the runtime role gets only the required queue, progress, and ownership access.
Managed-provider SQL privileges and PostgreSQL version must be tested, not
inferred from the absence of `REPLICATION`.

Initially support ordinary tables with declared primary keys. Composite keys
must be tested before being advertised. Partitioned tables, cascades, generated
columns, and transition-table triggers require explicit coverage. Reject or
document unsupported types before activation. `TRUNCATE` is blocked on captured
tables until a safe reset protocol exists; DDL is not silently treated as DML.

## Delivery and progress

| ID | Requirement | Acceptance |
| --- | --- | --- |
| R06 | Durable at-least-once delivery | Source progress advances only after every required Kafka ACK; crash windows may duplicate but never lose events |
| R07 | Stable event identity | Retries/restarts preserve identity; restore/reseed cannot silently reuse identities from a previous source epoch |
| R08 | Explicit order contract | One active output stream preserves its chosen source order, including dependent changes to the same row; no global commit-order claim without proof |
| R09 | Closed progress barriers | A checkpoint certifies delivery of the entire declared source boundary; it cannot pass a hole or an unfinished batch |
| R10 | Poison records fail closed | Invalid encoding, unknown schema, missing required values, or oversized records stop frontier advancement with a sanitized diagnostic |
| R11 | Ownership and fencing | Concurrent instances and stale leaders cannot corrupt progress or emit a misleading later barrier |
| R12 | Bounded memory and retries | Limits apply to event bytes, batch bytes/count, in-flight work, and retry backoff; an outage creates a durable backlog rather than unbounded RAM use |

Kafka ACK must mean the declared durability policy: baseline `acks=all`, producer
idempotence, and documented replication factor/minimum in-sync replicas.
Idempotence does not remove duplicates across a PostgreSQL/Kafka crash boundary.
Consumer-side deduplication or idempotent invalidation remains necessary.

A sequence ID is allocation order, not commit order. A transaction that reserved
a low ID and committed late must still be delivered. The same requirement applies
to checkpoints, maintenance, and retention, not only to the main SELECT.

## Protocol and consumers

| ID | Requirement | Acceptance |
| --- | --- | --- |
| R13 | Unambiguous data types and row presence | Exact integers/decimals, timestamps, bytes, SQL NULL, and absent fields survive Go-to-JVM decoding |
| R14 | Real Apicurio compatibility | Pinned Go writer/JVM reader fixtures agree on schema identity, framing, references, and Protobuf message selection |
| R15 | Controlled schema evolution | Compatible changes replay; incompatible/unknown versions stop before source ACK; old retained data remains decodable |
| R16 | Separate relay health from consumer freshness | Consumers certify applied progress, not merely relay uptime or Kafka publication; lag/poison/replay-gap policies fail closed |

See [protocol](protocol.md) for the canonical event semantics. A Jimmer cache
adapter is an integration target, not a dependency of the relay core. It must
validate actual table/cache coverage and exercise subtype and association
invalidation with the chosen released Jimmer version.

## Operations

| ID | Requirement | Acceptance |
| --- | --- | --- |
| R17 | Safe retention and maintenance | Unacknowledged or still-referenced records cannot be deleted/rotated; stalled consumers retain data and expose pressure |
| R18 | Replay, shutdown, and recovery | Bounded shutdown either completes a batch or leaves it replayable; runbooks cover PostgreSQL/Kafka/Registry outages and source restore |
| R19 | Explicit configuration and diagnostics | Invalid settings fail startup; secrets and row payloads never appear in normal errors; essential lag/backlog/retry/ownership state is observable |
| R20 | Reproducible performance evidence | Same durability, compression, topology, payload, and workload across comparisons; include source write cost and correctness results |

Retention must account for both source-queue progress and downstream Kafka
retention. An acknowledged source event may no longer be recoverable after
Kafka expires it; a lagging consumer must detect the gap and explicitly rebuild.
An automatic jump to the latest offset must not restore “healthy” cache reads.

## Non-goals for the first release

- General-purpose connector platform, multiple databases/brokers, or a plugin SDK.
- Global transactionally consistent materialized replicas or distributed exactly-once delivery.
- Automatic business-table backfill, schema migration, or DDL replication.
- Multiple Kafka partitions or arbitrary concurrent publishers for one ordered stream.
- Arbitrary transformations, event coalescing, or PK-only replacements for full images.
- An administration UI or broad deployment/operator platform.

## Decisions still requiring evidence

1. Pending-flag queue versus reusing/auditing a PgQ-style snapshot engine.
2. Minimum PostgreSQL release and the actual restricted-role installation path.
3. Ownership/fencing mechanism and corresponding Kafka consumer isolation policy.
4. Protobuf row representation, exact Apicurio framing, compatibility mode,
   and schema registration ownership.
5. Tick/batch/flush defaults, retention model, and numerical operating targets.

Resolve these in the relevant milestone and record a short decision with its
evidence. Do not add a “decision made” ADR for an untested hypothesis.
