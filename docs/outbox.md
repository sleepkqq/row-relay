# Business-outbox contract and migration

Status: **experimental PgQue transport with source-level fencing**. PgQue delivers
application-supplied bytes through the bounded runner. Several CDC/outbox streams
share one process with one fenced producer per source. Source-level takeover and
local TLS/SCRAM checks pass; full operational and real JVM migration acceptance
remain open gates in O2/O4 of [the plan](../tasks/plan.md).

## Ingress and immutable identity

Register each stream explicitly. Its database entry fixes its name, queue mode,
source epoch and Kafka topic. Event data cannot choose a destination. Existing
registration is never reset or silently repointed:

```sh
export DATABASE_URL='postgres://owner:password@localhost/example'
./bin/row-relay --install --outbox-stream orders --topic orders.events
```

This creates the versioned `rowrelay_outbox` schema on first installation.
Subsequent invocations register other stream names. It can coexist with the CDC
`rowrelay` schema in the same database. Installation requires the pinned
upstream SQL and installation rights described in the [lab](../local/README.md).

The application writes its business state, immutable fact and enqueue in **one
database transaction**, using bound parameters:

```sql
SELECT rowrelay_outbox.enqueue(
  $1::text,   -- registered stream name
  $2::uuid,   -- immutable delivery ID, retained on retry/replay
  $3::bytea,  -- original Kafka key
  $4::bytea,  -- complete original wire value, including Registry framing
  $5::jsonb   -- [{"key":"name","value":"base64-encoded bytes"}, ...]
);
```

The returned bigint is an internal queue identifier, not a commit watermark.
The optional headers argument defaults to `[]`; an `id` header containing the
delivery UUID is added if absent. If supplied, exactly one matching `id` header
is required. Header order, repeated non-ID headers, binary values and null
values are preserved. Key and value are nonempty; limits are 4 KiB key, 512 KiB
wire value and 16 KiB serialized header array. Violations abort the enqueue and
therefore the surrounding business transaction when handled normally.

Queue storage currently uses a JSON/base64 wrapper; Kafka receives the original
decoded bytes, not that wrapper. There is no Registry request or schema
registration in the delivery loop. This is not the typed CDC codec planned in P5.

Applications own semantic identity and business idempotency. Re-enqueue can
produce an identifiable duplicate; RowRelay does not deduplicate business
commands or permit reusing one delivery ID for a different immutable fact.
Kafka ACK is transport completion, not acknowledgement of business application.

## Ordering and ownership

Enqueue takes a transaction-scoped advisory lock for the stream/key before
allocating its queue ID. Same-key enqueues serialize until commit; unrelated
keys can commit independently. PgQue uses complete snapshot batches, so lower-ID late commits are not
skipped. Within a transaction, applications enqueue in the intended order and
retain their existing aggregate/revision locking. Opaque bytes do not let the
relay infer or repair domain revision order.

The key-lock namespace is separate from publisher/installer locks. Hash
collisions cause extra serialization, not skipped work. Multi-key transactions
can deadlock and must be retried atomically. Very large transactions must fit
the server's advisory-lock budget; application aggregate-row locking is the
upgrade path when that ceiling matters.

One active runner owns each stream through its dedicated PostgreSQL session.
Its stable Kafka transactional ID fences the predecessor on takeover. Source ACK
uses the original connection and verifies ownership after successful Kafka commit.
**Consumers require `read_committed`.** See the tested takeover/replay protocol in
[ownership.md](ownership.md). Source-level standby is implemented; automatic load
balancing or sharding one stream for horizontal throughput is not.

Outbox topics may have multiple partitions, using Kafka-compatible keyed
partitioning. Keep the partition count fixed when relying on key order. CDC
still requires one partition. Poison blocks its stream; other configured streams
continue. The current policy does not bypass one poisoned key within a stream.

## One process, several explicit streams

```json
{
  "streams": [
    {"name":"catalog-cdc", "database_env":"CATALOG_DATABASE_URL", "topic":"catalog.cdc"},
    {"name":"orders", "database_env":"CATALOG_DATABASE_URL", "outbox_stream":"orders", "topic":"orders.events"},
    {"name":"receipts", "database_env":"RECEIPTS_DATABASE_URL", "outbox_stream":"receipts", "topic":"orders.applied"}
  ]
}
```

```sh
KAFKA_BROKERS=localhost:29092 ./bin/row-relay --config streams.json
```

Configuration contains environment-variable references, not database secrets.
Each stream has an independent source loop, deadline and capped reopen backoff.
Optional `batch`/`batch_bytes` override the CLI defaults per stream. There are at
most 16 configured streams and their batch-byte budgets sum to at most 32 MiB;
each stream has its own fenced producer bounded by its assigned budget. These are buffering
limits, not a promise about total RSS. Worker `active` logs are operational state,
not caught-up/freshness certification. Shared Kafka TLS/SCRAM configuration and
optional worker probes are documented in [operations.md](operations.md).
Verifying the intended broker authentication/ACLs remains a cutover prerequisite.

For a single stream the existing CLI remains available:

```sh
./bin/row-relay --outbox-stream orders --topic orders.events
```

## Retention and replay

PgQue retains segments according to its consumer/snapshot safety rules. The
retired pending backend and its per-row cleanup are no longer supported.
RowRelay never scans retained application facts to
recreate missing delivery metadata. Immutable facts remain application-owned.

Replay is explicit: enqueue selected retained facts with their original delivery
IDs, keys, values and headers, under the application's existing deduplication
contract. A full managed recovery/replay command is not yet implemented. Do not
drop pending or failed work to shorten a backlog.

## Migration acceptance — before replacing an existing job relay

1. Verify the real producer enqueue transaction and fact schema. Replace its
   job insertion with the RowRelay enqueue call atomically. Preserve domain
   revision validation in this adapter; the generic byte relay cannot perform it.
2. Validate actual Kafka authentication/ACLs, source-role installation rights,
   and byte-level interoperability through the real consumer. Apply role changes
   through the provider's supported controls, not fixture administration scripts.
3. Test crash/replay, lost ACK response, poison, retention and the intended
   ownership/fencing policy. Existing E1 consumers/backfill flags stay unchanged.
4. Define and record the handoff boundary. Quiesce affected producers or provide
   another proven atomic transition, stop the old owner and settle its in-flight
   work, then copy outstanding deliveries with their original IDs and bytes.
   Do not treat all historical facts as pending, and do not run both owners.
5. Start the new owner, reconcile outstanding deliveries and applied effects,
   then resume producers. Retain old facts, job tables and consumer offsets.
6. Roll back by stopping the new owner first, reconciling its ACKed/pending work,
   restoring enqueue routing and recreating only outstanding old jobs. Stable
   IDs and consumer deduplication cover unavoidable ambiguous ACK windows.

This is an acceptance sequence, not an executed cutover or a ready-made migration
script. The [real JVM/Apicurio fixture](../interop/jvm/README.md) now passes.
Producer/infra changes in the intended deployment, multi-replica benchmarks and
application-specific replay tooling are still outstanding.

## Checks and measurements

`make check integration` covers wire/key/header fidelity, atomic rollback,
blocked same-key enqueue versus independent-key progress, late commits, batches
larger than Kafka chunks, failed source ACK followed by byte-identical replay,
route validation, and mixed CDC/outbox progress with a poisoned stream.

```sh
make bench-build
python3 benchmarks/run.py --outbox --output benchmarks/results/outbox-screening-001

# Isolate draining from producer admission: preload before launching the relay.
GOMAXPROCS=2 ./bin/row-bench --outbox --mode pgque \
  --backlog 100000 --rate 0 --warmup 0s --duration 60s \
  --output benchmarks/results/outbox-backlog.json
```

The binary fixture contains an ID/timestamp and synthetic bytes; it verifies
transport integrity, not JVM/Registry compatibility. Backlog elapsed time includes
relay container startup; latency includes time waiting during preload. Keep it
separate from steady-arrival latency. The pending/PgQue outbox series is separate
from the completed [CDC cohort](results/2026-09-19-local-screening.md).
The [19-case outbox report](results/2026-09-19-outbox-screening.md) records the
completed pending/PgQue comparison and its latency/capacity limits.
The [official pg-boss fixture](../benchmarks/pgboss/README.md) uses the same Go
oracle and common producer ingress. Its [18-case comparison](results/2026-09-19-pgque-pgboss-screening.md)
is complete; ordered multi-replica and intended-deployment acceptance remain open.
