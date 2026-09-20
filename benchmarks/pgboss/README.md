# Official pg-boss / KafkaJS comparison

This fixture uses **pg-boss 12.33.2**, **KafkaJS 2.2.4**, and **pg 8.23.0** from
their public npm releases. Transitive versions/integrities are locked. No private
application code or reimplementation of pg-boss claim/complete SQL is used.

## Run

With Node 22.12+ on the host, prepare the isolated lab while no benchmark is active:

```sh
make fetch-pgque lab-up pgboss-deps check bench-build integration-pgboss
./bin/row-bench --mode pgboss --outbox --node-ingress --compression none \
  --rate 100 --warmup 1s --duration 3s \
  --output benchmarks/results/pgboss-smoke/pgboss.json
python3 benchmarks/run.py --pgboss --output benchmarks/results/pgboss-comparison-001
```

The last command runs PgQue and pg-boss sequentially, with five shuffled steady
repetitions, idle, 5,000/s offered load, held snapshots and a prepared 100,000-event
backlog. It freezes the Node sources and locked dependencies under the result
directory before measurement. The relay uses the pinned official Node 24.16.0
image, one CPU, a 128 MiB cgroup ceiling and 96 MiB V8 old-space limit. Old-space
is not an RSS limit. Host Node version and resolved worker image are recorded.

For individual PgQue/pg-boss comparisons, use the **same** `--node-ingress
--outbox --compression none` flags. The legacy capture-disabled control has a
different producer path and is omitted from this cohort.

## Transaction and oracle

Go generates the synthetic input and its independent SHA-256 expectation. A
common stdin/stdout Node bridge for both candidates writes business rows,
the expectation ledger, append-only prepared `bytea` facts, and the queue entry
in **one PostgreSQL transaction**. Nanosecond timestamps cross JSON as decimal
strings. The integration test rejects the SDK's job insertion and verifies that
business rows, facts and oracle all roll back while an earlier commit survives.

For pg-boss, official `insert(..., {db, returnId: true})` enqueues stable-ID jobs
referencing those facts. For RowRelay, the same bridge calls the public enqueue
function with the prepared bytes. No application event is rebuilt by the worker.
The existing independent Go Kafka reader verifies identity, key, headers, exact
timestamps, payload hashes, missing events, phantoms, duplicates, final source ACK
and final Kafka high watermark. All readers use `read_committed`.

## Deliberate comparison boundaries

- This compares queue/client/runtime combinations, **not runtime alone**.
- The worker uses official `fetch`/`complete` APIs with a 50 ms explicit idle
  poll, matching the Go candidate. It does not use `work()` (whose SDK polling
  minimum is 500 ms), transactional handlers, LISTEN/NOTIFY or a patched SDK.
- One owner publishes in Kafka transactions under a stable source identity.
  Source completion uses the same pinned PostgreSQL session, verifies its advisory
  lock inside a short transaction **after Kafka commit**, and checks the SDK's
  affected count. Failed/ambiguous operations terminate the worker. This fixture
  has not passed RowRelay's two-process failure matrix and is not an HA reference.
- A separate SDK pool runs normal queue supervision. Cron scheduling is disabled.
  Queue policy is `key_strict_fifo`; source claim size is at most 1,000 records
  and bounded for the fixed fixture payload size. KafkaJS send chunks stay under
  the broker record-batch ceiling within the same transaction. In-flight requests
  are one for KafkaJS; the Go client retains its existing defaults.
- All candidates use no compression, `acks=all`, idempotence, transactions, the
  same one-partition topic and RF=1 broker. This is not a quorum-failure test.
- Completed-job retention is 30 seconds; pg-boss's ordinary supervision cadence
  and PgQue's 10-second rotation produce different
  physical retention shapes. Disk totals are not a strict equal-retention trial.
  Queue jobs have 14-day retention and bounded retries; this fixture is not a
  replacement for the production recovery/migration contract.
- Immutable fact bytes are reported separately from queue bytes. Producer bridge
  CPU/RSS and the Go generator/oracle are outside relay resource counters. SQL
  costs include their work; producer-operation latency includes bridge overhead.
- Current synthetic binary payloads are not Protobuf/Apicurio. Compatibility with
  real application consumers remains a separate acceptance gate.

Smoke checks exercise both paths and the rollback test. They are correctness
checks, not a measured throughput or resource comparison.
