# Benchmark program

Status: **full experiment specification with an implemented local screening
harness**. `benchmarks/run.py` measures the selected PgQue implementation;
the Eventuate and Protobuf variants and the longer runs below remain planned.
Published third-party results live in [research](research.md). Run
[correctness gates](testing.md) before interpreting performance.
The [official pg-boss/KafkaJS fixture](../benchmarks/pgboss/README.md) adds a
PgQue/pg-boss prepared-outbox comparison with common atomic ingress and a Go oracle.
Its smoke/rollback checks pass; measured comparative results are not yet complete.

## Questions and controlled variants

| Variant | Runtime | Queue | Wire format | Question |
| --- | --- | --- | --- | --- |
| A | Eventuate JVM | Pending flag, batched ACK | Eventuate JSON | What does the existing baseline cost? |
| B (retired) | Go | Pending flag, batched ACK | Same JSON envelope | Historical comparison only; implementation removed |
| C (selected) | Go | PgQue snapshot batches | Same JSON envelope | Current JSON relay resource costs |
| D | Go | Same selected queue as C | Typed Protobuf + Apicurio | What does the wire format change? |

The owner selected PgQue after the held-snapshot measurements. B is not rebuilt
or scheduled again. Future Eventuate comparisons change both queue and runtime
and must be labelled accordingly. A preliminary JSON-in-Protobuf envelope is a
separate experiment, not evidence for typed encoding.

Reference Eventuate source: `0.19.0.RELEASE` branch, commit
`093d972738f7d885763d5ef06368dd2cbdbee0d6`. Pin a corresponding image digest or
reproducible source build at execution time; a branch/tag alone is insufficient.
Record Kafka client defaults rather than assuming JVM and Go defaults match.

## Fairness controls

- Identical PostgreSQL/Kafka/Registry versions, CPU/memory limits, disks, network,
  partitions, source schema, workload seed, and consumer implementation.
- Logged tables, PostgreSQL durability enabled, Kafka `acks=all`, producer
  idempotence, and identical broker replication/min-ISR policy.
- Same compression codec/level, batch byte/count bounds, flush/linger policy,
  maximum record size, connection limits, retries, and in-flight limits.
- Same decoded semantics and complete OLD/NEW images; no coalescing or omitted
  fields in the supposedly faster variant.
- Same source and Kafka retention windows, cleanup cadence, and cache topology.
- Same observability overhead. Count required tick/maintenance/coordination
  processes in total resources, not only the main relay PID.

If Kafka transactions/fencing require different consumer isolation or ACK
semantics, declare that as a separate axis. Do not call such a comparison
runtime-only. Audit the baseline's progress barriers too: if they do not meet
R09, its data throughput can still be reported, but not as a correctness-passing
freshness reference.

Measure no compression and a common supported codec (initially Zstandard).
Kafka compresses record batches; raw JSON/Protobuf byte counts alone do not
predict network or stored-log savings.

## Workloads

Use deterministic synthetic data. Start with a small screening matrix; expand
only promising candidates rather than multiplying every setting immediately.

| ID | Workload | Purpose |
| --- | --- | --- |
| B01 | Empty/idle source | Idle queries, ticker writes, CPU, RSS, wake-up latency |
| B02 | Fixed open-loop 100 / 1,000 / 10,000 events/s where hardware permits | Latency at equal offered load; record overload rather than silently lowering the rate |
| B03 | Capacity ramp with a fixed latency/backlog criterion | Sustainable goodput, not peak accepted writes while backlog grows |
| B04 | Burst above capacity, then normal traffic | Peak memory, backlog growth, catch-up time, fairness to OLTP |
| B05 | 60/30/10 insert/update/delete mix plus update-heavy and hot-key runs | Image size, invalidation effects, dependent-row ordering |
| B06 | Single-row SQL vs multi-row INSERT/UPDATE/DELETE and COPY | Trigger overhead; optional transition-table experiment |
| B07 | 0.5 / 2 / 16 KiB typical row images; 256 KiB and configured-limit tails | Allocation, compression, byte limits, large-record handling |
| B08 | Long source transactions and held read snapshots, with realistic autovacuum | Late-commit correctness and vacuum/visibility sensitivity |
| B09 | Backlog larger than available RAM; cold and warm cache separately | I/O behavior, replay, bounded buffering |
| B10 | Kafka/Registry outages, restart, ownership loss, recovery | Fault correctness and recovery cost |
| B11 | At least two hours and multiple actual retention/rotation cycles | Steady-state disk growth, metadata churn, maintenance stalls |
| B12 | Producer without trigger, row trigger, optional statement trigger | Business-transaction overhead independent of relay speed |

Pin the exact mix and payload sizes in each result manifest. The numbers above
are workload inputs, not throughput promises. Include nullable and realistic
high-entropy fields; repeated strings alone overstate compression gains.

Sweep polling/tick intervals **1/10/100/1,000 ms** and batch counts **1/100/1,000**
only after the initial controlled comparison. Keep byte bounds active. Test the
actual external Go ticker; published `pg_cron` behavior is not a substitute.

## Metrics

### Latency and delivery

- p50/p95/p99/max and histograms for source transaction latency, source selection
  to Kafka ACK, and end-to-end consumer application/cache invalidation.
- Source commit-to-application latency as a desired metric, with an explicit
  measurement definition. A trigger timestamp precedes commit; producer receipt
  of COMMIT success follows it. Neither is the exact commit instant.
- Report transaction-start-to-application and COMMIT-response-observed-to-apply
  separately when an exact commit timestamp is unavailable. An event can be
  applied before the producer receives COMMIT success; retain that fact rather
  than clamping a negative observed interval or pretending it measures commit.
- Offered, committed, ACKed, uniquely delivered, and applied events/s separately.
- Backlog count/bytes/oldest age, duplicate count/rate, retries, recovery time,
  and certified-barrier lag. Never hide failed or missing events from percentiles.

Use monotonic clocks for same-process intervals. Record synchronization/error
bounds for cross-host timestamps. The generator must report admission delay and
unsatisfied scheduled arrivals to avoid coordinated omission under overload.

### Resources and producer cost

- Relay and auxiliary-process CPU, RSS/PSS where available, peak memory, startup
  time, allocations/GC, and connection counts. Image size and heap flags are not RSS.
- PostgreSQL CPU, query calls/time, buffers, lock waits, live/dead tuple estimates,
  table/index size, vacuum work, and WAL bytes generated by normal durability.
- Business-write TPS and p95/p99 latency relative to capture-disabled control.
- Kafka bytes/records, actual compressed network/log sizes, producer batch fill,
  broker CPU, and replication traffic under the declared topology.
- Registry request counts/cache hit rate; no hidden HTTP lookup per event.
- Raw and encoded event sizes plus compressed batch sizes, including framing,
  schema IDs, keys, headers, and OLD/NEW duplication.

Use `EXPLAIN (ANALYZE, BUFFERS)` on the actual queue/maintenance queries in the
disposable lab with representative fresh and churned data. Save plans separately
from timed runs. Collect statistics consistently and state which values are
estimates, samples, counters, or exact oracle observations.

## Run procedure

1. Pin implementation commit, image digests, configuration, host details, and seed.
2. Pass the applicable C01–C20 cases and record unsupported cases.
3. Calibrate the generator and consumer so they are not the hidden bottleneck.
4. Predeclare the primary metric and latency/resource limits for the comparison.
   Record these before running the candidate; do not move a threshold afterward.
5. Restore equivalent fresh database/broker state, warm up for at least 60 seconds,
   then measure for at least 10 minutes for steady runs. Keep idle/startup runs
   separate. Churn/soak runs intentionally retain their evolving state.
6. Run at least five repetitions, randomizing A/B/C/D order. Avoid other heavy
   builds or workloads on the host. Record throttling and environmental incidents.
7. Drain and reconcile against the independent oracle. A count mismatch invalidates
   a performance claim until explained; preserve failed runs.
8. Report per-run distributions and the median/spread or confidence interval of
   comparable run statistics. Do not merge unlike workloads into one percentile
   or add percentile values from separate pipeline stages.

An algorithm change needs demonstrated improvement in its declared primary
metric without violating correctness, latency, or retention limits. If the gain
is within run-to-run noise, retain the simpler implementation. Initial numerical
production SLOs will be set from the baseline and intended deployment budget,
not invented as achieved results during bootstrap.

## Artifacts and report template

### Duration / rotation soak

```sh
make check bench-build integration
python3 benchmarks/run.py --outbox --soak-seconds 7200 --warmup-seconds 60 \
  --output benchmarks/results/outbox-soak-001
```

This is one PgQue run at 500 offered events/s, using the existing independent
oracle and frozen source/binaries. It retains one-second cgroup/queue-lag samples,
the actual rotation timestamp/current table, and queue-storage estimates about
every 30 samples. Diagnostic JSONL survives a failed run. Rotation must be
observed, not inferred solely from the configured 10-second period.

The relay remains limited to one CPU / 128 MiB, with a 96 MiB Go soft limit.
The generator/oracle is outside that budget and retains expectations for the
full run. Plan disk space for business rows, the oracle and retained Kafka data
as well as the rotating queue. Gate on complete reconciliation and source ACK,
no OOM/worker exit, continued rotations and bounded queue storage; report memory
trends and latency separately. A local RF=1 duration run does not certify
multi-broker failover or managed-service installation rights. Shorter durations
are useful harness preflights, not substitutes for the two-hour gate.

The runnable screening harness lives in `benchmarks/`. Raw generated data goes
under ignored `benchmarks/results/<run-id>/`; publish reviewed, sanitized result
summaries under `docs/results/` only when real runs exist. Do not commit fake
sample results. Archive large raw artifacts separately with checksums and links.

Each report must contain:

```text
Question / primary metric / preregistered limits:
Status: measured | incomplete | invalidated
Commit, queue implementation, dependency versions, image digests:
Hardware, OS, storage, topology, CPU/memory limits:
PostgreSQL durability, Kafka ACK/ISR/idempotence/isolation, compression:
Workload ID, seed, rows/bytes, transaction sizes, offered rate:
Batch/tick/flush/retry/retention settings:
Clock method, warm-up, duration, repetitions, run ordering:
Correctness: expected, unique received, missing, phantoms, duplicates, barriers:
Latency distributions, throughput/goodput, backlog and recovery:
Relay + DB + broker + Registry resource measurements:
Producer overhead, query plans, table/index/dead-tuple growth:
Failures, unsupported cases, limitations:
Raw artifact locations and SHA-256 checksums:
Conclusion, uncertainty, and decision:
```
