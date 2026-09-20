# Local JSON relay screening — 2026-09-19

Status: **measured — all 17 cases completed**. All 2,020,000 relayed events
reconciled with zero missing/corrupt/phantom/duplicate events and complete source
ACK. The capture-disabled control committed another 70,000 rows.

## Results and decision

At 1,000 offered events/s, medians of five independent runs:

| Metric | Pending | PgQue |
| --- | ---: | ---: |
| End-to-end p99, ms | 82.50 | 177.85 |
| Per-run p99 range, ms | 75.02–83.81 | 168.55–180.75 |
| Relay CPU, % of one core | 2.18 | 1.94 |
| Relay sampled cgroup peak, MiB | 17.73 | 17.87 |
| PostgreSQL CPU, % of one core | 7.44 | 10.87 |

Additional scenarios were run once each:

| Scenario | Pending | PgQue |
| --- | ---: | ---: |
| Held snapshot 5 min: p99, ms | 336.93 | 194.48 |
| Held snapshot: PostgreSQL CPU, % core | 64.70 | 15.53 |
| Held snapshot: relay CPU, % core | 1.79 | 1.91 |
| Held snapshot: exact relay cgroup peak, MiB | 21.94 | 17.45 |
| Held snapshot: sampled process RSS peak, MiB | 27.14 | 23.04 |
| Held snapshot: end queue size, MiB | 824.30 | 39.95 |
| Held snapshot: estimated dead tuples, all queue tables | 564,736 | 17,291 |
| Idle: relay CPU, % core | 0.22 | 0.33 |
| Idle: exact relay cgroup peak, MiB | 10.09 | 9.75 |
| 5,000 offered/s: measured committed/s | 2,523 | 4,354 |
| 5,000 offered/s: p99 admission delay, s | 58.96 | 9.16 |
| 5,000 offered/s: event p99, ms | 323.12 | 848.94 |
| 5,000 offered/s: exact relay cgroup peak, MiB | 26.58 | 25.21 |

Both candidates met the preregistered steady/held latency and memory gates.
**PgQue is the preferred local candidate under the declared worst-case source-CPU
criterion:** its maximum observed source CPU was 19.80% of a core, versus 64.70%
for pending. Its held-snapshot source CPU was about 4.2 times lower. This is a
provisional workload-specific selection; held/high-load cases need repetitions
and longer runs before production adoption.

**Pending remains the portable CLI baseline:** it requires no PgQue bootstrap
and has lower healthy-path latency and database CPU. PgQue adoption additionally
depends on managed-provider installation rights and the uncompleted safety gates.
Neither candidate sustained the offered 5,000 events/s in this lab. The local
producer/oracle/DB path is part of this limitation; this is not an isolated
measurement of maximum Kafka publisher throughput.

The Go processes stayed below 27 MiB exact cgroup peak in this cohort; sampled
process RSS reached about 29.4 MiB. These measurements cover one active CDC
source, before the subsequent business-outbox/multi-stream extension. They do
not predict memory for an arbitrary number of streams or prove a saving against
Node.js/Eventuate. The source/binary manifest identifies this frozen cohort.

## Question and method

Compare the implemented Go pending-flag queue with upstream PgQue snapshot
batches, using the same JSON envelope, publisher, consumer oracle, 50 ms cadence,
1,000-record / 4 MiB Kafka chunks, Zstandard compression, `acks=all`, and producer
idempotence. This isolates the queue implementation, not Go versus Java or JSON
versus Protobuf.

- PostgreSQL 18.4 and Kafka Native 4.3.0, pinned image digests in
  [`local/compose.yml`](../../local/compose.yml); ordinary PostgreSQL durability
  remains enabled. One Kafka partition, one broker, RF=1 / min ISR=1.
- Host: AMD Ryzen 9 9900X, 24 logical CPUs, approximately 30 GiB RAM,
  Linux `7.1.11-arch1-1`, x86_64; Go `go1.27.0-X:nodwarf5`. Docker volumes use
  shared host storage; device throughput was not independently calibrated.
- PgQue 0.2.0, commit `e8ee488d2c1d87ab09eed2581ec4bbc1e68315f6`, installed from
  its unmodified upstream checkout. No replication privilege is used by capture
  or runtime. Its administrative bootstrap rights still need provider validation.
- Relay: one CPU, 128 MiB hard memory ceiling, `GOMAXPROCS=1`,
  `GOMEMLIMIT=96MiB`. PostgreSQL and Kafka: two CPUs / 768 MiB each.
- Synthetic 1 KiB high-entropy text, seed 42, an independent checksum ledger in
  every business transaction. The generator batches scheduled arrivals into
  10 ms windows: 10 rows/transaction at 1,000 events/s, 50 at 5,000 events/s.
- Five randomized-order 60-second repetitions per algorithm at 1,000 events/s,
  each following 10 seconds of warm-up. Additional capture-disabled control,
  idle, 5,000 events/s, and five-minute held-repeatable-read-snapshot cases.
- Pending retains acknowledged rows for 30 seconds; PgQue has a 10-second
  rotation interval plus snapshot/consumer safety constraints. Their retention
  shapes differ, so end-of-run disk sizes are not an equal-retention comparison.

The preregistered local selection policy requires zero missing/corrupt/phantom
events and complete source ACK, the relay resource ceiling above, steady p99
below 250 ms, and held-snapshot p99 below 1,000 ms. Among candidates passing these
gates, compare worst-case source CPU and report healthy-path tradeoffs.

Latency means **transaction start → decoded Kafka event**, not exact commit time
or cache invalidation. CPU is measured from cgroup counter deltas: 100% is one
core. Memory distinguishes one-second cgroup/anonymous/RSS samples from the
relay's exact cgroup `memory.peak` since startup. RSS and cgroup memory account
for shared/file-backed pages differently and are not interchangeable.

## Failures found and fixed

### Pending cleanup timeout

The original series passed five 1,000 events/s repetitions for each algorithm,
then the pending relay exited at the higher offered rate. A diagnostic rerun
captured `maintenance (deadline exceeded)`, exit code 1, and `OOMKilled=false`.
This was a maintenance failure, not a demonstrated memory-limit failure.

The old cleanup predicate used volatile `clock_timestamp()` for every candidate
row. With 200,000 fresh acknowledged rows and nothing eligible for deletion,
`EXPLAIN (ANALYZE, BUFFERS)` showed 200,000 rows filtered and 200,000 heap fetches.
It could not use the timestamp as an index range boundary.

The fix obtains one server-clock cutoff per maintenance cycle and deletes up to
10,000 eligible rows per statement through a materialized `ctid` selection. The
new plan has an indexed timestamp condition; the empty-range check touched three
index buffers rather than fetching every fresh row. `ctid` never survives its
statement. A fixed cycle boundary also prevents cleanup from chasing arrivals.
The plan timings are not a controlled speedup ratio: their cache states differ.

A real-database regression test removes more than one chunk of expired,
acknowledged rows and preserves old unacknowledged, recent, and future-dated
rows. `make fmt check bench-build integration` passed after the fix.

The immediate higher-load rerun delivered all 350,000 committed events, with zero
missing/corrupt/phantom/duplicate events and complete source ACK. **It did not
sustain 5,000 events/s:** the measured 300,000 events took 116.48 seconds, about
2,576 events/s, with p99 admission delay of 56.2 seconds. Successful draining is
not a capacity claim. The final series repeated this case alongside PgQue.

Failed artifacts remain under `benchmarks/results/screening-20260919-50ms/` and
`benchmarks/results/diagnostic-load5000-pending.json.{container.log,state.json}`.
The successful diagnostic rerun is `fixed-load5000-pending.json`; query plans are
`cleanup-query-plan.txt` and `cleanup-empty-query-plan.txt`, all in the ignored
`benchmarks/results/` directory. Container logs/exit state are now saved before
cleanup even when a failure prevents producing a result JSON.

### Snapshot cadence and empty batches

Early held-snapshot pilots at 10 ms showed large tails. Diagnostics identified
WAL-sync waits and accumulated batch lag, while maintenance execution itself was
short. The runner now advances immediately after completing an empty snapshot
batch and avoids caching dynamically generated per-batch SQL statements.
Subsequent pilots motivated a **shared 50 ms cadence for both candidates**.
PostgreSQL durability and Kafka ACK policy were not weakened. These pilots are
separate from the final repeated cohort, not additional repetitions of it.

## Scope of the evidence

This is a local screening experiment, not the full
[benchmark program](../benchmarks.md). It does not establish two-hour stability,
multi-broker durability, automatic failover/fencing, certified consumer
freshness, restore safety, managed-provider installation eligibility, or
Go-to-JVM Protobuf/Apicurio interoperability. No Eventuate runtime was measured.
The independent oracle adds database work; reported PostgreSQL CPU includes it.
Disk/dead-tuple figures are observed after drain and snapshot release, not peak
backlog values. The host is shared and disk performance is not isolated.

## Reproduction and evidence

```sh
make fetch-pgque lab-up bench-build integration
python3 benchmarks/run.py --output benchmarks/results/screening-20260919-fixed-cleanup
python3 benchmarks/summarize.py benchmarks/results/screening-20260919-fixed-cleanup
make lab-down
```

Use a new output directory when reproducing: the runner refuses to overwrite an
existing cohort. The local manifest records source/binary SHA-256, image digests,
host details, fixed case order, and selection policy before execution. The
summary records result checksums, reconciliation status, measured arrival rate,
latency, and resources. Raw artifacts remain local and ignored; no public raw
artifact upload or Git commit has been made.

Frozen cohort checksums:

- `manifest.json`: `b430a0e891a64149d995e87bffa68711ab112938d2af58d1e04fb379694359c1`
- `summary.json`: `669b645ff1bb20903c94036012cbd518bcf4fea717535c137592d9c24e40564f`

Both measured executables were copied into that ignored cohort directory and
verified against the manifest before rebuilding for business-outbox work.
This initial cohort predates source-archive capture; subsequent cohorts also
archive their exact source files because this repository has no commit yet.
