# PgQue / official pg-boss outbox screening — 2026-09-19

## Result and scope

**Measured, complete: all 18 delivery checks passed.** The independent Go oracle
reconciled **2,220,000 committed events**, with zero missing, corrupt, phantom or
duplicate records and complete source ACK in every case.

At 1,000 events/s the Go/PgQue relay used about **4.1× less relay CPU** than the
Node/pg-boss fixture. pg-boss had lower healthy-path latency. Under a held source
snapshot, PgQue p99 was **313 ms**, versus **64.8 s** for pg-boss. A prepared
100,000-event backlog drained through final source ACK in **3.59 s / 18.38 s**.

This supports PgQue for the declared resource/held-snapshot workload. It is not
a universal latency win: PgQue exceeded the preregistered 250 ms steady p99 limit
in **one of five** repetitions. Neither implementation sustained the offered
5,000 events/s. Delivery correctness and latency acceptance are separate gates.

The removed pending backend is absent from this cohort. Its historical decision
is recorded in [queue selection](2026-09-19-queue-selection.md). The interrupted
earlier three-candidate run is retained separately and is not part of these results.

## Controlled setup and differences

- Shared AMD Ryzen 9 9900X host, 24 logical CPUs, approximately 30 GiB RAM,
  Linux `7.1.11-arch1-1`, Go `1.27.0-X:nodwarf5`, Node `24.16.0`.
- Pinned PostgreSQL `18.4-alpine`, Kafka Native `4.3.0`; each has two CPUs and
  768 MiB. The relay has one CPU and a 128 MiB hard memory limit; Go soft memory
  limit / Node old-space limit is 96 MiB. These heap settings are not RSS limits.
- Upstream PgQue `0.2.0`, commit
  `e8ee488d2c1d87ab09eed2581ec4bbc1e68315f6`; official pg-boss `12.33.2`, KafkaJS
  `2.2.4`, pg `8.23.0`, with a frozen npm lockfile and pinned Node image digest.
- PostgreSQL durability remains enabled. Both publishers use Kafka transactions,
  idempotence and `acks=all`; the oracle uses `read_committed`. Kafka is a **single
  broker**, RF=1/min-ISR=1; this is not a quorum-failure experiment. No compression.
- One source, one partition, one active publisher. Explicit 50 ms polling for
  both. The Node fixture uses the official **fetch/complete APIs**, rather than
  `work()` with its 500 ms minimum scheduler interval. It does not copy claim SQL.
- Both source producers use the same Node ingress bridge. One PostgreSQL
  transaction writes business rows, independent Go expectations, immutable
  prepared wire facts, and either RowRelay enqueue or official SDK bulk insert.
  pg-boss jobs reference those facts; RowRelay stores the prepared wire value in
  its queue. Thus this compares complete transport paths, **not runtime alone**.
- Payloads are deterministic, high-entropy 1 KiB strings, framed as two int64s
  plus UTF-8 bytes, with stable UUID headers and decimal keys. This is **not
  Protobuf**. Arrivals are scheduled in 10 ms windows; admission delay is reported.
- Common bounds are 1,000 records / 4 MiB. KafkaJS uses one in-flight request and
  splits a partition send below 900,000 bytes inside the source-batch transaction;
  franz-go performs its own record-batch splitting. The clients are not identical.
- PgQue rotates at 10 seconds; pg-boss retains completed jobs for 30 seconds with
  ordinary SDK maintenance. Actual retained bytes are consequently not an equal
  retention-window comparison. Unfinished-job expiration/retry policies also
  differ; this cohort does not certify the Node fixture as an HA/fault baseline.
- Go generator/Node ingress CPU is outside relay counters. PostgreSQL metrics
  include writes, the independent oracle, queueing and delivery. Immutable fact
  storage is recorded separately from queue storage in raw results.

Each steady case has 10 seconds warm-up and 60 seconds measurement, five
repetitions in seeded shuffled order. Held-snapshot cases measure 300 seconds;
idle cases measure 30 seconds after five seconds warm-up. These are screening
runs, shorter than the full benchmark/soak specification.

Source, both executables, and Node fixture files were frozen before the cohort;
executable hashes were checked per case. During measurement, development was
limited to source/documentation changes, external API research and short,
single-worker compile/unit/race checks. No RowRelay integration workload, heavy
build or lab reconfiguration ran concurrently. The host was shared, not reserved;
system-wide interference was not continuously attributed to other processes.

## Steady 1,000 events/s

Values below are medians of five per-run values, not pooled percentiles.

| Metric | Go / PgQue | Node / pg-boss |
| --- | ---: | ---: |
| Delivery p99 | 226.456 ms | 121.617 ms |
| Per-run p99 range | 200.949–313.823 ms | 105.635–124.747 ms |
| Relay CPU, % of one core | 1.385% | 5.692% |
| PostgreSQL CPU, % of one core | 16.819% | 23.269% |
| Sampled relay cgroup-memory peak | 13.328 MiB | 42.535 MiB |
| Exact lifetime cgroup-memory peak | 13.727 MiB | 43.004 MiB |
| Sampled process RSS peak | 21.289 MiB | 83.508 MiB |
| Steady p99 ≤250 ms | 4/5 runs | 5/5 runs |

Cgroup memory and RSS are different measurements. Shared/file-backed pages can
be charged outside a process's cgroup; do not substitute one number for the other.
The Go relay was approximately 4.1× lower in CPU, 3.1× lower in exact charged
peak memory and 3.9× lower in sampled RSS in these median comparisons.

## Held snapshot, 1,000 events/s

| Metric | Go / PgQue | Node / pg-boss |
| --- | ---: | ---: |
| Committed, including warm-up | 310,000 | 310,000 |
| Delivery p99 | 313.461 ms | 64,750.548 ms |
| Delivery maximum | 1,272.233 ms | 67,048.215 ms |
| PostgreSQL CPU, % of one core | 20.725% | 84.683% |
| Relay CPU, % of one core | 1.335% | 3.794% |
| Exact relay cgroup-memory peak | 13.297 MiB | 67.648 MiB |
| Queue storage at observation | 58.211 MiB | 243.125 MiB |
| Estimated dead tuples | 16,511 | 631,127 |
| Delivery p99 ≤1,000 ms | Pass | Fail |

The snapshot is released before final drain; successful final reconciliation
does not erase the latency accumulated while it was held. Dead tuples are
PostgreSQL estimates and storage reflects the different retention mechanisms.

## Offered 5,000 events/s

Both generators eventually committed all 300,000 measured events plus 50,000
warm-up events. Neither met the offered schedule:

| Metric | Go / PgQue | Node / pg-boss |
| --- | ---: | ---: |
| Time to admit measured events | 127.644 s | 108.626 s |
| Actual measured admission rate | 2,350.285/s | 2,761.773/s |
| Admission-delay p99 | 67.625 s | 47.164 s |
| Transaction-start → decode p99 | 1,252.829 ms | 572.994 ms |
| Exact relay cgroup-memory peak | 15.262 MiB | 54.035 MiB |

Admission delay precedes the per-event transaction-start timestamp. The delivery
percentile alone must not be presented as scheduled-arrival latency, and the
configured 5,000/s rate must not be presented as achieved throughput.

## Idle and prepared backlog

| Metric | Go / PgQue | Node / pg-boss |
| --- | ---: | ---: |
| Idle relay CPU, % of one core | 0.314% | 0.890% |
| Idle exact cgroup-memory peak | 10.848 MiB | 30.426 MiB |
| 100,000-event drain through final source ACK | 3.594878 s | 18.377297 s |
| Complete-drain rate | 27,817.355/s | 5,441.497/s |
| Backlog exact cgroup-memory peak | 24.758 MiB | 75.008 MiB |

Drain duration includes container startup and equals `producer_elapsed_s +
recovery_s`. The raw producer-only rate excludes final source ACK. These are
prepared-backlog observations, not sustainable source-write capacity. Backlog
latency percentiles include preload waiting time and are not used as drain time.

## Reproduction and frozen evidence

```sh
make fetch-pgque lab-up bench-build pgboss-deps integration integration-pgboss
python3 benchmarks/run.py --pgboss --output benchmarks/results/<new-run-id>
```

Local raw evidence: `benchmarks/results/pgque-pgboss-screening-20260919/`.
The runner saved every raw case, state/log files, source archive, executables,
Node fixture copy, dependency tree, image digests and selection policy. Current
source additionally contains progress-consumer and JVM compatibility work which
is **not** part of this measured build; progress records were disabled here.

| Artifact | SHA-256 |
| --- | --- |
| `manifest.json` | `5fce2af38194a8db0bef79fad104886de90d55fb35c83f6de81852afdfb5563d` |
| `summary.json` | `f4905cbe1363e69c4d425cb91b5bb0997ad48f0bfc7da6848091ed60fc214ce6` |
| `source.tar.gz` | `223d1f637ad8a2b11ef48fba85d9c820226cc9cd04cde40460cf95101f8553cf` |
| `row-relay` | `437afe2b01581da8c4522131382e58f1ed7e933fcfb0fbb46f106e280b6790bd` |
| `row-bench` | `8a1d97f5a5f720c203e972a4884bf619266a80d39a99791cb23cf43c481629fb` |
| `node-baseline/package-lock.json` | `c87345b989698f267accb4e631d5ccefb39b1f22877b3df942cdf67a889a6d11` |

Remaining acceptance includes progress/cache integration, managed-environment
privileges, multi-replica throughput, broker-quorum failures and long soak.
The comparison does not authorize a live cutover or prove business-consumer ACK.
