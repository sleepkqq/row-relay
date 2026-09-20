# Local business-outbox screening — 2026-09-19

Status: **all 19 delivery checks completed**. All **2,220,000 relayed events**
reconciled with zero missing, corrupt, phantom, or duplicate events and complete
source ACK. The enqueue-disabled control committed another 70,000 rows.
Correct delivery does not mean every latency or offered-rate target was met.

## Results and provisional choice

At 1,000 offered events/s, medians of five independent 60-second runs:

| Metric | Pending | PgQue |
| --- | ---: | ---: |
| Transaction-start-to-decode p99, ms | 85.50 | 198.97 |
| Per-run p99 range, ms | 82.78–87.26 | 180.54–207.23 |
| Relay CPU, % of one core | 2.24 | 1.96 |
| Relay sampled cgroup peak, MiB | 16.75 | 16.46 |
| Relay exact cgroup peak since startup, MiB | 17.21 | 17.00 |
| PostgreSQL CPU, % of one core | 10.61 | 12.90 |

Additional scenarios were run once per backend:

| Scenario | Pending | PgQue |
| --- | ---: | ---: |
| Held snapshot, 5 min: p99, ms | 48,335.68 | 217.16 |
| Held snapshot: PostgreSQL CPU, % core | 52.66 | 15.74 |
| Held snapshot: exact relay cgroup peak, MiB | 35.35 | 18.96 |
| Held snapshot: end queue size, MiB | 1,226.95 | 58.12 |
| Held snapshot: estimated dead tuples | 546,917 | 17,105 |
| Idle: relay CPU, % core | 0.21 | 0.34 |
| Idle: exact relay cgroup peak, MiB | 12.41 | 9.98 |
| 5,000 offered/s: measured committed/s | 1,823 | 3,464 |
| 5,000 offered/s: p99 admission delay, s | 103.65 | 26.15 |
| 5,000 offered/s: event p99, ms | 353.68 | 1,089.45 |
| Prepared 100,000-event backlog: time through source ACK, s | 16.597 | 2.965 |
| Prepared backlog: events/s through source ACK | 6,025 | 33,731 |
| Prepared backlog: exact relay cgroup peak, MiB | 36.75 | 29.82 |

**PgQue is the provisional candidate for this outbox workload.** Both backends
passed the 250 ms steady p99 gate and the 128 MiB relay resource limit. Pending
failed the 1,000 ms held-snapshot p99 gate; PgQue passed it. This is stronger
separation than in the earlier [CDC cohort](2026-09-19-local-screening.md), where
both passed that latency gate. Pending still has lower ordinary-path latency and
database CPU, and remains the portable CLI baseline without PgQue installation.

**Neither backend sustained 5,000 offered events/s.** The measured 300,000 events
took 164.55 seconds with pending and 86.59 seconds with PgQue. Admission delay
must accompany delivery latency: measuring from transaction start excludes time
an event spent waiting for its scheduled write to begin.

Prepared backlog isolates draining from concurrent producer admission. Its clock
starts before relay-container launch, so startup is included. The totals above
add final source-ACK recovery time to the consumer-drain interval. The raw summary's
`measured_events_per_s` alone excludes that final interval: using it would report
6,039/s and 44,628/s instead. Backlog event latency also includes preload waiting
and must not be compared directly with steady-arrival event latency. These short,
single runs are not sustained-capacity or multi-replica throughput measurements.

The largest exact relay cgroup peaks were 36.75 MiB for pending and 29.82 MiB for
PgQue; largest sampled process RSS peaks were 30.56 and 32.12 MiB respectively.
The two memory measures account for pages differently and are not interchangeable.
There is no Node.js/pg-boss memory comparison in this cohort.

## Method and measured implementation

- The same Go publisher, original wire-byte transport, 50 ms poll/tick cadence,
  1,000-record / 4 MiB chunk bounds, Zstandard, Kafka idempotence and `acks=all`
  were used for both backends. Kafka had one partition, one broker, RF=1/min ISR=1.
- PostgreSQL 18.4 and Kafka Native 4.3.0 use the pinned images in
  [the lab compose file](../../local/compose.yml). PostgreSQL durability stayed on.
  PgQue 0.2.0 is the unmodified upstream commit
  `e8ee488d2c1d87ab09eed2581ec4bbc1e68315f6`.
- Host: AMD Ryzen 9 9900X, 24 logical CPUs, approximately 30 GiB RAM,
  Linux `7.1.11-arch1-1`, Go `go1.27.0-X:nodwarf5`. Each relay had one CPU,
  128 MiB hard memory limit, `GOMAXPROCS=1`, and `GOMEMLIMIT=96MiB`.
  PostgreSQL and Kafka each had two CPUs / 768 MiB.
- Each source transaction writes business rows, an independent checksum ledger,
  and the registered outbox. Synthetic values contain two binary int64 fields
  followed by 1 KiB seeded high-entropy text; keys and delivery-ID headers are
  independently checked. This fixture is **not a real Protobuf/Apicurio message**.
- Five randomized-order steady repetitions per backend used 10 seconds of
  warm-up and 60 seconds of measurement. Control, idle, 5,000 offered/s,
  five-minute held repeatable-read snapshot, and preloaded 100,000-event backlog
  cases complete the 19-case matrix. Scheduled arrivals are grouped into 10 ms
  transactions; backlog preload uses bounded transactions of at most 1,000 keys.
- The frozen implementation includes same-key transactional enqueue exclusion
  using two-int advisory locks, distinct from publisher/installer lock keys.
  It predates the subsequent TLS/SCRAM and HTTP health modules.
- Source and binary hashes were fixed before execution; both measured binaries
  were preserved and verified before rebuilding. A source archive is retained.
  Independent source/document edits and brief single-worker unit/race/vet checks
  occurred on the shared host; full builds/integration tests followed completion.

CPU percentages use sampled cgroup counter deltas; 100% means one CPU core.
Database CPU includes the independent oracle. The very short backlog CPU samples
do not include the entire startup/final-ACK interval. Queue size and estimated
dead tuples are collected after drain and snapshot release, not at peak backlog.
Pending retains ACKed rows for 30 seconds; PgQue uses 10-second rotation subject
to snapshot/consumer safety. These are different retention shapes. Host disk and
other processes were not isolated, and this is a screening rather than a soak.

The original manifest's `selection_policy.scope` retained a legacy “JSON relay”
label. Its `business_outbox: true`, case arguments, source archive, and binary
hashes identify the actual byte-preserving outbox run. Future manifests now use
the correct scope label; the measured manifest is preserved unchanged.

## Evidence, reproduction, and remaining gates

```sh
make fetch-pgque lab-up bench-build integration
python3 benchmarks/run.py --outbox --output benchmarks/results/outbox-screening-001
python3 benchmarks/summarize.py benchmarks/results/outbox-screening-001
make lab-down
```

Use a new output directory. Raw evidence remains local and ignored under
`benchmarks/results/outbox-screening-20260919/`:

- `manifest.json`: `b66bf3f12c43c80d9d0fd1c5cfe01648a5ccabdbeece35542accde80264122f6`
- `summary.json`: `533c5379904510915b76a4fafd80c4f5f611f29f35156d357f31bda8a7fb5159`
- `source.tar.gz`: `11eafcc9fb26f7181e6c12796fb5240613d690df566356bcfa434d8082569856`

The summary contains per-case raw-result checksums. Earlier short smoke runs are
separate evidence, not additional repetitions of this cohort. No commit or
public artifact upload has been performed.

O3 remains open: an equivalent pinned official pg-boss/Node baseline, repeated
held/backlog/high-load trials, retained-history/long-soak tests, mixed-stream and
fenced multi-replica benchmarks are still needed. Provider installation rights,
real consumer compatibility, authentication/ACL acceptance, lost-ACK/takeover
cases and the producer cutover remain separate gates. This experiment supports
a queue candidate, not a claim that pg-boss has already been safely replaced.
