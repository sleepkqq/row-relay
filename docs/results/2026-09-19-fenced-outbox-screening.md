# Fenced outbox screening — 2026-09-19

**Status: incomplete; pending held-snapshot case failed.** This cohort uses the
Kafka-transaction/fencing runtime and `read_committed` consumers. It is separate
from both earlier, nontransactional screening reports. Of 19 planned cases,
**13 passed delivery reconciliation, one failed, and five were not run** after
the failure. No complete-cohort or production-readiness claim follows.

## Setup and gates

The same local workload as the [earlier outbox cohort](2026-09-19-outbox-screening.md):
Ryzen 9 9900X, 24 logical CPUs, approximately 30 GiB host RAM; PostgreSQL 18.4 and
Kafka Native 4.3.0, each capped at two CPUs/768 MiB. Relay: one CPU/128 MiB,
96 MiB Go soft limit. Pinned source, binary and image identities are in the raw
manifest. Kafka has the corrected internal inter-broker listener required by
transaction coordination. PostgreSQL durability stays enabled; Kafka uses
`acks=all`, idempotence, transactions, RF=1/min-ISR=1 and Zstandard.

One source, one partition, synthetic prepared 1 KiB binary bodies with stable
IDs/keys/headers; these are not Protobuf. Source writes and independent oracle
entries commit together. Poll/tick 50 ms, batch 1,000/4 MiB, pending retention
30 seconds and PgQue rotation 10 seconds. Latency is producer-operation start to
Go decode, not exact commit-to-apply. Primary steady p99 gate: 250 ms; held-snapshot
p99 gate: 1,000 ms; hard relay memory ceiling: 128 MiB.

Five shuffled 60-second steady repetitions at 1,000 offered events/s follow
10-second warm-ups. Other planned cases are control, idle, 5,000 offered events/s,
300-second held snapshots and prepared 100,000-event backlogs. Source/document
editing and a small, script-disabled npm dependency installation occurred on the
shared host; no concurrent integration suite or heavy build ran during measurement.

## Completed measurements

Medians of the five steady repetitions:

| Metric | Pending | PgQue |
| --- | ---: | ---: |
| Decode latency p99, ms | 89.896 | 182.953 |
| Per-run p99 range, ms | 87.920–98.802 | 178.180–202.250 |
| Relay CPU, % of one core | 2.660 | 2.184 |
| Sampled cgroup peak, MiB | 17.297 | 18.871 |
| Exact lifetime cgroup peak median, MiB | 17.297 | 19.383 |
| PostgreSQL CPU, % of one core | 10.402 | 12.793 |

Both completed steady groups satisfy those steady latency/resource gates.
Across all 13 completed cases, **1,050,000 relayed events plus 70,000 control
writes** reconciled with zero missing, corrupt, phantom or duplicate events and
complete source ACK. This statement excludes the failed case.

Pending idle relay CPU was 0.255% of one core, with exact lifetime peak 9.973 MiB.
Pending's offered 5,000/s run delivered 300,000 measured events in 161.382 seconds
(1,858.94/s); admission p99 was 99.518 seconds and decode p99 411.660 ms. It did
not sustain the offered rate. Its exact relay peak was 21.645 MiB.

## Failure and remaining cases

The pending held-snapshot run terminated with:

```text
row-relay: maintenance (deadline exceeded); source remains replayable failed
```

The harness then encountered the removed container cgroup while sampling.
Container logs/state and the failure log are retained. The failed case has no
completed independent reconciliation and must not be counted as delivery-correct.
Cleanup diagnosis and a separately identified corrected-runtime rerun are required.

The separately instrumented reproduction also failed. Its preserved SQL plan
uses the published-row partial index and TID deletion; it is not a fallback to
a full-table DELETE join. At one observation the queue occupied 867,942,400 bytes,
with estimated 33,112 live and 355,972 dead tuples. Individual cleanup calls
reached 15.837 seconds in the sampled trace, repeatedly waiting on `DataFileRead`.
Several calls shared one 30-second maintenance-cycle deadline and exhausted it.
The fix yields after each 10,000-row chunk and continues on the next tick with a
fresh bounded call; it does not increase the SQL timeout or remove retention.
The regression asserts that one call leaves the next chunk pending and preserves
unacknowledged/recent/future rows. Full integration passed after the fix (72.832 s).
The corrected-runtime workload rerun is separate and is not included above.

Diagnostic evidence: `benchmarks/results/fenced-held-diagnosis-20260919/`, including
the continuously saved wait samples and `cleanup-plan-before.json`.

Not run: pending backlog; PgQue idle, 5,000/s, held snapshot and backlog. The
previous cohort's values cannot fill these gaps. No fenced-runtime worst-case
queue selection can be finalized from this incomplete matrix.

**Follow-up:** the separately frozen [corrected six-case cohort and selection](2026-09-19-queue-selection.md)
completed. Pending still failed held-snapshot latency, and the owner selected
PgQue and retired pending. The original incomplete-cohort results above remain
unchanged.

## Preserved evidence

Ignored raw directory: `benchmarks/results/fenced-outbox-screening-20260919/`.
The measured executables were copied and hash-verified before rebuilding.

| Artifact | SHA-256 |
| --- | --- |
| `manifest.json` | `53e3810b1b2715018bf4909d9fbbcae35e98f11d36718f1dad56bd12ce2a2235` |
| `summary.json` | `77f8dcd4daac32e775660f8eeb043444d532b063dab6727de477c3c6fb98a507` |
| `source.tar.gz` | `950e1fdf6cd50f4e4845224b3ebec5ba8079173eb9e421001551d9904f81c777` |
| `row-relay` | `160afca92ba3bf35b957dcef22aca5372c98a7c15e8edce42c1670a12c25f455` |
| `row-bench` | `147adc77ddabc922c141aaad416bd15588d3aab22ab9683703b7740e49d85cb2` |

This screening does not cover a two-hour soak, broker quorum failure, managed
installation privileges, real Protobuf consumers or certified cache freshness.
