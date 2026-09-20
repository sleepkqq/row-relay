# Queue selection after corrected fencing runs — 2026-09-19

**Decision: PgQue selected; pending retired at the owner's request.** The corrected
six-case cohort reconciled **1,170,000 events**, with zero missing, corrupt,
phantom or duplicate events and complete source acknowledgements. Pending's
maintenance failure was fixed, but its held-snapshot latency still failed the
preregistered 1,000 ms p99 gate. No further pending benchmarks are scheduled.

This is a separately frozen cohort, not a completion or rewrite of the
[failed 19-case cohort](2026-09-19-fenced-outbox-screening.md). The six cases contain
no new steady repetitions; do not merge different binaries into one experiment.

## Conditions

Same shared Ryzen 9 9900X host and isolated PostgreSQL 18.4 / Kafka Native 4.3.0
lab as the preceding report. Database and broker: two CPUs / 768 MiB each. Relay:
one CPU / 128 MiB hard memory ceiling, 96 MiB Go soft limit. PostgreSQL durability
enabled; Kafka `acks=all`, idempotence, transactions, RF=1 / min-ISR=1;
`read_committed` consumer, one partition, Zstandard, 50 ms poll/tick,
1,000 records / 4 MiB chunk bounds. Source, binaries and configuration are frozen.

Synthetic 1 KiB prepared values retain IDs, keys and headers. Source writes and
the independent checksum ledger commit together. This is not Protobuf or a
business-consumer test. Latency is producer-operation start to Go decode.
The held case measures 300 seconds at 1,000 offered events/s after 10 seconds of
warm-up. Pending retained completed rows for 30 seconds; PgQue used 10-second
rotation plus snapshot/consumer safety. Disk retention shapes are not identical.
Source/document edits occurred during the run; heavy checks ran outside it.

## Results

| Held-snapshot metric | Pending | PgQue |
| --- | ---: | ---: |
| Decode p99, ms | 36,575.702 | 206.352 |
| Decode maximum, ms | 38,208.729 | 962.356 |
| PostgreSQL CPU, % of one core | 50.595 | 15.442 |
| Relay CPU, % of one core | 1.592 | 2.020 |
| Exact relay cgroup lifetime peak, MiB | 37.152 | 19.832 |
| Queue size after drain, MiB | 1,227.359 | 58.797 |
| Estimated dead tuples after drain | 561,233 | 17,165 |
| Held p99 gate | Fail | Pass |

Prepared 100,000-event drain **through final source ACK**, including relay
container startup: pending **14.937896 s / 6,694.38 events/s**; PgQue
**5.100161 s / 19,607.23 events/s**. These totals add `recovery_s` to
`producer_elapsed_s`; decode-only rates overstate completed drain capacity.

PgQue idle CPU was 0.377% of one core and exact lifetime peak 10.801 MiB.
At 5,000 offered events/s, its 300,000 measured events took 87.476913 seconds
(3,429.48/s), with admission p99 27,588.404 ms and decode p99 945.197 ms.
It **did not sustain 5,000/s**. Exact relay peak was 26.930 MiB.

Selection is workload-specific, not a universal ranking. PgQue is now the sole
RowRelay backend; the official pg-boss/KafkaJS fixture remains the external
comparison. Managed installation rights, production consumer compatibility,
broader fault cases and a two-hour soak remain acceptance gates.

## Preserved evidence

Ignored directory: `benchmarks/results/fenced-cleanup-correction-20260919/`.
The archive retains the measured pending implementation after its removal from
current source. Existing installations/data are never automatically deleted or
converted; legacy pending installations are rejected by the current relay.

| Artifact | SHA-256 |
| --- | --- |
| `manifest.json` | `fd1abaecac1ea0e5b4a1e4edc095f982a35a17b03674836404f6933bc9f262be` |
| `summary.json` | `f692c09f260802c32614221ad7a6d46ae92d6ac9abe642ac990fe20d54fe1fd8` |
| `source.tar.gz` | `a93c936ad898e784dd5df689027bcbfde9d68a731e627ebf136ce00605393dba` |
| `row-relay` | `d079713935f03edfcb392d072246f2633f0da4c8d2b5202dcf6fa3ad441f045f` |
| `row-bench` | `88d9519f8495a7f408e9a5a3561f3b9e2cf12ebcf39d5864062d8ca651198904` |
