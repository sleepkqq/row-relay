# PgQue business-outbox two-hour soak — 2026-09-19

**Status: completed, correctness reconciled.** This is the frozen **fenced Kafka
transaction** runtime, not the subsequently added managed-provider delivery profile.
It verifies sustained delivery and repeated queue rotation under the conditions
below; it is not a production-readiness or multi-broker durability certification.

## Workload and evidence

- Started at `2026-09-19T17:53:36Z`: 60 seconds warm-up, then 7,200 measured seconds
  at 500 events/s. One source, one Kafka partition, one active publisher.
- Synthetic prepared records: int64 ID + int64 timestamp + 1 KiB payload, stable
  delivery-ID header and key. This workload does not exercise Protobuf decoding.
- Pinned unmodified PgQue 0.2.0 at
  `e8ee488d2c1d87ab09eed2581ec4bbc1e68315f6`; 50 ms poll/tick, 10-second rotation.
- PostgreSQL 18.4 and Kafka Native 4.3.0; ordinary PostgreSQL durability, Kafka
  `acks=all`, transactions, `read_committed`, Zstandard. Single broker, RF=1.
- Relay: one CPU, 128 MiB hard memory limit, 96 MiB Go soft limit. PostgreSQL and
  Kafka: two CPUs and 768 MiB each. Shared Linux host; exact versions, images,
  source archive and binary hashes are recorded in the frozen manifest.
- Independent Go-generated expected ledger committed with source writes; Kafka
  decoded bytes/identity reconciled against it, followed by complete source ACK.

**Environment limitation:** dev deployment work, short container builds and focused
integration tests overlapped this run on the same host/local lab. Resource totals
and latency include that interference and diagnostic sampling. Treat them as
observations from this soak, not isolated runtime-cost measurements or a measured
managed-mode speedup. No broker profile was switched during the run.

## Results

| Observation | Result |
| --- | ---: |
| Committed and uniquely delivered, including warm-up | 3,630,000 |
| Measured events | 3,600,000 |
| Missing / corrupt / phantom / duplicate | 0 / 0 / 0 / 0 |
| Final source ACK | Complete |
| Measured producer interval | 7,200.001 s |
| Final reconciliation/source-ACK recovery | 0.101 s |
| Delivered measured events / producer interval | 500.000/s |
| Transaction-start → decode p50 / p95 / p99 | 62.14 / 123.31 / 183.51 ms |
| Maximum transaction-start → decode | 2,291.13 ms |
| Write-transaction p99 | 45.79 ms |
| Scheduled admission delay p99 / maximum | 1,807.63 / 5,147.43 ms |
| Relay CPU, percentage of one core | 1.56% |
| Relay exact cgroup peak since startup | 22.73 MiB |
| Sampled relay process RSS peak | 25.79 MiB |
| PostgreSQL / Kafka CPU, percentage of one core | 10.64% / 6.78% |
| Final queue size | 19.70 MiB |

Admission delay is retained separately: the producer fell behind its offered
schedule during parts of the run. Transaction-start latency does not include that
delay and is not commit-to-application latency. RSS and cgroup memory have different
accounting definitions and are not interchangeable.

There are 7,191 diagnostic observations and 240 queue-storage samples. Observations
contain 716 distinct rotation timestamps and all three event-table positions.
Sampled queue storage ranged from 20.38 to 31.26 MiB; its first/last 30-minute
medians were 26.77/24.52 MiB. This run shows repeated rotation and bounded observed
queue storage rather than monotonically accumulating the complete event history.

## Frozen artifacts

Local raw directory: `benchmarks/results/outbox-soak-20260919/`. It contains the
measured executables, source archive, manifest, raw JSON, diagnostic JSONL, logs
and summary. These ignored artifacts have not been overwritten. Executable and
source-archive hashes were rechecked after completion.

| Artifact | SHA-256 |
| --- | --- |
| `manifest.json` | `64e1449dc76759b18994a4505c40a54bd9d398067b64433af727a56363dfab6b` |
| `source.tar.gz` | `f2f44f5528652c72b8fd68d5d8d6215deae863abd694ac8884822766f57cefca` |
| `row-relay` | `1374c5172d35cd6956434b1b67aaa87dcce5155d2194cebfa47dd17603f4dbe9` |
| `row-bench` | `c184ee802f9f51b8cf8b363a41947b8a81bf2437d075b7564587e5ba93fce824` |
| `soak-pgque.json` | `5d0f1e5acd1ac2c4ac28b671cbb0e506567985b856cf12b3ac37da1e69be77ae` |
| `summary.json` | `07901a715ef9402e8d87749647121b23389fc500df922c6ac4f337d1b861f9f7` |
