# RowRelay documentation

**[← Project overview](../README.md)** · [Quick start](../README.md#quick-start) ·
[Releases](https://github.com/sleepkqq/row-relay/releases)

## Build an integration

| Guide | What you will find |
| --- | --- |
| [Operations](operations.md) | Installation, table support, runtime grants, Kafka TLS/SCRAM, containers and health probes |
| [Business outbox](outbox.md) | Atomic enqueue, byte-preserving delivery, multiple streams, ordering and migration |
| [CDC wire protocol](protocol.md) | Full row images, exact numbers, SQL NULLs, Protobuf and Apicurio framing |
| [Ownership and recovery](ownership.md) | Kafka fencing, managed delivery, source ACK and replay takeover |
| [CDC progress](progress.md) | Closed boundaries, fail-closed consumers and cache-invalidation contracts |
| [JVM / Apicurio fixture](../interop/jvm/README.md) | Runnable cross-language compatibility checks |

## Understand and verify

| Document | What you will find |
| --- | --- |
| [Architecture](architecture.md) | Capture and delivery boundaries, queue design |
| [Requirements](requirements.md) | Scope, guarantees and acceptance criteria |
| [Correctness tests](testing.md) | Failure matrix and independent delivery oracle |
| [Local lab](../local/README.md) | Disposable PostgreSQL/Kafka and runnable integration checks |
| [Benchmarks](benchmarks.md) | Workloads, metric definitions and reproducibility requirements |
| [Research](research.md) | Prior art and published evidence |
| [pg-boss baseline](../benchmarks/pgboss/README.md) | Official SDK fixture and comparison boundaries |

## Read the evidence

Results describe the versions and workloads that were actually tested. Historical
reports retain retired backends and failed runs; they do not expand the current
support contract. Raw local benchmark artifacts are not committed to this repository.

| Report | Focus |
| --- | --- |
| [Queue selection](results/2026-09-19-queue-selection.md) | Corrected failure-case measurements and the decision to retain PgQue |
| [PgQue / pg-boss comparison](results/2026-09-19-pgque-pgboss-screening.md) | 18-case official-SDK comparison, latency, CPU/RAM and backlog drain |
| [Two-hour outbox soak](results/2026-09-19-outbox-soak.md) | 3.63 million events reconciled and repeated queue rotation |
| [Dev event-chat cutover](results/2026-09-19-dev-event-chat-cutover.md) | Application consumers, replay/takeover and migration acceptance |
| [Local CDC screening](results/2026-09-19-local-screening.md) | Resource and latency measurements across CDC workloads |
| [Local outbox screening](results/2026-09-19-outbox-screening.md) | Wire bytes, held snapshots and prepared-backlog drain |
| [Fenced outbox screening](results/2026-09-19-fenced-outbox-screening.md) | Transactional runtime results and a retained failure |

## Contribute

[Contribution guide](../CONTRIBUTING.md) · [Implementation plan](../tasks/plan.md) ·
[Current task status](../tasks/todo.md) · [Report an issue](https://github.com/sleepkqq/row-relay/issues/new/choose)
