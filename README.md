# RowRelay

**Committed changes. Reliably delivered.**

A PostgreSQL outbox relay for Kafka, written in Go. Transactional triggers capture
row changes without logical replication or the PostgreSQL `REPLICATION`
privilege. Typed CDC and prepared outbox Protobuf/Apicurio records are verified
with real JVM consumers.

> **Release: 1.0.0.** Capture, bounded
> Kafka publishing, ACK-before-source-ACK, replay, integration tests, and a
> resource/correctness benchmark harness are implemented. Registered business
> outboxes preserve prepared wire bytes and can share a process with CDC. Source-level
> takeover in the default `fenced` profile uses Kafka transactions and requires
> `read_committed` consumers. `--delivery-mode managed` supports an idempotent,
> deployment without transactional-ID permissions. Registered outboxes can opt into
> [consumer-protected replay takeover](docs/ownership.md#managed-replay-takeover)
> with atomic delivery-ID deduplication; this is not broker fencing or a freshness certificate.
> Opt-in closed CDC boundaries and a fail-closed reference consumer are tested.
> Typed CDC and local application-cache acceptance pass; deployment acceptance
> and production hardening remain separate gates.
> See the [local lab](local/README.md) for the tested boundary.

## Why

Some managed PostgreSQL installations allow ordinary tables, functions, and
triggers but do not expose replication privileges. RowRelay captures full
`OLD`/`NEW` row images in the writing transaction and delivers committed changes
through Kafka. This includes writes made outside the application.

The goal is a small, focused relay with measurable resource costs and explicit
failure semantics. Go, a different queue algorithm, and a different wire format
will be evaluated separately.

```text
business transaction
  └─ SQL trigger → logged outbox
                       └─ RowRelay → Kafka → application consumers
                               └─ Apicurio schema lookup (pre-registered contracts)
```

## Delivery contract

- Durable **at-least-once** delivery: source acknowledgement follows Kafka ACK.
- Full insert/update/delete images, including old foreign keys and SQL NULLs.
- Explicit source-progress barriers; a heartbeat alone cannot certify freshness.
- Fail-closed handling of poison records, unknown schemas, and lost ownership.
- Bounded batches, replay, safe retention, and measured backlog recovery.
- Go-to-JVM Protobuf/Apicurio compatibility verified with real consumers.

PostgreSQL's ordinary durability WAL remains enabled. This project avoids
**reading WAL for CDC**; it does not eliminate WAL writes.

## Container

```sh
docker run --rm ghcr.io/sleepkqq/row-relay:1.0.0 --version
```

The image includes the relay, CA roots and the pinned PgQue 0.2.0 installer.
For a fresh database, supply an administrative `DATABASE_URL` and run once:

```sh
docker run --rm -e DATABASE_URL ghcr.io/sleepkqq/row-relay:1.0.0 --install-pgque
```

Then use the installation owner's connection to initialize a CDC source with
`--install`, or a business stream with `--install --outbox-stream NAME --topic TOPIC`.
Both installers refuse existing installations; runtime startup never installs or
upgrades database objects. See [operations](docs/operations.md) for runtime grants,
schema registration, routing and Kafka configuration.

Tests can run the same versioned image through Testcontainers. No local Go checkout,
binary extraction or SQL download is required. Releases are built and tested by
the [release workflow](.github/workflows/release.yml) from matching `vX.Y.Z` tags.

## Local development

Requires Go **1.27+** and Make. CI uses the latest 1.27 patch release. Integration
tests and benchmarks additionally require Docker; resource measurement requires
Linux with cgroup v2. PostgreSQL and Kafka clients are pinned in `go.mod`.

```sh
make check
./bin/row-relay --version
go run ./cmd/row-relay --help
```

`make check` checks formatting, runs `go vet` and `go test -race`, and builds
`bin/row-relay`. Unit tests run without infrastructure. For the real delivery
tests and local measurements:

```sh
make lab-up bench-build integration
python3 benchmarks/run.py --output benchmarks/results/screening-001
make lab-down
```

The benchmark creates and cleans up its own databases, topics, and relay
containers. Read the [lab runbook](local/README.md) for resource limits, metric
definitions, installation, individual cases, and the longer experiment options.

## Project documents

| Document | Purpose |
| --- | --- |
| [Requirements](docs/requirements.md) | Scope, guarantees, acceptance criteria, open decisions |
| [Architecture](docs/architecture.md) | Capture/delivery boundaries and queue candidates |
| [Queue selection](docs/results/2026-09-19-queue-selection.md) | Corrected fault-runtime measurements and the decision to retain PgQue only |
| [Protocol](docs/protocol.md) | Typed CDC semantics and verified Protobuf/Apicurio interoperability |
| [Business outbox](docs/outbox.md) | Transactional wire-byte ingress, multiple streams, migration and current limits |
| [Operations](docs/operations.md) | Kafka TLS/SCRAM, per-stream worker state and operational probes |
| [Ownership](docs/ownership.md) | Transaction fencing, source ACK, standby replicas and replay boundaries |
| [CDC progress](docs/progress.md) | Opt-in closed boundaries and experimental fail-closed reference consumer |
| [JVM / Apicurio fixture](interop/jvm/README.md) | Typed prepared-outbox compatibility and Registry-failure checks |
| [PgQue / pg-boss results](docs/results/2026-09-19-pgque-pgboss-screening.md) | Official-SDK comparison: CPU/RAM, healthy/held latency and complete backlog drain |
| [Two-hour outbox soak](docs/results/2026-09-19-outbox-soak.md) | 3.63 million events reconciled, repeated rotation and bounded observed queue storage |
| [Dev event-chat cutover](docs/results/2026-09-19-dev-event-chat-cutover.md) | Real Social/Chat consumers, replay/takeover acceptance and pg-boss worker retirement |
| [Correctness tests](docs/testing.md) | Failure matrix and independent delivery oracle |
| [Benchmarks](docs/benchmarks.md) | Controlled comparisons, workloads, metrics, result format |
| [Local CDC results](docs/results/2026-09-19-local-screening.md) | Measured CPU/RAM/latency and workload-specific queue decision |
| [Local outbox results](docs/results/2026-09-19-outbox-screening.md) | Wire-byte delivery, held-snapshot behavior and prepared-backlog drain |
| [Fenced outbox screening](docs/results/2026-09-19-fenced-outbox-screening.md) | Transactional-runtime steady results and retained held-snapshot failure |
| [pg-boss baseline](benchmarks/pgboss/README.md) | Official SDK fixture, common atomic ingress and comparison boundaries |
| [Research](docs/research.md) | Prior art, published evidence, and its limitations |
| [Local lab](local/README.md) | Runnable integration tests, relay setup, and resource measurements |
| [Implementation plan](tasks/plan.md) | Dependency-ordered, independently verifiable milestones |
| [Task status](tasks/todo.md) | Current execution checklist |
| [Contributing](CONTRIBUTING.md) | Development and review workflow |

PgQue is the only supported queue backend and the CLI default. Install the pinned
upstream PgQue 0.2.0 SQL engine with `--install-pgque` before installing a source. The pending-flag
implementation was removed after held-snapshot measurements and the owner's
selection; its frozen reports remain historical evidence. Existing pending
installations are rejected, never silently converted or discarded.
The relay's `--mode` / `--retention` flags and stream configuration `mode` field
were removed. Benchmark `--mode` still selects PgQue, the pg-boss baseline, or control.

## License

Copyright 2026 sleepkqq. Licensed under [Apache-2.0](LICENSE).
