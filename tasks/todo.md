# Task status

This file owns the execution checklist. Details, dependencies, and acceptance
criteria live in [plan.md](plan.md); stable requirements live in
[requirements.md](../docs/requirements.md).

## P0 — Foundation

- [x] Clone the repository and initialize the Go module/CLI bootstrap.
- [x] Add formatting, vet, race-test, and build commands.
- [x] Configure quick GitHub Actions checks and dependency-update checks.
- [x] Write requirements, architecture candidates, and protocol semantics.
- [x] Record prior art, correctness matrix, and controlled benchmark program.
- [x] Define ordered milestones and contribution/agent instructions.
- [x] Complete local bootstrap verification and documentation-link check.
- [x] Refresh the open-source landing page, visual identity, documentation index
  and contributor/issue/PR guidance for the released CDC and outbox capabilities.
- [x] Run the workflow on GitHub after the authorized push: CI and the
  [release workflow](https://github.com/sleepkqq/row-relay/actions/workflows/release.yml) pass.

## P1 — Durable JSON slice

- [x] Pin local PostgreSQL/Kafka images and test a non-superuser/non-replication source owner.
- [x] Add transactional row capture and an independent expected-mutation ledger.
- [x] Implement one bounded pending-flag publisher with ACK-before-source-ACK.
- [x] Add reference JSON decoding and replay-safe event identity.
- [ ] Verify initial C01–C06 and outage/privilege/limit cases with a runnable command.

`make integration` currently covers committed full images, rollback/savepoints,
late commits, partial source-ACK failure/replay, poison records, and ordinary
concurrent-start rejection, atomic oversize rejection, and broker rejection
without source ACK. Successful-but-response-lost ACK is now covered. Separate
application/publisher roles pass real positive/negative checks, including
maintenance without `pgque_admin`. Broker quorum/outage and all type/size limits
still need work; managed-provider installation rights remain unverified.

## Active — Algorithm and resource screening

- [x] Integrate pinned PgQue 0.2.0 without modifying/reimplementing its snapshot engine.
- [x] Verify a snapshot batch larger than the Kafka chunk limit is fully delivered.
- [x] Add sequential benchmarks, independent reconciliation, cgroup CPU/RAM sampling,
  source/binary manifests, and raw result summaries.
- [x] Diagnose the long-tail result in the initial held-`xmin` snapshot run:
  WAL sync waits, excessive durable batch cadence, and a redundant sleep after
  empty completed batches. Verify immediate empty-batch advancement and compare
  the shared 10/50 ms cadence without weakening durability.
- [x] Complete repeated CDC screening and record the measured selection/tradeoffs:
  [17-case report](../docs/results/2026-09-19-local-screening.md).
- [x] Fix the 5,000 events/s cleanup maintenance timeout; retain failed-run logs,
  inspect query plans, and regression-test multi-chunk retention safety.

## Later milestones

- [x] O1 transport slice: transactional business-outbox ingress, original wire
  bytes/key/headers, registered streams and bounded CDC/outbox workers sharing
  one process; each source now owns its fenced producer. Mixed-stream/poison and
  source-ACK replay integration checks pass.
- [x] O1 failure windows: successful-but-lost source/Kafka commit responses,
  partial broker success and byte-identical replay on both queue backends.
- [x] O1 real consumer compatibility: downstream transactional application, commit
  failure, conflicting duplicates and retained-fact replay pass with actual consumers.
- [ ] O1 remaining resource/failure cases.
- [x] Add shared Kafka TLS/SCRAM configuration and operational health probes;
  verify configuration rejection, TLS trust/hostname checks, stale/shutdown
  readiness and source-step/poison reporting. This is not a freshness certificate.
- [x] Verify the managed profile against the intended SASL broker: original bytes/IDs,
  source ACK and rollback; direct partition reader avoids unrelated consumer-group privileges.
- [x] Add explicit managed idempotent/acks-all delivery without transactional-ID
  privileges. Verify partial-success replay and lost source-ACK responses in both
  profiles; reject managed freshness certificates and report ownership conflicts.
- [x] Verify TLS/SCRAM, bad credentials/untrusted CA and exact topic/transactional
  ID ACL rejection with a real pinned Kafka JVM broker in the isolated auth lab.
- [ ] O2: ordered multi-replica ownership, stale-publisher fencing and recovery tests.
- [x] Add opt-in managed outbox replay takeover with PostgreSQL ownership expiry
  and ready standbys. Two-process tests cover a late old publisher, consumer
  transaction rollback, conflicting duplicate contents and exactly-once database
  effects under the documented atomic inbox contract; no broker fencing claim.
- [x] O2 source-level takeover: stable Kafka transaction identity, conditional
  source ACK and two-process stale-owner tests for CDC/outbox on PgQue.
- [x] Verify automatic idle ownership expiry and takeover with a paused process.
- [x] Verify real CLI SIGTERM/SIGKILL during source ACK and byte-identical replay.
- [ ] O2 remaining: additional shutdown schedules, sharding/load
  balancing and multi-replica throughput acceptance.
- [ ] O3: equivalent outbox transport benchmarks: PgQue/pg-boss, backlog,
  sustained arrivals, retained history, mixed streams and multiple replicas.
- [x] Add the pinned official pg-boss/KafkaJS fixture, common atomic ingress and
  independent Go oracle; verify SDK enqueue rollback and both retained smoke paths.
- [x] Reproduce and diagnose the fenced cohort's held-snapshot cleanup timeout;
  bound each maintenance call to one chunk and pass the retention regression.
- [x] Verify the six corrected-runtime workloads: 1,170,000 events reconciled.
  Pending still failed held-snapshot latency; PgQue passed that gate.
- [x] Owner selected PgQue; remove pending storage, delivery, cleanup, configuration
  and future benchmark cases while preserving historical reports/artifacts.
- [x] Complete the initial 19-case pending/PgQue outbox screening and preserve
  [results and frozen artifacts](../docs/results/2026-09-19-outbox-screening.md).
- [x] O4 dev event-chat migration: real producers/consumers, compatibility, retained
  replay, pod takeover and pg-boss worker retirement; rollback runbook prepared.
  See [dev acceptance](../docs/results/2026-09-19-dev-event-chat-cutover.md).

- [ ] P2: closed progress boundaries, fencing, and consumer fail-closed behavior.
- [x] Add opt-in PgQue closed-boundary records and a cold-generation reference
  consumer with apply-before-progress, terminal errors and source-time expiry;
  focused race/unit checks pass. See [the protocol boundary](../docs/progress.md).
- [x] Run the broker-backed progress/late-commit/apply-failure/retention-gap cases.
- [x] Verify that poison after a committed prefix and external-ticker metadata
  cannot produce a closed-boundary certificate or acknowledge the failed batch.
- [ ] Complete cache-adapter acceptance, including in-flight fill fencing.
- [x] Add local cache read/fill fencing; race-test stale fills, invalidation ordering,
  expiry/failure gating and cross-generation tokens. Distributed-cache and ORM
  adapters remain separate acceptance work.
- [x] Complete the [18-case official pg-boss comparison](../docs/results/2026-09-19-pgque-pgboss-screening.md):
  2.22 million events reconciled. Record healthy-latency tradeoffs and the one
  PgQue steady-p99 gate miss separately from delivery correctness.
- [ ] P3: reproducible Eventuate/Go A/B baseline with reviewed raw evidence.
- [ ] P4: snapshot queue prototype, fault/rotation tests, and measured adoption decision.
- [ ] P5: typed Protobuf, Apicurio/JVM fixtures, and controlled format comparison.
- [x] Verify prepared typed Protobuf with real Apicurio 3.3.3 and JVM Kafka consumers:
  payload/header schema IDs, full OLD/NEW presence, exact numbers and binary data,
  unchanged ordered headers, and cold Registry-failure rejection.
- [ ] P6: retention/recovery/packaging, full fault/soak gates, release candidate.
- [x] Build the pinned multi-stage, non-root scratch image with CA roots; verify
  real delivery/source ACK, read-only operation and clean SIGTERM shutdown.
- [x] Verify separate application/publisher roles and the documented runtime grants.
- [x] Complete the [two-hour fenced outbox soak](../docs/results/2026-09-19-outbox-soak.md):
  3.63 million events, zero oracle errors/duplicates, complete source ACK and
  repeated actual queue rotations. Record shared-host build/test interference.
- [x] Deploy the managed outbox pilot to the intended dev broker using existing
  credentials and topic permissions; verify two Ready replicas, source ownership
  transfer after active-pod deletion and unchanged post-takeover delivery.
- [x] Document offline `--check-config` validation, bounded diagnostics and the
  existing 1..16 stream / 32 MiB aggregate limits for the 1.1.0 release.
- [x] Verify `make check`, `make integration` and `make integration-container`
  for 1.1.0, including the real long-drain readiness/ACK regression.
- [x] Publish and verify the tag-triggered 1.1.0 release.

Typed Go CDC now passes the real Apicurio/JVM contract test and local downstream
application-cache acceptance. Released dependencies, dev schema registration and
native application activation are complete. Live acceptance covered all nine capture
tables, six Ready application replicas, cross-replica invalidation after an external
SQL change, and business outbox replay without duplicate effects. Full HA certification
and controlled format comparisons remain pending. The 1.0.0 release includes a
versioned container, bundled unmodified
PgQue installer and tag-triggered verification/publication. Image
`ghcr.io/sleepkqq/row-relay:1.0.0` is published and anonymously pullable. P0's historical verification
below predates the implementation.

Version 1.1.0 passes local `make check`, integration and packaged-container gates,
including the long-drain readiness/ACK regression. It adds offline `--check-config`
validation, structured bounded diagnostics and drain-aware operational readiness;
the existing stream limits, wire protocol and delivery guarantees are unchanged.
The [1.1.0 release](https://github.com/sleepkqq/row-relay/releases/tag/v1.1.0)
and [tag-triggered verification](https://github.com/sleepkqq/row-relay/actions/runs/37160688861)
are complete. The published image is anonymously pullable; its version and the
13-stream offline configuration check also pass with networking disabled.
Image digest: `sha256:65be71c82599913e5bfa55a92673a05b504bf7c874137143ecb99fbb0af28675`.
The 1.0.0 evidence above is unchanged.

Local bootstrap verification (2026-09-19): `make check` passed on
`go1.27.0-X:nodwarf5 linux/amd64`; Go reports no test files yet. CLI smoke checks
covered version/help and rejection of unsupported startup/arguments. All 26
local Markdown links resolved; new-file whitespace checks passed and build
output is ignored. The GitHub workflow has not been executed.
