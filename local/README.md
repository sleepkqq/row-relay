# Local delivery and resource lab

This is a disposable **Linux / cgroup v2** lab, separate from existing services.
It requires Go, Make, Docker Compose, Git, and Python 3 (standard library only).
It uses PostgreSQL 18.4 and Kafka Native 4.3.0 pinned by image digest. Ports bind
only to loopback: PostgreSQL `25432`, Kafka `29092`. The `lab` credentials are
public disposable fixture credentials, not deployment defaults.

## Build and check

From the repository root:

```sh
make fetch-pgque
make lab-up
make check bench-build
make integration
```

`fetch-pgque` verifies the official repository, tag, exact commit, and clean
checkout. The clone stays ignored under `.slim/clonedeps/`; its provenance is in
`.slim/clonedeps.json`. SQL is installed only in newly created lab databases.
No third-party SQL has been copied into the application source.

Integration tests create uniquely named `rrlab_*` databases/topics and remove
their own fixtures. They exercise real Kafka ACKs and PostgreSQL transactions,
including rollback/savepoints, late commits, OLD/NEW images, batches larger than
the Kafka chunk limit, failed source ACK/replay, poison records, and rejection
of a second active publisher. They also cover committed-but-response-lost ACKs,
partial Kafka acceptance, and two-process takeover with a resumed stale producer.
Consumers use `read_committed`; multi-broker quorum failures are separate tests.

### TLS/SCRAM and ACL acceptance

```sh
make lab-auth-up integration-auth
make lab-up # restore the native broker before resource screening
```

Run this while no benchmark is active: switching modes recreates the disposable
Kafka service. The auth override uses pinned **Kafka JVM 4.3.0**, a separate
loopback TLS/SCRAM listener on `29095`, and the existing plaintext loopback/admin
and internal listeners. PostgreSQL is retained. The test creates a temporary
SCRAM-SHA-512 principal, grants only its exact topic/transactional-ID ACLs, and
removes its credentials and ACLs afterwards. Bad passwords, untrusted certificates,
missing topic ACLs and missing transactional-ID ACLs leave the source pending;
the authorized transaction is delivered to a committed reader.

OpenSSL 3 generates disposable 30-day certificates under ignored `local/secrets/`;
these are test fixtures, not deployment credentials. Existing files are reused.
Stop the lab before recreating expired fixture files.

Kafka Native 4.3.0 failed this real SASL test with `Unable to find suitable
Subject#doAs or Subject#callAs implementation` and missing reflective security
classes. Evidence is retained locally as `kafka-native-sasl-failure.log` under
`benchmarks/results/`. The official JVM image passes the same client test;
authentication has not been disabled or certificate checks relaxed to make it pass.

### Separate quorum fault fixture

```sh
make lab-quorum-up integration-quorum
make lab-quorum-down # restore the single native broker before measurements
```

This three-node combined broker/controller fixture uses loopback ports
`29092`–`29094`, RF=3 and min-ISR=2 for data and transaction-state topics.
The test kills the actual partition leader, attempts delivery with two replicas,
then kills a second node and requires source ACK to stop. Recovery checks the
original IDs/keys/bytes through Kafka's final high watermark. Killed fixture
nodes are restarted on test exit. Switching profiles recreates the disposable
Kafka service: run only when no benchmark is active.

The fixture is implemented but its first execution is pending completion of the
two-hour single-broker soak. It is not yet evidence of a passed quorum gate.

## Measure one case

Business-outbox ingress and multi-stream commands are documented in
[`docs/outbox.md`](../docs/outbox.md). Add `--outbox` to the case/matrix runner
for prepared binary records. The outbox matrix also preloads 100,000 records per
backend before starting delivery; this is separate from producer-rate tests.

```sh
GOMAXPROCS=2 ./bin/row-bench \
  --mode pgque --rate 1000 --payload-bytes 1024 \
  --warmup 10s --duration 60s \
  --output benchmarks/results/example-pgque.json
```

Use `--mode control`
for the same producer/oracle workload with capture disabled. `--rate 0` measures
idle resources. `--hold-xmin` pins a repeatable-read source snapshot during the
measured interval. `--diagnostics` additionally samples SQL waits and PgQue tick
lag; keep diagnostic runs distinct from comparable non-diagnostic runs.

The harness writes synthetic high-entropy text and an independent checksum
ledger in the business transaction. The consumer verifies full payload hashes
and source timestamps, distinguishes duplicates, waits for source ACK and Kafka's
final high watermark, and reconciles every expected row. Raw results survive a
correctness failure. Container logs and exit/OOM state are also saved beside the
requested result before cleanup, including when a runtime error prevents writing
the result JSON. Do not compare throughput from a failed oracle run.

## Repeated comparison

```sh
python3 benchmarks/run.py --output benchmarks/results/screening-001
```

Default: five PgQue repetitions at 1,000 events/s,
60-second measured windows plus 10-second warm-ups; capture-disabled control;
idle and 5,000 events/s cases; five-minute held-`xmin` cases. Runs are sequential.
The shared default tick/poll interval is 50 ms; use `--poll-ms` to compare another
cadence without changing only one algorithm's setting.
`--pgboss` selects the two-candidate PgQue/pg-boss outbox comparison, using common
Node ingress and no compression. Pending is retired and never scheduled.
The manifest records binary/source checksums, dependency/image versions, host
details, settings, and the selection policy before executing cases.

These are **screening runs**, shorter than the full program in
[`docs/benchmarks.md`](../docs/benchmarks.md). For its steady-run duration:

```sh
python3 benchmarks/run.py --output benchmarks/results/steady-001 \
  --steady-seconds 600 --warmup-seconds 60 --held-seconds 600
```

The two-hour soak, broader type/workload matrix, multi-broker fault tests,
Eventuate JVM baseline, and Protobuf comparison remain separate work. Avoid
builds or unrelated stress workloads on the host during measurement.

## Reading the results

- Relay container: **one CPU**, **128 MiB hard memory limit**, `GOMAXPROCS=1`,
  `GOMEMLIMIT=96MiB`. PostgreSQL and Kafka each have two CPUs / 768 MiB limits.
- CPU comes from cgroup `usage_usec` deltas. **100% means one fully used core**,
  regardless of host core count; 5% means approximately 50 millicores.
- Memory samples are `memory.current` and `memory.stat:anon`. Cgroup memory
  includes page cache/kernel charges. It is not Go heap size or process RSS.
  Peaks are sampled at one-second intervals, not guaranteed instantaneous peaks.
  For single-process containers the harness also records `/proc` RSS, and the
  relay's cgroup `memory.peak` includes startup/warm-up peaks without sampling loss.
- End-to-end latency is **producer transaction start → decoded event** on the
  same host clock. It is not exact commit-to-cache-invalidation latency.
- Admission delay exposes generator overload. A run that takes longer than its
  intended offered-load interval is not evidence that it sustained that rate.
- PostgreSQL statistics include the independent oracle workload and warm-up/setup
  statements. Dead tuples are estimates. Queue size is sampled at the end, after
  drain and release of the held snapshot, not a peak during the hold.
- PgQue uses a 10-second rotation interval and snapshot/consumer safety constraints.
  Its physical retention differs from pg-boss's completed-job cleanup. Disk figures
  are operational observations, not an identical-wall-clock-retention comparison.
- Kafka has **RF=1 / min ISR=1**, `acks=all`, transactional producers, one partition,
  and Zstandard compression. The lab measures algorithm/resource differences;
  it cannot establish multi-node durability or production capacity.

The broker has a separate internal listener for transaction-coordinator traffic.
Its host-facing advertised address is not reachable from inside the container.
Earlier non-transactional cohorts used the original single-listener configuration;
their frozen manifests and source archives remain unchanged.

To regenerate a summary from saved complete or partial artifacts:

```sh
python3 benchmarks/summarize.py benchmarks/results/screening-001
```

## Use the experimental relay manually

Create the business tables and an output topic with exactly one partition first.
Supply connection settings through the environment:

```sh
export DATABASE_URL='postgres://rowrelay_owner:lab@127.0.0.1:25432/example?sslmode=disable'
export KAFKA_BROKERS='127.0.0.1:29092'
./bin/row-relay --install --tables public.items
./bin/row-relay --topic example.cache-cdc
```

The `example` database/table/topic are placeholders that the operator creates;
these commands are not a fixture installer. Installation refuses an existing
source instead of resetting it. The relay requires the pinned upstream PgQue
SQL installed and its administrative grants. Its bootstrap creates cluster-level
roles; managed-provider installation rights still need verification.

Current scope: source-level fenced publishers, JSON row images and opaque business
outboxes. The default lab listener is plaintext; the separate auth fixture verifies
TLS/SCRAM. The source account is a non-superuser/non-replication database owner;
the production least-privilege split remains unverified. See
[ownership](../docs/ownership.md) for tested takeover and its limits. Consumer
freshness certification remains a separate acceptance gate.

Stop only this lab's containers when finished; volumes are retained:

```sh
make lab-down
```
