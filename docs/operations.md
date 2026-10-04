# Runtime configuration and worker health

These settings apply to CDC and registered business outboxes. Installation is
explicit; starting a worker never creates a source or chooses routes from event
data. Multi-stream routes are described in [outbox.md](outbox.md).

CDC installation accepts logged ordinary tables with primary keys and no
inheritance children. Partitioned roots, views, temporary/unlogged tables and
keyless tables are rejected atomically. Capture registrations describe explicit
physical tables; schema/topology changes require coordinated migration and
revalidation. Real cascade, primary-key move, exact numeric/binary/NULL preservation
and TRUNCATE rejection integration checks pass.

Install bundled PgQue once with `row-relay --install-pgque`, using an administrative
connection. The SQL and its license notices ship with the binary/image; consumers
do not fetch a separate SQL file. This command rejects an existing `pgque` schema,
including concurrent installation, and rolls back failed setup. It never upgrades
or resets an existing installation. Then bootstrap a source once with
`row-relay --install` (without `--tables`). Application migrations can subsequently
call `rowrelay.install_capture('schema', ARRAY['table'])` after their domain DDL.
The caller must own those tables and have schema usage/function execution rights;
the capture trigger itself enqueues with its trusted installation owner's rights.
Keep the source epoch across redeployments and give its expected value to consumers.

## Offline configuration validation

`--check-config` validates the effective configuration and exits without
connecting to PostgreSQL, Kafka, or the schema registry, without starting an
installer, and without opening the probe listener or any worker goroutine. It
reads only the explicit `--config` file, if given. Required configuration is
still checked: a single-source run needs its `DATABASE_URL`, broker and, for CDC
Protobuf, `SCHEMA_REGISTRY_URL` environment; a multi-stream run resolves every
`database_env` reference and applies the same strict route validation as
startup, so each referenced variable must be present. A missing or invalid value
fails with a safe message, and no source, broker or registry connection is
attempted. On success it prints one bounded summary line (version, stream count,
summed byte budget, delivery mode, poll and timeout) and never prints DSNs,
credentials, or row data.

## Stream limits

A multi-stream configuration holds 1..32 streams, and the sum of their
`batch_bytes` budgets must not exceed 64 MiB. Each route's `batch_bytes` is
1..64 MiB and defaults to 4 MiB, so sixteen default routes already reach the
64 MiB aggregate bound. Within the stream cap, more streams require a smaller
per-route `batch_bytes`: 20 streams at `--batch-bytes 2097152` (2 MiB each) sum
to 40 MiB and are accepted, while 33 streams exceed the 32-stream cap and, at
that size, would also sum to 66 MiB, so they must be split across process
configurations. The bounds are intentional and are not raised in place.

## Kafka transport

| Environment variable | Meaning |
| --- | --- |
| `KAFKA_BROKERS` | Comma-separated broker addresses; empty entries are rejected |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` (default), `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL` |
| `KAFKA_SASL_USERNAME` | Username for SCRAM-SHA-512 |
| `KAFKA_SASL_PASSWORD` | Password for SCRAM-SHA-512; preserve its exact bytes |
| `KAFKA_TLS_CA_FILE` | Optional PEM CA bundle added to system trust roots |

For example, provide credentials through the process environment or the platform's
secret facility, then select the transport and launch the configured streams:

```sh
export KAFKA_BROKERS='broker.example:9093'
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_TLS_CA_FILE=/run/secrets/kafka-ca.pem
./bin/row-relay --config streams.json --health-address 127.0.0.1:8080
```

TLS requires at least TLS 1.2 and verifies both the certificate chain and each
broker hostname. There is no insecure-verification switch or plaintext fallback.
Credentials without an explicit SASL protocol, incomplete credential pairs, and
CA configuration on a plaintext transport are errors. Secrets and driver error
details are not included in CLI failure summaries or probe responses.

Streams share Kafka transport settings, with a separate bounded producer per
stream. Database credentials remain per-stream environment references. Restart
the process after changing credentials or trust files. Mutual TLS, live credential
rotation, and SASL mechanisms other than SCRAM-SHA-512 are not implemented.

The local benchmark uses plaintext. TLS/SCRAM workload costs and authentication
against the intended broker/ACL configuration require separate verification.
`make lab-auth-up integration-auth` verifies the real TLS/SCRAM and exact
topic/transactional-ID ACL path against pinned Kafka JVM 4.3.0; see the
[auth lab runbook](../local/README.md#tlsscram-and-acl-acceptance).

## Optional operational probes

`--health-address` enables the HTTP listener; it is disabled by default. Use an
explicit address appropriate for the deployment's probe network.

| Endpoint | Contract |
| --- | --- |
| `GET /live` | HTTP 200 while the HTTP server responds, including during worker retries |
| `GET /ready` | HTTP 200 when every worker is active or has successfully checked that it is standby, and none is failed, stalled, or stopping; otherwise 503 |
| `GET /status` | HTTP 200 with the same per-worker state and overall readiness |

Responses contain configured stream names and `starting`, `active`, `failed`, or
`stalled` or `standby` states. A successful step includes an empty read. A step containing
records is reported only after its source ACK succeeds. Success expires after
the stream operation timeout plus its poll interval and one second of scheduling
margin. A retrying poisoned stream makes overall readiness fail while independent
healthy streams continue. State transitions are logged once instead of once per
poll. Shutdown makes readiness fail and drains the probe listener.

**Worker readiness is not source freshness or consumer catch-up.** Every status
response explicitly contains `"freshness_certified": false`. Empty source polls
do not revalidate broker reachability; a broker error is observed by the next
Kafka operation. These endpoints must not authorize cache reads, advance a CDC
visibility frontier, or certify that application effects have been applied.
Those guarantees require the [progress-barrier/consumer protocol](progress.md)
and a conforming application adapter.

Healthy long drains stay operational: real worker activity (completed operations
and chunk progress) refreshes readiness while a stream is genuinely making
progress, so a large snapshot drain does not appear stalled between step
boundaries. Activity renews only a worker that is already active or standby; a
worker still has to complete its first successful step to become ready, and
healthy empty polls keep it ready. This is liveness only. It does not advance the
source ACK boundary and does not certify a CDC closed boundary, consumer
position, or freshness; `freshness_certified` remains false. Readiness fails for
failed or stopped workers and for any worker without operation progress before
its deadline.

Probe responses exclude DSNs, passwords, row bytes, and raw error messages. The
listener uses bounded HTTP header/read/write timeouts; it does not serve events
or offer administrative mutation endpoints.

### Delivery profiles

`--delivery-mode fenced` is the default: Kafka transactions, stable transactional
ID and `read_committed` consumers. This profile requires transactional-ID ACLs.

`--delivery-mode managed` uses Kafka's ordinary idempotent producer with `acks=all`.
It makes no transaction-coordinator or transactional-ID requests. The existing
principal must support ordinary idempotent publishing and topic access; no Kafka
administration or transaction permissions are requested. There is no automatic
fallback between profiles. A route may override the CLI default with
`"delivery_mode": "managed"` in its stream configuration.

Both profiles retain stable event IDs/bytes and acknowledge the complete PgQue
batch only after every Kafka chunk succeeds. Managed partial successes are
immediately visible, including to `read_committed` readers, and replay can produce
duplicates. Consumers must apply stable-ID deduplication or idempotent effects.

Managed mode without replay takeover or explicit invalidation support requires **one publisher per source**, stop-before-start updates
(for example, one Kubernetes replica with `Recreate`), and confirmation that a
previous instance has stopped before replacing an unreachable node. Session locks
still guard source reads/ACKs; idle ownership expiry is disabled. They cannot fence
a paused process at Kafka after its database connection is lost. Ordered live
takeover, standby readiness and `--progress-interval` are unsupported in this
configuration. An ownership conflict makes that managed worker unready.

For a registered outbox whose consumer atomically commits stable-ID deduplication
with its business effects, set `"managed_takeover": true` on the stream route.
This enables idle ownership expiry and ready standbys without Kafka transactions.
Late duplicates remain visible in Kafka; the consumer receipt prevents repeated
effects. It does not enable CDC freshness certificates. See the
[managed replay contract and failure tests](ownership.md#managed-replay-takeover).

See [ownership.md](ownership.md) for the full protocol and failure boundaries.

For CDC adapters that only invalidate caches, `"managed_invalidation": true` with
a positive `progress_interval` enables the distinct managed invalidation protocol.
Use `schema_topics` for multiple schemas sharing one source database; omit `topic`
and `outbox_stream` on that route. It supports ready standbys without broker fencing,
but never generic row materialization. Read the complete
[managed invalidation contract](progress.md#managed-invalidation-only-profile).

## Diagnostics

Startup and lifecycle messages are timestamped and structured. Startup records
the effective bounded runtime shape (version, stream count, delivery mode, poll
and timeout); the configuration file is read within a 64 KiB limit. Per-stream
transitions (active, standby, ownership conflict, failure) are logged once, and
failures carry a simplified safe reason instead of a raw driver error. Repeated
failures are summarized at a bounded cadence, recovery records the outage
duration and failure count, and shutdown records each stream's final state and
failure count. Messages never include DSNs, broker addresses, credentials, or
row payloads.

## Container packaging

```sh
make container-build
docker run --rm --read-only --cap-drop=ALL --security-opt=no-new-privileges \
  --memory=128m --cpus=1 --pids-limit=64 \
  -e GOMAXPROCS=1 -e GOMEMLIMIT=96MiB \
  -e DATABASE_URL -e KAFKA_BROKERS \
  rowrelay:local --topic your-precreated-topic --outbox-stream events
```

The multi-stage build pins the Go builder by digest. The final image contains
only the static binary and system CA bundle, runs as numeric UID/GID 65532, and
supports a read-only root filesystem. Build context excludes credentials, local
results, third-party checkouts and application configuration. Configuration and
private CA files can be mounted read-only; they must be readable by UID 65532.
Provide Kafka security environment variables as described above when needed.

Installation remains an explicit operation using a suitable migration role;
starting this container never installs PostgreSQL objects or Kafka topics.
For probes, pass `--health-address 0.0.0.0:8080` and expose it only to the
deployment's probe network. The scratch image has no shell or HTTP probe client;
use the orchestrator's HTTP probes. SIGTERM initiates bounded shutdown. If it is
killed before source ACK, takeover replays the unacknowledged snapshot.

`make integration-container` builds locally and checks real delivery, source ACK,
non-root/read-only operation and graceful stop against the isolated lab. It does
not publish an image or deploy to shared infrastructure.

## PostgreSQL role separation

The integration suite verifies separate, non-superuser, non-replication
application and publisher roles. Installation uses the table/migration owner;
runtime connections do not need business-table write access or `pgque_admin`.
For pre-created roles named `app_writer` and `rowrelay_publisher`, the tested
grants are:

```sql
-- Grant ordinary application DML only on its actual business tables.
GRANT SELECT, INSERT, UPDATE, DELETE ON public.your_table TO app_writer;

-- CDC: the installed SECURITY DEFINER trigger enqueues as its trusted owner.
GRANT USAGE ON SCHEMA rowrelay TO rowrelay_publisher;
GRANT SELECT ON rowrelay.source TO rowrelay_publisher;

-- Business outbox: these replace the preceding CDC metadata grants as needed.
GRANT USAGE ON SCHEMA rowrelay_outbox TO app_writer, rowrelay_publisher;
GRANT EXECUTE ON FUNCTION rowrelay_outbox.enqueue(text,uuid,bytea,bytea,jsonb)
  TO app_writer;
GRANT SELECT ON rowrelay_outbox.stream TO rowrelay_publisher;

GRANT pgque_reader TO rowrelay_publisher;
GRANT EXECUTE ON FUNCTION pgque.batch_event_sql(bigint), pgque.batch_event_tables(bigint),
  pgque.quote_fqname(text), pgque.ticker(text),
  pgque.maint(), pgque.maint_rotate_tables_step2() TO rowrelay_publisher;
GRANT UPDATE(queue_switch_step2) ON pgque.queue TO rowrelay_publisher;
```

Apply the relevant CDC/outbox statements only after explicit installation. The
last column grant is required by upstream rotation step 2, which runs with the
caller's privileges; it does not grant arbitrary queue configuration changes.
PgQue maintenance and its reader role are database-wide capabilities, not
per-stream tenant isolation. The application enqueue grant permits every
registered outbox stream in that database. Roles and connection access must match
that trust boundary. No role passwords are embedded in installation scripts.

The real-role test proves application writes/capture, outbox enqueue, publisher
delivery/progress and maintenance work with these grants. It also proves the
application cannot acknowledge source batches or change epochs, and the publisher
cannot write business rows, change source epochs or drop queues. Existing
installation-owner credentials are not a substitute for running this role split
in the intended managed PostgreSQL environment; that provider's installation
rights remain to be checked.
