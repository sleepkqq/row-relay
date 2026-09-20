# Ownership, fencing, and ambiguous acknowledgements

The ownership unit is one registered outbox stream, or one database CDC source.
The protocol below applies to the default **fenced** delivery profile. Consumers **must configure
`isolation.level=read_committed`** (franz-go: `FetchIsolationLevel(ReadCommitted())`).
Stop pre-transactional publishers before upgrading; mixing unfenced writers into
the same route invalidates the protocol.

The explicit **managed** profile keeps PostgreSQL ownership, complete-batch source
ACK and stable replay identities, but uses ordinary idempotent `acks=all` publishing
without Kafka transactions or transactional-ID ACLs. Partial writes are visible;
there is no stale-publisher broker fence or certified freshness. By default it
requires one publisher and stop-before-start replacement, with idle ownership
expiry disabled. Registered outboxes can opt into the consumer-protected replay
takeover below. See [the operational constraints](operations.md#delivery-profiles).

## Managed replay takeover

For a registered business outbox, `managed_takeover: true` in its stream route
(or `--managed-takeover` for the CLI) enables PostgreSQL idle ownership expiry
and operationally ready standbys. It requires `delivery_mode: managed`; CDC and
freshness certificates are excluded. No Redis or additional Kafka privileges
are needed. PostgreSQL already provides the per-stream ownership lock.

This mode protects **business effects at the consumer**, not Kafka's raw log:

1. Enqueue immutable bytes and stable delivery IDs in the business transaction.
   The existing per-key enqueue lock must precede event-ID allocation; keep Kafka
   partition counts and key partitioning fixed.
2. A replacement resumes the same durable PgQue subscription and whole pending
   snapshot batch. Only successful Kafka ACKs permit source ACK, on the original
   still-owned PostgreSQL connection. A failed owner never reconnects to ACK.
3. The consumer processes its partition serially. It stores the delivery receipt
   and a content digest **in the same database transaction as domain changes and
   any downstream outbox**. A matching duplicate does nothing; the same ID with
   different key/content fails closed. Kafka offsets advance only after commit.
4. An old process can still publish after takeover. For example, the log can
   contain `A1, A2, A1`. Durable receipt checking yields effects `A1, A2`, never a
   second `A1`. `read_committed` alone does not filter these nontransactional duplicates.

For one key, each publisher sends an ordered prefix of the same durable pending
work; a successor cannot skip unacknowledged work. Consequently late publication
can add repeats, but cannot justify skipping a previously unapplied event.
This argument depends on the producer/consumer conditions above, not just a lock.

Do not replace the transactional receipt with Redis `SETNX`: recording it before
SQL commit can lose effects on a crash, and recording it afterwards can duplicate
them. A Redis lease also cannot revoke an old publisher's Kafka connection.
Receipt/state retention must cover every permitted replay and delayed publisher;
an arbitrary receipt TTL is unsafe. Keep aggregate deduplication state durably,
or prove a replay floor before removing receipts. External nontransactional side
effects still require their own idempotency key or transactional outbox.

Multiple replicas provide active/standby failover per source. Independent streams
can be assigned to different replicas/deployments for throughput. This does not
parallelize a single queue, automatically rebalance streams, or certify freshness.

`TestManagedTakeoverDeduplicatesLatePublisherEffects` runs two real processes
against PostgreSQL and Kafka. It tests connection loss and automatic idle expiry,
then deliberately lets the old process publish after the successor's complete
ACK. It verifies visible `first, second, first` records, rejected stale source ACK,
exactly `first, second` database effects, rollback before consumer commit, and
rejection of conflicting content under the same ID. This is separate from the
stronger broker-fenced tests below.

## Acquisition and delivery

1. Open a dedicated PostgreSQL connection and acquire the stream's session-level
   advisory lock. Never reconnect it inside a batch. Use a direct connection;
   transaction/statement pooling is unsupported.
2. Set PostgreSQL `idle_session_timeout` and `idle_in_transaction_session_timeout`
   to the operation timeout plus the poll interval and five seconds. This requires
   PostgreSQL 14+. The server releases abandoned ownership sessions independently
   of client scheduling, including a publisher paused inside a cursor transaction.
3. Validate source mode/route. Create a Kafka producer with transactional ID
   `rowrelay-<persistent-source-epoch>`. Initialize its producer ID under source
   ownership, fencing its predecessor before reading work.
4. Read complete PgQue snapshot membership through a read-only cursor. The maximum
   source record size bounds each fetch by count and bytes. Close each FETCH result
   before publishing and execute a source-session query before every Kafka chunk;
   resetting a Go timer alone would not renew PostgreSQL ownership. Each bounded Kafka
   chunk has its own transaction: begin, produce with `acks=all`, then obtain a
   successful transaction-commit response.
5. After every chunk succeeds, start a short PostgreSQL transaction on the
   original connection. Verify its exact advisory lock, then acknowledge the
   complete PgQue batch and commit.

The drain holds a read-only cursor transaction, not business-row locks. Its local
timeout measures inactivity, not the total snapshot duration. Source read buffers
and encoded Kafka buffers are separately bounded; a large row can reduce fetch
size for the entire snapshot. Historical throughput results predate this cursor
change and are not new measurements of its cost. A disconnected
former owner cannot reconnect to ACK; a live but manually unlocked connection
also fails the ownership check.

A failed step makes the Runner terminal. Its Kafka client is closed, not reused
to recover a producer epoch without source ownership. The supervisor reacquires
the source lock before creating a new producer. Reopen delay is at most five
seconds; default abandoned idle-session expiry is about 35 seconds. This is not
a recovery SLA: unavailable infrastructure, active blocked SQL and source poison
can delay recovery.

## Commit boundaries

- Partial Kafka acceptance remains invisible to `read_committed` until commit.
  A successor's epoch initialization aborts an unfinished predecessor transaction;
  broker transaction timeout also bounds abandoned transactions.
- A lost Kafka commit response leaves the source unacknowledged even if Kafka
  committed. Replay preserves IDs, keys, values and headers: **at-least-once**,
  not cross-system exactly-once.
- A lost source commit response stops the owner. Its successor reads durable
  source state: committed ACKs survive; uncommitted ACKs cause replay.
- An old process resuming after takeover cannot commit its old Kafka epoch or
  acknowledge through its lost PostgreSQL session.
- Earlier chunks of a large PgQue batch may commit before a later chunk fails.
  The whole unacknowledged snapshot batch replays. Consumers must deduplicate
  stable identities before applying effects.

Read-uncommitted consumers can see aborted fragments and violate this contract.
Kafka transaction markers are not application events. The benchmark retains them
only to verify its final log position and excludes them from its event oracle.

## Replicas and isolation

Each stream has its own bounded producer and source loop inside the common
process. A shared transactional producer would couple independently owned streams.
Configured batch buffers total at most 32 MiB across at most 16 streams; this is
not a total-process RAM limit.

Replicas with identical multi-stream configurations contend for source locks.
A contender reports `standby` and retries ownership checks. Standby is operationally
ready while those checks continue, then expires to `stalled` if they stop. It does
not certify source freshness. Single-source CLI mode is fail-fast on contention;
use `--config` for supervised contenders.

Source-level takeover does not automatically balance or shard a single stream.
Poison blocks its whole stream. An unfinished Kafka transaction can also hold back
`read_committed` on a partition shared by other streams: use separate topics or
partitions for transaction-level failure isolation. Keep outbox partition counts
fixed when depending on key order. A source restore/clone must not reuse its epoch
against a live original. Restore/reseed tooling and CDC freshness barriers remain
separate work.

## Failure verification

`make integration` uses real PostgreSQL/Kafka and actual Go subprocesses:

- PostgreSQL proxy waits for committed `ReadyForQuery`, drops the source commit
  response, and verifies recovery without resending acknowledged work.
- Kafka proxy drops EndTxn responses and rejects reconnects. An independent
  committed reader proves Kafka committed; the source remains pending and replay
  preserves bytes and identities.
- Old process pauses after Produce ACK; its database session is lost; a successor
  publishes; the old process resumes and fails Kafka commit and source ACK.
  Both forced backend termination and automatic server idle-session expiry are
  exercised; idle expiry does not rely on the paused process running a timer.
- Two-partition publication accepts a small record and rejects an oversized one.
  An unsafe reader proves partial acceptance; a committed reader sees nothing
  until the topic limit is repaired and a new owner recovers the full transaction.
- A live connection with its advisory lock explicitly released cannot ACK.

Response-loss and takeover cases cover both CDC and outbox on PgQue.
Source-ACK response loss and partial broker acceptance/replay also run in managed
mode, asserting its visible-fragment semantics and unchanged replay headers.
Multi-broker quorum durability, every shutdown schedule, restore safety and
consumer freshness require their own acceptance checks.
