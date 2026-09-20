# Research and prior art

Research recorded: **2026-09-19**. These are external sources and source-code
observations, **not RowRelay benchmark results**. Mutable upstream pages must be
pinned to commits/digests when reused in an experiment.

## Non-logical-replication approaches

| Approach | Suitability for full row-change delivery |
| --- | --- |
| Poll current rows by timestamp/version/`xmin` | Misses deletes, intermediate states, and old keys without extra capture; naive high-water marks lose late commits |
| Full snapshots, hashes, Merkle-style comparisons | Useful for reconciliation of net state; expensive and not a full change history |
| Application-written outbox | Transactional when implemented correctly, but misses external SQL writers |
| Trigger-written logged outbox | Fits complete OLD/NEW capture from ordinary SQL without replication privileges |
| LISTEN/NOTIFY | Optional wake-up hint; requires durable storage and polling recovery |
| SQL proxy, audit logs, network calls inside triggers | Incomplete semantics or undesirable coupling for this target |

Queue selection is a separate question from capture. Pending flags, row leases,
and MVCC snapshot batches can all consume trigger-captured events, with different
ordering, retention, and acknowledgement costs. `SKIP LOCKED` is useful for work
queues; competing workers do not automatically preserve an ordered CDC barrier.

## Eventuate reference

The [Eventuate CDC source](https://github.com/eventuate-foundation/eventuate-cdc/tree/093d972738f7d885763d5ef06368dd2cbdbee0d6)
was inspected at commit `093d972738f7d885763d5ef06368dd2cbdbee0d6`, the examined
`0.19.0.RELEASE` branch revision. Its polling path selects unpublished rows,
publishes a batch, waits for Kafka futures, and marks the batch published with
one SQL UPDATE. It is **not** one UPDATE round trip per event, although it still
updates every event tuple and later deletes published rows.

The examined tree has 22 Gradle modules and roughly 7,793 physical main-Java
lines across 146 files, including blanks/comments and excluding dependencies.
The polling module itself is about 604 lines across 11 main files. Its broader
connector support should not be confused with the size of the relevant loop.

The existing JSON serialization path and transport can be adapted independently
of the queue algorithm. That is useful prior art, but RowRelay's experiment
must isolate runtime, queue, and format rather than credit all changes to Go.
Smaller memory/startup cost is a hypothesis; throughput may remain I/O-bound.

## PgQ and the snapshot candidate

- [Original PgQ / Skytools presentation, PGCon 2009](https://www.pgcon.org/2009/schedule/attachments/91_pgq.pdf):
  transactional batches, at-least-once delivery, batch acknowledgement, and
  rotating event tables. The design originated at Skype in 2006–2007.
- [PgQue repository](https://github.com/NikolayS/PgQue): a newer SQL/PLpgSQL
  implementation with a Go client; its documentation targets PostgreSQL 14+.
- [Two snapshots and a diff](https://thebuild.com/blog/2026/05/03/pgque-two-snapshots-and-a-diff/):
  explanation of visibility boundaries, in-progress transactions, and rotation.

The mature PgQ algorithm and a newer implementation's operational maturity are
different claims. Audit installation privileges, maintenance, fencing, and
failure tests before adopting SQL. No PgQ/PgQue code or dependency is currently
vendored. Check upstream licenses and preserve notices if reuse is selected.

“Zero bloat” applies to append-only event-table update churn, not zero retained
rows, zero metadata writes, or bounded disk during an indefinite consumer stall.
No per-event update does not mean no WAL: ordinary durable PostgreSQL writes
still generate WAL. Snapshot batching also does not establish global commit order.

## Published measurements and limitations

| Source / setup | Reported result | What it does and does not show |
| --- | --- | --- |
| [PgQue xmin-horizon](https://github.com/NikolayS/PgQue/blob/main/benchmark/xmin-horizon/results/results.md), PostgreSQL 17, Docker/laptop, 4 producers + 4 consumers, capped ~800 events/s | Baseline SKIP LOCKED / PgQue: 797 / 792 events/s; held repeatable-read snapshot: 517 / 804; ~91,593 / 0 dead event tuples | Author-run, short, capped-rate comparison; evidence about vacuum sensitivity, not maximum capacity or Eventuate performance. PgQue retained larger tables in this setup |
| [PgQue tick-rate](https://github.com/NikolayS/PgQue/blob/main/benchmark/tick-rate/README.md), PostgreSQL 16 + pg_cron 1.6.2, laptop, 100 events/s, 30-second cells | 100 ms tick: p50 52.62 / p99 103.48 ms; 10 ms: 8.05 / 863.50 ms; 1 ms: 3.26 / 460.47 ms | Faster ticks improved median but worsened tail in that scheduler/setup; suspected cron scheduling effect is not a universal bound for an external Go ticker |
| [Trigger overhead](https://infinitelambda.com/postgresql-triggers/), PostgreSQL 15.12, t2.micro, 1M transactions, 10 clients / 4 threads | No trigger 3,566 TPS / 2.804 ms; JSON insert trigger 3,471 TPS / 2.881 ms | About 2.7% latency overhead in one insert workload, not a full OLD/NEW CDC pipeline guarantee |
| [Transition tables](https://www.tigerdata.com/blog/speed-up-triggers-by-7x-with-transition-tables), Timescale 2.18, 10M rows, COPY batches of 10k | Row trigger 370 s; statement transition trigger 53 s; no trigger 42 s | Bulk metadata-aggregation result; not a 7× promise for ordinary single-row transactions or client-side batches of separate statements |
| [LISTEN/NOTIFY batching](https://www.dbos.dev/blog/postgres-listen-notify-scalability), author benchmark | Per-write notifications ~2.9k writes/s; batched notification design ~60k with 15–100 ms latency | Durable table plus batched hints/fallback, not a drop-in per-row trigger replacement or a durability mechanism |
| [Outbox partition experiment](https://dev.to/msdousti/postgresql-outbox-pattern-revamped-part-1-3lai), PostgreSQL 17.5 / Mac M3 | Partial-index selection degraded from ~0.133 ms to a reported worst 18.5 s; partitioned design ~1–3 ms | UNLOGGED, ID-only/index-visibility experiment; not production durability or end-to-end throughput. Moving rows between partitions still writes tuples |
| [RudderStack queue](https://www.rudderstack.com/blog/scaling-postgres-queue/) | Industry account of scaling to 100k events/s using bounded datasets, append-only status, COPY, compaction, and completed-table cleanup | Production experience with a substantial platform, not a controlled head-to-head CDC benchmark |

Supporting operational discussions:
[Keeping a Postgres queue healthy](https://planetscale.com/blog/keeping-a-postgres-queue-healthy)
and [Postgres job queues and long-running transactions](https://brandur.org/postgres-queues).
They motivate mixed-workload and long-transaction tests; they do not establish
that every UPDATE-based queue is unusable.

The reviewed [2024 custom CDC study](https://zenodo.org/records/14127166) is a
SQL Server/ETL-oriented overview, not a controlled PostgreSQL relay comparison.
No universal independent ranking of the fastest non-WAL CDC algorithm was found
in the reviewed material. Use the [local benchmark program](benchmarks.md).

## PostgreSQL semantics to verify in implementation

- [Snapshot/transaction functions](https://www.postgresql.org/docs/18/functions-info.html):
  top-level versus subtransaction IDs matter; visibility helpers are not a safe
  basis for treating arbitrary 32-bit `xmin` values as a permanent cursor.
- [CREATE TRIGGER](https://www.postgresql.org/docs/18/sql-createtrigger.html):
  transition-table restrictions and operation-specific triggers; pair OLD/NEW
  safely when primary keys can change.
- [NOTIFY](https://www.postgresql.org/docs/18/sql-notify.html) and
  [LISTEN](https://www.postgresql.org/docs/18/sql-listen.html): commit delivery,
  notification folding, and initial subscription race; always recover from the
  durable source rather than trusting hints as a log.
- [Notification wake-up optimization](https://github.com/postgres/postgres/commit/282b1cde9dedf456ecf02eb27caf086023a7bb71):
  an upstream change is version-specific and does not prove all notification
  contention disappears on a managed provider's installed release.

These documentation links describe PostgreSQL 18; they do not set RowRelay's
minimum supported version. Verify the exact installed version and APIs in the
restricted-role lab before writing the production installer.
