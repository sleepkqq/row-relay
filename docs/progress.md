# Closed CDC progress and reference consumer

Status: **opt-in**. The fenced reference consumer and managed invalidation adapter
use distinct control records and acceptance boundaries. Focused race/unit and real
broker-backed failure checks pass; application integration is verified separately.

## Fenced certificate

With `--progress-interval 1s`, the default Protobuf publisher emits a
`CacheCdcRecord.boundary` with `mode=MODE_FENCED`, a source epoch and the closed
`committed_before` timestamp. Its Kafka key is the source epoch. All subjects
must be registered beforehand and `SCHEMA_REGISTRY_URL` configured.

The explicit `--cdc-format legacy-json` reference/benchmark path emits:

```json
{"rowrelay_progress":{"version":1,"source_epoch":"<source UUID>","committed_before":"2026-09-19T12:00:00Z"}}
```

It certifies that every captured commit visible before `committed_before` has
been published ahead of this record. It does **not** include transactions merely
started before that time: a long transaction can commit later and its low-ID
events must still be delivered. Event IDs are never a commit watermark.

The boundary is the completed PgQue batch's `batch_end`. The ordinary ticker
takes its timestamp before its visibility snapshot. RowRelay obtains that
metadata, delivers the **entire** snapshot in bounded Kafka transactions, and
successfully acknowledges the source batch before publishing the certificate
in another Kafka transaction. Any poison event, unconfirmed Kafka commit or
failed source ACK stops that runner before a certificate can pass the failure.

External-ticker queues are rejected for this protocol. Publishing also requires
the original PostgreSQL ownership lock and a primary database. Transaction
fencing and `read_committed` are mandatory; see [ownership](ownership.md).

## Opt-in and compatibility

- Default interval is `0`: no control records are inserted. CDC wire format is
  independently selected; old JSON topics require explicit `legacy-json`.
- The CLI flag is `--progress-interval`; a stream entry may set
  `"progress_interval": "1s"`.
- Only CDC supports these records. Business outboxes continue to forward their
  original key, value and headers without inserting control messages.
- Use a versioned topic and compatible consumers when enabling this feature.
  A data-only Eventuate/Jimmer reader cannot be assumed to accept it.
- `/ready` and `/status` are operational worker probes. They continue to report
  `freshness_certified: false`: producer uptime cannot prove consumer application.

## Managed invalidation-only profile

For an adapter that **only invalidates caches**, explicitly select
`--delivery-mode managed --managed-invalidation --progress-interval 1s`. The typed
boundary uses `mode=MODE_MANAGED_INVALIDATION`, which the application adapter
requires. Only the explicit legacy wire mode uses this JSON control shape:

```json
{"rowrelay_invalidation_progress":{"version":1,"source_epoch":"<source UUID>","committed_before":"2026-09-19T12:00:00Z"}}
```

All Kafka records in the closed PgQue snapshot must be acknowledged before the
original owner's source ACK and subsequent control record. Source ownership,
primary-database checks and ordinary-ticker validation remain mandatory. This
profile enables ownership expiry and ready standbys, but **not broker fencing**.
An old publisher can deliver late duplicate invalidations or an already-validated
older boundary. Neither may restore row values or extend a boundary's original
source-time lease. The fenced reference consumer rejects this control kind.

Databases shared by multiple application schemas use one CDC source with explicit
routes, rather than multiple competing subscriptions on the same source:

```json
{"name":"cache-cdc","database_env":"DATABASE_URL","delivery_mode":"managed","managed_invalidation":true,"progress_interval":"1s","schema_topics":{"accounts":"accounts.cache-cdc","media":"media.cache-cdc"}}
```

Every route must have a distinct, pre-created one-partition topic. An unrouted
schema fails the whole source batch without acknowledging it. Control fanout
happens only after **all** routed records and the source ACK succeed, including
for routes that had no data in this batch.

A conforming adapter validates the expected epoch/schema/table and complete row
images, invalidates both old and new dependants, starts with a cold namespace and
replays retained history. It bypasses reads and fills during apply and advances
freshness only after an entire fetched batch succeeds. Older/repeated boundaries
are ignored, not used to renew the lease. Source-clock skew is subtracted from
the monotonic local expiry; excessive future timestamps, invalidation failures,
poison and retention gaps terminate that generation. No consumer-group commits
are required by the invalidation protocol. This contract must not be used for
row materialization or business effects.

## Fenced reference application boundary

`internal/relay.ProgressTracker` and its `Follow` method demonstrate the explicit
legacy-JSON, bounded, single-partition consumer. The caller supplies its expected source epoch, exact
schema/table allowlist, maximum lag, clock-skew bound and an idempotent apply
callback. Row images remain raw JSON: integer/decimal precision, SQL NULL and
full old associations are preserved for the adapter.

The lifecycle is deliberately cold:

1. Every tracker creates a unique `Namespace()`; all cache entries must use it.
2. Obtain a `BeginCacheRead()` token before the cache lookup or opening a database
   read snapshot. If unavailable, bypass cache reads **and fills**.
3. The reader starts at retained history, with `read_committed` and **no automatic
   offset reset** after a retention gap. It does not share a consumer group with
   another independent cache instance.
4. The callback must finish invalidating both old and new dependent state before
   the applied offset advances. It must tolerate identical source-event replay;
   deleting the same cache entry twice is a valid idempotent operation.
5. A certificate can activate the namespace only when its boundary is later than
   tracker startup plus the declared clock-skew bound. Old replayed certificates
   cannot activate a newly created cache.
6. Expiry is derived from the **source boundary**, subtracting clock skew. A
   repeated certificate cannot renew its own lease. The local deadline retains a
   monotonic clock, and readiness expires when publishing or consumption stalls.
7. Readiness is false during apply. Apply failure, bad input, wrong epoch,
   unsupported control version, unknown table, decreasing boundary, fetch error,
   or shutdown is terminal. Recovery creates another cold namespace.

An adapter must additionally validate its supported row schema. The reference
tracker supplies a local fill fence, but not durable business-effect
deduplication. `Ready()` means that the
declared apply callback completed through a sufficiently recent closed boundary;
it is a cache-safety gate only when the adapter meets the requirements above.
Clocks must stay within the configured skew bound; a future boundary beyond that
bound fails closed. Namespace/callback state is never silently restored from an
old process, database clone or old source epoch.

### Local cache read and fill fencing

- After a cache lookup, call `CacheReadValid(token)` before using the result. A
  concurrent invalidation, expiration or terminal failure rejects it.
- On a cache miss, start a **fresh** database read after obtaining the token.
  Publish the returned value only through `StoreCache(token, store)`; a false
  result means skip caching. Do not use a transaction/snapshot opened before the
  token was obtained.
- Tokens are tied to their tracker/namespace and invalidation revision. Every
  data event advances the revision **before** its apply callback. A later
  certificate cannot resurrect an older token.
- `StoreCache` holds the tracker mutex for its short local cache mutation, making
  storage and the start of invalidation mutually exclusive. The callback must
  not perform network I/O or call the tracker again. Do not enter with a cache
  lock already held. Acquire cache locks inside the store/invalidation callbacks;
  invalidation callbacks run outside the tracker mutex.
- The fence is stream-wide: unrelated events can reject a fill. This is a
  deliberate conservative bound. Distributed Redis fills need their own atomic
  version check in the cache backend; wrapping a remote request in this callback
  is not a supported distributed-cache protocol.

## Checks

```sh
go test -race ./internal/relay -run 'TestColdProgress|TestConsumerFailures|TestCacheFill'
make lab-up build
go test -race -tags=integration ./internal/relay \
  -run 'TestClosedProgress|TestReferenceConsumer|TestManagedInvalidation' -count=1 -timeout=3m
```

Unit cases cover cold replay, exact large numbers and old-FK invalidation,
idempotent replay, concurrent readiness during apply, terminal failures,
non-renewing expiry, stale fills after invalidation, cross-generation tokens and
store/invalidation ordering. Broker cases exercise a late low-ID commit across a closed
snapshot, multi-chunk publication, an apply failure before a later certificate,
and a real Kafka log-start advance overtaking a blocked consumer.

Managed cases additionally cover multi-schema complete-prefix routing, an idle
route, unmapped-schema poisoning, a held low-ID transaction and two real processes:
late old-publisher data, and an old boundary serialized before ownership loss but
published after the successor's newer boundary.
