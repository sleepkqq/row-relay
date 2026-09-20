# Correctness and fault-injection plan

Status: **target matrix with a working integration subset**. `make integration`
runs real PostgreSQL/Kafka cases; [`local/README.md`](../local/README.md) describes
the lab. Correctness gates come before performance conclusions.

Current coverage includes full-image/NULL capture, rollback/savepoints, low-ID
late commits, PK moves/deletes, source-ACK failure and stable replay, an oversized
snapshot batch, empty-batch progress, poison records, and concurrent-start
rejection. Oversized capture is rejected atomically, and broker rejection leaves
the source unacknowledged. The retired pending backend and its cleanup-specific
tests were removed; delivery and failure scenarios now exercise PgQue. This is
partial coverage of C01–C05/C07–C09/C19/C20, not completion of the whole matrix.
Real TCP response-loss tests now
cover committed source ACK and Kafka EndTxn; two-process tests fence a resumed
old owner, and partial broker acceptance remains invisible until transaction
commit. Real CLI SIGTERM/SIGKILL tests pause at source ACK and verify replay after
restart. See [ownership.md](ownership.md). Closed-boundary tests cover late
commits, a published prefix before poison, external-ticker rejection, failed
consumer application and an actual retention gap. Local read/fill fencing is
race-tested. Prepared Protobuf passes the real JVM/Apicurio fixture; multi-broker
failure, restore and full application-cache integration remain open.

## Independent oracle

Use synthetic mutations with independently assigned logical mutation IDs and
an expected ledger written in the business transaction. Keep that ledger
independent of the capture trigger being tested. After quiescence, reconcile
all committed expected mutations against decoded Kafka events and applied
consumer effects. Aborted/savepoint-rolled-back mutations must be absent.

Check complete row images and order-sensitive effects, not only record counts.
Track attempted writes, committed writes, captured rows, Kafka ACKs, unique
decoded IDs, duplicates, applied effects, and certified barriers separately.
Use a fresh source namespace for each test; preserve the same identities during
restart/replay within a test. Deterministic latches/fault hooks are preferable to
sleep-based timing assertions.

## Required cases

| ID | Scenario | Required observation | Requirements |
| --- | --- | --- | --- |
| C01 | INSERT/UPDATE/DELETE, external SQL writer | Complete, correct images for every committed mutation | R01–R04 |
| C02 | Full rollback and nested savepoint rollback | No phantom events; top-level transaction visibility is correct | R01, R06 |
| C03 | Low event ID held in a long transaction while higher IDs commit | Late commit still arrives; no cursor/checkpoint/cleanup skips it | R06, R08, R09, R17 |
| C04 | Repeated same-row updates, PK/FK moves, subtype changes | Correct dependent order and old/new association invalidation | R02, R08, R13 |
| C05 | Crash before send, during partial batch, after Kafka ACK before source ACK | Retry produces zero loss; duplicates are identifiable and safe | R06, R07 |
| C06 | Source ACK succeeds but its response is lost | Restart reconciles durable source state without skipping work | R06, R18 |
| C07 | Two instances, ownership loss, old process resumes or has delayed sends | Stale publisher cannot corrupt progress or mint a valid later barrier | R09, R11 |
| C08 | One event or encoding fails inside a batch | Frontier/barrier stays before the gap; subsequent success cannot hide poison | R09, R10 |
| C09 | Broker outage, slow ACKs, insufficient ISR, leader failover | Source unacknowledged until policy is met; bounded retries/RAM | R06, R12 |
| C10 | PostgreSQL disconnect/restart during selection or ACK | Reconnect/replay retains ownership and progress invariants | R05, R11, R18 |
| C11 | Idle source, stuck publisher, stale checkpoint, clock skew | Heartbeats do not manufacture consumer freshness | R09, R16 |
| C12 | Consumer apply failure or cache/Redis outage | No applied-progress advancement; cache reads fail closed until recovery | R16 |
| C13 | Kafka retention gap and consumer restart | Gap is detected; explicit rebuild/cold generation is required | R16, R18 |
| C14 | NULL/absent, numeric extremes, binary/Unicode/large values | Lossless Go-to-JVM round-trip or explicit unsupported-type failure | R02, R13, R14 |
| C15 | Schema change, missing imports, unknown ID, Registry outage | Known-schema policy holds; unknown data cannot be acknowledged away | R10, R14, R15 |
| C16 | Rotation while old transactions/consumers still reference a segment | Data retained; later catch-up completes without loss | R17 |
| C17 | Source backup restore, reseed, or transaction ID boundary | Epoch collision is rejected; reset/recovery is explicit | R07, R18 |
| C18 | SIGTERM at each batch phase and timeout followed by SIGKILL | Batch completes durably or remains retryable | R06, R18 |
| C19 | Trigger installation, privileges, partition/cascade/TRUNCATE paths | Supported paths captured; unsupported operations fail explicitly | R03–R05 |
| C20 | Event/batch size overflow, disk pressure, malicious identifiers/config | Bounded behavior, no silent truncation, no payload/secret leakage | R10, R12, R19 |

For snapshot candidates, run C02/C03/C16/C17 against their actual SQL, ticker,
batch selection, and maintenance implementation. A Go mock cannot prove MVCC or
rotation safety. For C17 use a controlled lab/simulation appropriate to the
transaction-ID API; do not force wraparound on a shared PostgreSQL instance.

## Check tiers

1. **Quick:** `make check`: `gofmt`, `go vet`, `go test -race`, build. Includes
   envelope precision/stable identity and poison error-sanitization tests.
2. **Integration:** `make integration`: disposable real PostgreSQL/Kafka; run per
   relevant implementation change. The local benchmark adds an independent
   expected-mutation/checksum ledger.
3. **Wire compatibility:** `make interop-up interop-build integration-interop`:
   pinned real JVM serializer/consumer and Registry, with byte-exact Protobuf
   assertions; run for protocol/dependency changes.
4. **Fault/soak:** process kills, network partitions, rotation, retained backlog,
   and long-duration runs; scheduled or explicitly invoked, with saved artifacts.

The implementation milestone must add its runnable command alongside its first
test. Pin infrastructure versions, seeds, and fault timings. A reproduction must
include the smallest failing schedule and sanitized logs, not just a timeout.

## Release gate

Zero missing committed mutations, zero phantoms, zero unexplained value changes,
and zero barrier/fencing violations. Duplicates are permitted by the contract
but must be measured and shown safe for the consumer. All applicable cases must
pass for each queue/protocol combination being advertised. Performance gains
never waive a failed correctness case.
