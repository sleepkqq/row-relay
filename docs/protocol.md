# Event and wire protocol

Status: **typed Protobuf CDC and prepared-outbox transport implemented**. CDC
defaults to `--cdc-format protobuf`. Set `SCHEMA_REGISTRY_URL` to Apicurio's
Confluent-compatible `/apis/ccompat/v7` endpoint. Every route's `<topic>-value`
subject must already contain the exact source schema; startup only looks it up,
never registers it or falls back to JSON. IDs are cached for that runner lifetime.

The public `cdc.CacheCdcRecord` schema lives in
[`internal/cdcwire/cache_cdc.proto`](../internal/cdcwire/cache_cdc.proto).
Its generated JVM classes use `io.github.sleepkqq.rowrelay.cdc`.
One schema serves all cache topics, including row changes
and closed boundaries. Framing is magic byte 0, positive four-byte big-endian
schema ID, the optimized message-index path `[0]`, then binary Protobuf. The first
top-level message must remain `CacheCdcRecord`. Prepared outboxes remain byte-exact.

The historical Eventuate-shaped JSON envelope requires explicit
`--cdc-format legacy-json`. Its benchmark fixtures and reference consumer retain
that choice; their old measurements are not Protobuf measurements.

## Logical envelope

| Field | Meaning |
| --- | --- |
| protocol version | Version of RowRelay envelope semantics, independent of Registry schema identity |
| source identity / epoch | Stable dataset identity; explicitly changes on incompatible restore/reset |
| event identity | Stable across delivery retries and process restarts; unique within its source epoch |
| schema and table | Unambiguous source relation identity with defined identifier handling |
| operation | INSERT, UPDATE, DELETE, or a distinct progress/control record |
| schema identity | Metadata needed to decode the exact row/envelope schema, including references |
| before / after | Full physical row images according to the operation |

The physical event ID can be a database sequence plus a durable epoch or another
equally testable identity scheme. Do not generate a new identity on each retry.
Do not regenerate the source epoch on an ordinary relay restart.

### Row image rules

| Operation | Before | After |
| --- | --- | --- |
| INSERT | absent | complete row |
| UPDATE | complete row | complete row |
| DELETE | complete row | absent |

“Complete” means every captured column, including SQL NULL values. Absence of a
column and presence with SQL NULL are different states. A row's PK must be valid;
an UPDATE may contain different old/new PK or FK values. Dropping old values
breaks association invalidation even when the current row can be re-read.

Progress records use a separate envelope alternative, never a fake table name
that could collide with business data. Carry the certified source boundary and
the applicable source/ownership epoch. Receivers must reject invalid/regressing
boundaries according to the specified replay policy.

## Typed representation

`RowImage` contains a map of `ColumnValue` oneofs: explicit null, boolean, string,
exact number text, object or ordered array. There is no opaque JSON row on the
wire. Number text follows JSON number grammar and never passes through a double;
integer range, decimal scale and exponent are preserved. UUIDs, timestamps, bytea
and PostgreSQL special numeric values retain the string representation produced
by PostgreSQL's capture conversion. Strings are not guessed to be other types.
An absent image differs from an empty image, and an absent column from SQL NULL.
Value nesting is limited to 30 levels to stay within the JVM Protobuf recursion
limit; unsupported depth stops delivery without acknowledging the source batch.

Regenerate the Go types with protoc and `protoc-gen-go v1.36.12`:

```sh
protoc --proto_path=internal/cdcwire --proto_path=/usr/include \
  --go_out=. --go_opt=module=github.com/sleepkqq/row-relay \
  internal/cdcwire/cache_cdc.proto
```

Update the public schema first, then regenerate. Applications should generate
their consumer types from the schema shipped with their relay version. Register
that exact source under each output subject before starting the relay.

Java package options affect generated class names and exact Registry schema lookup,
but do not change Protobuf field numbers or message framing. When changing those
options, register the updated source and update generated JVM imports together;
keep prior registered versions available for retained records.

## Apicurio and JVM interoperability gate

The [real JVM/Registry fixture](../interop/jvm/README.md) passes prepared-outbox
round trips for both payload and header schema IDs, typed row presence/precision,
byte-exact forwarding, and a cold consumer's Registry-failure path. It uses
Apicurio 3.3.3 and preserves the official serializer's framing without decoding
or re-encoding it in the relay.

Additional downstream acceptance used real Go, PostgreSQL, Kafka and Apicurio,
registered the raw source schema and deserialized Go-produced records with generated
JVM types. It covered three schema routes sharing one schema ID, insert/update/delete,
rollback, OLD/NEW/NULL, exact large numbers, bytea, progress and cold Registry
unavailability. Those application-specific tests are not part of this repository;
the public runnable fixture above verifies prepared-outbox interoperability.

`proto.Marshal` alone is not Apicurio-compatible Kafka serialization. Pin the
Registry and JVM serializer/deserializer versions and establish:

- Registry API, artifact/group/version naming, and schema registration ownership.
- Content/global ID choice, ID width, header-versus-payload placement, and any magic bytes.
- Protobuf message selection/indexes and transitive imported schema references.
- Kafka record key and value serialization, including control records.
- Compatibility policy and behavior for unknown or removed schema versions.
- Authentication/TLS, bounded lookup retries, cache capacity, and cache misses.

Use the real JVM consumer stack to decode Go-produced Kafka records. Maintain
golden bytes plus cross-language semantic assertions, including precision and
NULL/presence. Test the reverse direction where it validates a shared format.
Do not assume Confluent framing and Apicurio framing are interchangeable.

Registry lookups are cached; registration must not happen once per event. During
a Registry outage, known cached schemas may proceed under the defined policy;
unknown schemas block source progress. Retained Kafka records must remain
decodable throughout the documented replay window.

## Consumer and migration contract

Consumers validate before applying and acknowledge only after successful
application. Cache consumers need the full stream per cache generation, not an
ordinary competing-consumer group that distributes invalidations across caches.
Poison records and retention gaps must not become a silent “skip and ready.”

A Jimmer adapter must preserve old associations and subtype invalidation and
prove compatibility with its actual released dependency versions. It owns cache
coverage, cache generations, and DB bypass; RowRelay remains ORM-independent.

The Eventuate-compatible JSON baseline is a separate benchmark protocol. Changing
to Protobuf needs a versioned topic or explicit consumer cutover plan; changing
only shared Kafka client settings does not change the wire format. Migration
must account for old backlog, duplicate delivery, rollback, and freshness state.
