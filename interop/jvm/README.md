# Prepared Protobuf / Apicurio interoperability

**Verified local fixture:** an official JVM serializer prepares typed messages;
the business transaction stores those bytes in the PgQue-backed outbox; RowRelay
publishes them; an actual JVM Kafka consumer deserializes with the official
Apicurio Protobuf deserializer and `read_committed`.

```sh
make fetch-pgque lab-up interop-up interop-build
make integration-interop
```

Requires Maven and JDK 17+ in addition to the ordinary lab prerequisites. The
fixture's Maven plugins, serializer, Kafka client and Protobuf versions are
pinned in `pom.xml`. Registry 3.3.3 is pinned by digest in
[`local/interop.compose.yml`](../../local/interop.compose.yml), binds only to
loopback port 28080 and uses disposable in-memory storage. `make lab-down` stops
all owned lab services. Never use this Registry configuration for durable schemas.

## Assertions

Both Registry-ID placements are exercised: payload framing and Kafka headers.
The three typed operations are INSERT, UPDATE and DELETE. Checks include:

- Exact `int64` 9007199254740993 and arbitrary-precision decimal text.
- OLD/NEW presence, changed old association and optional-owner absence for NULL.
- Unicode, binary key/value, repeated headers, null and non-null empty headers.
- An independent Go reader compares the prepared key/value/header bytes exactly.
- A fresh JVM with an unreachable Registry must fail deserialization rather than
  silently apply a record. A separate fresh JVM with the real Registry succeeds.

Messages are synthetic. The local test dynamically registers fixture artifacts
in group `rowrelay-lab`, using the disposable topic as artifact ID. Generation and
schema lookup happen in the official JVM stack; the Go outbox path forwards the
already-prepared bytes without re-encoding or per-message Registry requests.

This proves prepared-outbox compatibility. Go encoding of typed CDC rows,
general schema evolution, imported-schema graphs and a controlled JSON/Protobuf
performance comparison remain separate work. The `.proto` here is a test fixture,
not a frozen public RowRelay CDC schema.
