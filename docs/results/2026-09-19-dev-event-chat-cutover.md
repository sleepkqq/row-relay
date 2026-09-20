# Dev event-chat cutover — 2026-09-19

The two business outbox directions now use RowRelay on the existing managed dev
PostgreSQL/Kafka services. The pg-boss worker Deployment, ConfigMap and NetworkPolicy
were removed after reconciliation. No database roles/users, Kafka ACLs, production
resources, Git commits or Git pushes were added.

## Artifacts

This public summary preserves measurements and outcomes. Deployment identifiers,
application image digests and record IDs remain in the private acceptance archive.
Applications used pinned, tested JVM artifacts. Native verification was prepared
in CI but not run.

PgQue is unchanged upstream 0.2.0. Both app migrations add an AFTER INSERT trigger
that calls `rowrelay_outbox.enqueue` in the fact's transaction, preserving its UUID,
aggregate key and prepared bytes. Handwritten pg-boss inserts and the old consumer Node
relay/publishing workflow were removed in those worktrees.

## Dev configuration corrections

- Both event-chat feature flags were initially false and business fact queues empty.
  Flags are now enabled; startup backfill remains false. Existing events were not
  mass-provisioned.
- The consumer's dev profile lacked Kafka/SASL/Registry settings. It now follows the producer's
  existing dev settings, using the existing secrets.
- The existing principal permits the selected topic-specific groups.
  The old application-default group names failed
  with `GroupAuthorizationException`. No ACL changes were needed.
- Apicurio lookup requires the exact JVM SDK canonical Protobuf string, including
  its final newline. Both message descriptors produce the same 822 bytes, SHA-256
  `fb76fb07c8c3b0f5d0f9e70fd116381b1a7d281e89aa9f2ce275831271ae9dbe`.
  Compatibility checks passed; exact lookup returns ID 11 / version 3 on both
  subjects. Raw ID 9 and the unsuccessful trimmed canonical ID 10 remain intact.
- Applications use zero-surge rollout because the single dev worker had insufficient
  CPU requests for an additional app pod. RowRelay keeps two replicas, managed replay
  takeover, original runtime credentials, and uncompressed Kafka records matching
  the legacy producer's compression choice.

## Live acceptance

A real media upload and producer HTTP create produced a DEMO event. Its source fact
was published by RowRelay, the consumer committed its state/chat/member/system message/ACK
fact, and the producer applied the returned acknowledgement.

The active publisher pod was deleted. Both original source
and ACK facts were re-enqueued unchanged, then another event was created through
the producer HTTP API. Final ownership was held by the surviving publisher (three advisory
ownership locks). The rollout returned to two Ready RowRelay replicas.

| DEMO event | Chats | Applied revision | Messages | Members |
| --- | --- | --- | --- | --- |
| Before takeover | 1 | 1 | 1 | 1 |
| After takeover | 1 | 1 | 1 | 1 |

Final reconciliation:

- Two source facts, two ACK facts, matching delivery IDs/chat IDs on both sides.
- Both Kafka topics: partition 1 high offset **3**, group committed offset **3**;
  partitions 0/2 empty (high 0, no committed offset). Thus all six records, including
  the two deliberate replays, were consumed without repeated business effects.
- Zero legacy event-chat jobs in either schema; closed source-ACK observation on all
  PgQue subscriptions. Tickers can transiently open another empty batch.
- After retirement, the legacy workload/config/policy were confirmed absent;
  Both applications each had one Ready replica and HTTP readiness 200/UP, and RowRelay
  had two Ready replicas at the pinned images.
- Database facts, deduplication receipts, legacy tables, schema versions and old
  immutable images are retained. The two explicitly named DEMO verification events
  remain as acceptance fixtures.

## Checks and scope

Producer: **578 tests**, consumer: **76 tests**, zero failures/errors/skips; `ktlintCheck`
and `build` passed. The consumer's full check was repeated after its dev configuration fix.
The producer's full suite required a 1536 MiB test JVM heap; the first default-heap run
ran out of heap late in the suite. Gradle itself stayed capped at 2 GiB / one worker.

The real-consumer integration test additionally covers a deferred COMMIT failure
rolling back domain/receipt/downstream outbox writes without Kafka ACK, same-ID
conflicting content, restart, source rollback, unavailable broker/Registry, delayed
ACK, canonical bootstrap and replay. It installs no pg-boss tables.

Operational procedures, pinned application patches, exact acceptance evidence and
pre-cutover manifests/install logs are retained in the private deployment archive.
Rollback is documented but was not executed against dev.

This accepts the dev business-outbox replacement. It does not certify production,
node/zone HA, native execution, distributed ORM-cache adapters or throughput scaling.
The managed broker remains single-broker/RF1; the provider-catalog route remains inactive.
