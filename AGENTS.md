# RowRelay — working rules

RowRelay is an independent Go/PostgreSQL/Kafka project. Start with `README.md`
and the relevant contract in `docs/`. Current implementation status lives in
`tasks/todo.md`; outcomes and dependencies live in `tasks/plan.md`.

The pinned PgQue SQL source for snapshot experiments lives in the ignored
`.slim/clonedeps/NikolayS__PgQue/` checkout. Use `make fetch-pgque` to verify it;
provenance is tracked in `.slim/clonedeps.json`. Do not edit the dependency to
make a benchmark pass. See `local/README.md` for integration and resource checks.

- Implement the smallest end-to-end slice. Prefer Go's standard library and
  existing dependencies; add packages and interfaces only for actual use.
- Use idiomatic Go, `gofmt`, explicit errors, and cancellation-aware I/O.
- Source acknowledgement must follow successful Kafka acknowledgement. Never
  discard an unacknowledged event to make lag or readiness look healthy.
- No ID-only commit watermark, unlogged event storage, per-write network calls
  inside triggers, or heartbeat-only freshness claims.
- Preserve full OLD/NEW images, SQL NULL/presence, exact numbers, and old keys.
- Snapshot queues, ticker tuning, and Protobuf gains are hypotheses until
  correctness tests and controlled benchmarks substantiate them.
- Add focused behavioral tests for delivery, ordering, encoding, and recovery
  changes. Keep expensive integration/soak runs separate from quick checks.
- Run `make check`; report checks actually run and infrastructure limitations.
  Keep CI and documented commands aligned as the project grows.
- Use `ubuntu-latest` for GitHub-hosted CI and official stable numeric release
  tags for Actions (for example, `@v7.0.1`), rather than commit-SHA references.
- Keep published evidence separate from local results. Record workload,
  versions, durability, compression, raw data, and failures with every result.
- Never commit secrets, real row payloads, private infrastructure identifiers,
  or raw benchmark artifacts. Use synthetic fixtures and redacted diagnostics.
- Do not commit, push, publish, deploy, or change shared infrastructure without
  an explicit request. Preserve unrelated work.
- Update contracts and task status with implementation. Do not mark a planned
  feature implemented because its documentation or placeholder exists.

Before changing or publishing this repository, read `.local-context.md` when present; it is private local context and must never be staged.
