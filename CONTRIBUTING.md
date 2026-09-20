# Contributing

Thanks for helping improve RowRelay. Bug reports, documentation fixes, integration
feedback and reproducible correctness/performance cases are welcome.

Start with the [project overview](README.md), [documentation](docs/README.md) and
[task status](tasks/todo.md). The [implementation plan](tasks/plan.md) records the
remaining acceptance gates and their dependencies.

## Issues and proposals

Use the [issue templates](https://github.com/sleepkqq/row-relay/issues/new/choose)
for bugs and feature requests. Include the RowRelay version, delivery profile,
relevant PostgreSQL/Kafka versions and a minimal synthetic reproduction.
For a larger change, open an issue first so the scope and consumer contract can
be discussed before implementation.

Share redacted configuration and synthetic records rather than credentials,
private infrastructure identifiers or real row payloads.

## Development

```sh
make fmt
make check
```

Requires Go 1.27+ and Make. `make check` runs formatting checks, vet, race tests
and the build without external infrastructure. The [local lab](local/README.md)
provides real PostgreSQL/Kafka tests and resource measurement; integration work
also requires Docker Compose. Protobuf/Apicurio interoperability has a separate
[JVM fixture](interop/jvm/README.md).

Add dependencies when a working slice needs them; include corresponding `go.sum`
changes. Keep Go 1.27 as the minimum unless a concrete dependency requires a
change. Update `go.mod`, CI, and the README together when changing that minimum.

Use small changes with observable outcomes. A delivery change needs a failure
or replay test, not just mocks asserting the same sequence of method calls.
Use disposable PostgreSQL/Kafka/Registry instances for integration work. Never
point a benchmark or a failure injector at a shared database or broker.

## Before proposing a change

- Run `make check` and the affected integration cases from
  [the test matrix](docs/testing.md). State what ran and any environment limitations.
- State which guarantees and failure windows the change affects.
- Update the relevant contract and `tasks/todo.md`.
- Attach reproducible evidence for performance claims using
  [the benchmark format](docs/benchmarks.md).
- Preserve attribution and license notices when reusing third-party SQL/code.

Pull-request CI runs quick Go checks. The tag-triggered release workflow also
checks real delivery and the packaged container before publication. Run
`make integration` against the disposable lab for delivery changes; the command
uses the race detector. JVM wire compatibility and sustained benchmarks remain
separate checks. A passing quick workflow is not a CDC integration result.
