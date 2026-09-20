# Contributing

Start with the [implementation plan](tasks/plan.md) and
[task status](tasks/todo.md). The first implementation milestone is a local,
single-source delivery slice with an independent correctness oracle.

## Development

```sh
make fmt
make check
```

The experimental CLI installs capture explicitly and runs a single-source JSON
publisher. The [local lab](local/README.md) provides real PostgreSQL/Kafka tests
and resource measurement. Protobuf and production image/release packaging remain
future milestones.

Add dependencies when a working slice needs them; commit `go.sum` when it is
generated. Keep Go 1.27 as the minimum unless a concrete dependency requires a
change. Update `go.mod`, CI, and the README together when changing that minimum.

Use small changes with observable outcomes. A delivery change needs a failure
or replay test, not just mocks asserting the same sequence of method calls.
Use disposable PostgreSQL/Kafka/Registry instances for integration work. Never
point a benchmark or a failure injector at a shared database or broker.

## Before proposing a change

- Run quick checks and the affected integration cases from
  [the test matrix](docs/testing.md), once those runners exist.
- State which guarantees and failure windows the change affects.
- Update the relevant contract and `tasks/todo.md`.
- Attach reproducible evidence for performance claims using
  [the benchmark format](docs/benchmarks.md).
- Preserve attribution and license notices when reusing third-party SQL/code.

CI currently runs quick Go checks. Run `make integration` against the disposable
lab for delivery changes; the command uses the race detector. Remote integration,
JVM wire compatibility, and sustained benchmarks need separate jobs. A passing
quick workflow is not a CDC integration result.
