#!/usr/bin/env python3
"""Sequential, disposable local screening. Raw artifacts remain gitignored."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import shutil
import subprocess
import tarfile
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--steady-seconds", type=int, default=60)
    parser.add_argument("--warmup-seconds", type=int, default=10)
    parser.add_argument("--held-seconds", type=int, default=300)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--poll-ms", type=int, default=50)
    parser.add_argument("--outbox", action="store_true", help="prepared binary business-outbox instead of CDC")
    parser.add_argument("--pgboss", action="store_true", help="PgQue/pg-boss outbox comparison, common Node ingress and no compression")
    parser.add_argument("--soak-seconds", type=int, default=0, help="single PgQue duration run at 500 events/s, with rotation/storage diagnostics")
    parser.add_argument("--case", action="append", dest="selected_cases", help="run only this named matrix case (repeatable)")
    args = parser.parse_args()
    if args.pgboss:
        args.outbox = True
    if min(args.steady_seconds, args.held_seconds, args.repetitions, args.poll_ms) < 1 or args.warmup_seconds < 0:
        parser.error("durations and repetitions must be positive")
    if args.soak_seconds < 0 or (args.soak_seconds and args.pgboss):
        parser.error("soak duration must be nonnegative and is a standalone PgQue run")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    for binary in ("bin/row-relay", "bin/row-bench"):
        if not Path(binary).is_file():
            parser.error("run make bench-build first")

    def command(*argv):
        return subprocess.check_output(argv, text=True).strip()

    candidates = ["pgque", "pgboss"] if args.pgboss else ["pgque"]
    node_files = []
    if args.pgboss:
        frozen = output / "node-baseline"
        frozen.mkdir()
        node_files = sorted(p for p in Path("benchmarks/pgboss").iterdir() if p.suffix in (".mjs", ".json", ".txt"))
        for path in node_files:
            shutil.copy2(path, frozen / path.name)
        subprocess.run(["npm", "ci", "--prefix", str(frozen), "--ignore-scripts", "--no-audit", "--no-fund"], check=True)
        node_image = (frozen / "node-image.txt").read_text().strip()
        command("docker", "image", "inspect", node_image)  # Fail before measurement if not prepared.

    rng = random.Random(42)
    cases = []
    for repetition in range(args.repetitions):
        modes = candidates.copy()
        rng.shuffle(modes)
        for mode in modes:
            cases.append(dict(name=f"steady-{repetition}-{mode}", mode=mode, rate=1000,
                              duration=args.steady_seconds, warmup=args.warmup_seconds, held=False))
    if not args.pgboss:  # Legacy control lacks the common durable-fact/Node ingress cost.
        cases.append(dict(name="control", mode="control", rate=1000, duration=60, warmup=10, held=False))
    for mode in candidates:
        cases.append(dict(name=f"idle-{mode}", mode=mode, rate=0, duration=30, warmup=5, held=False))
        cases.append(dict(name=f"load5000-{mode}", mode=mode, rate=5000, duration=60, warmup=10, held=False))
        cases.append(dict(name=f"held-{mode}", mode=mode, rate=1000, duration=args.held_seconds, warmup=10, held=True))
        if args.outbox:
            cases.append(dict(name=f"backlog100000-{mode}", mode=mode, rate=0, duration=60, warmup=0, held=False, backlog=100000))
    if args.soak_seconds:
        cases = [dict(name="soak-pgque", mode="pgque", rate=500,
                      duration=args.soak_seconds, warmup=args.warmup_seconds, held=False, diagnostics=True)]
    if args.selected_cases:
        unknown = set(args.selected_cases) - {case["name"] for case in cases}
        if unknown:
            parser.error(f"unknown cases: {sorted(unknown)}")
        cases = [case for case in cases if case["name"] in args.selected_cases]

    source_files = [Path("go.mod"), Path("go.sum"), Path("local/compose.yml"), Path("Makefile"),
                    Path("benchmarks/run.py"), Path("benchmarks/summarize.py"), *node_files]
    for directory in ("cmd", "internal"):
        source_files.extend(p for p in Path(directory).rglob("*") if p.suffix in (".go", ".sql"))
    checksums = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(source_files)}
    with tarfile.open(output / "source.tar.gz", "w:gz") as archive:
        for path in source_files:
            archive.add(path, arcname=str(path))
    manifest = dict(
        started_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        platform=dict(system=platform.system(), release=platform.release(), machine=platform.machine(),
                      logical_cpus=os.cpu_count(),
                      cpu_model=next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                                      if line.startswith("model name")), "unknown")),
        meminfo=Path("/proc/meminfo").read_text(),
        loadavg=Path("/proc/loadavg").read_text(),
        go=command("go", "version"), source_sha256=checksums,
        binaries_sha256={name: hashlib.sha256(Path(name).read_bytes()).hexdigest()
                         for name in ("bin/row-relay", "bin/row-bench")},
        images=[json.loads(line) for line in command(
            "docker", "image", "inspect", *command(
                "docker", "inspect", "rowrelay-lab-postgres-1", "rowrelay-lab-kafka-1",
                "--format", "{{.Image}}").splitlines(), "busybox:1.36", *([node_image] if args.pgboss else []),
            "--format", "{{json .RepoDigests}}").splitlines()],
        pgque_commit=command("git", "-C", ".slim/clonedeps/NikolayS__PgQue", "rev-parse", "HEAD"),
        cases=cases, poll_ms=args.poll_ms, business_outbox=args.outbox,
        pgboss_baseline=args.pgboss, compression="none" if args.pgboss else "zstd",
        delivery="fenced Kafka transactions; read_committed consumers",
        experiment="duration-soak" if args.soak_seconds else "screening",
        source_archive_sha256=hashlib.sha256((output / "source.tar.gz").read_bytes()).hexdigest(),
        selection_policy={
            "scope": ("local wire-byte outbox" if args.outbox else "local JSON CDC relay")
                     + "; one source, one partition, one active process",
            "correctness": "zero missing/corrupt/phantom events and complete source ACK in every run",
            "resource_ceiling": "relay: 128 MiB hard limit, 96 MiB Go soft limit, one CPU",
            "screening_latency_limit_ms": {"steady_p99": 250, "held_p99": 1000},
            "selection": "prefer lower worst-case source CPU when both satisfy latency/resource gates; report healthy-path tradeoffs",
            "limitations": "single Kafka broker RF=1; screening is not a two-hour production soak or an HA proof",
        },
    )
    if args.soak_seconds:
        manifest["selection_policy"]["limitations"] = "local single-broker RF=1 duration soak; not managed-environment or multi-broker HA acceptance"
        manifest["selection_policy"]["soak_acceptance"] = "zero delivery errors and complete source ACK; no OOM/worker exit; observe actual rotation and queue storage"
    if args.pgboss:
        manifest["node"] = command("node", "--version")
        manifest["node_dependencies"] = json.loads(command("npm", "ls", "--prefix", str(frozen), "--all", "--json"))
        manifest["selection_policy"]["resource_ceiling"] = "relay: 128 MiB hard limit, one CPU; Go soft limit / Node old-space 96 MiB"
        manifest["baseline"] = "official pg-boss fetch/complete, explicit polling; immutable facts and common Node ingress for all candidates"
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for name, digest in manifest["binaries_sha256"].items():
        target = output / Path(name).name
        shutil.copy2(name, target)
        if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
            raise SystemExit(f"Binary changed while freezing experiment: {name}")
    env = {**os.environ, "GOMAXPROCS": "2"}
    for case in cases:
        for name, digest in manifest["binaries_sha256"].items():
            if hashlib.sha256((output / Path(name).name).read_bytes()).hexdigest() != digest:
                raise SystemExit(f"Binary changed during experiment: {name}")
        print(f"START {case['name']}", flush=True)
        argv = [str((output / "row-bench").resolve()), "--relay-binary", str((output / "row-relay").resolve()),
                "--mode", case["mode"], "--rate", str(case["rate"]),
                "--duration", f"{case['duration']}s", "--warmup", f"{case['warmup']}s",
                "--poll", f"{args.poll_ms}ms",
                "--output", str(output / f"{case['name']}.json")]
        if case["held"]:
            argv.append("--hold-xmin")
        if case.get("diagnostics"):
            argv.append("--diagnostics")
        if args.outbox:
            argv.append("--outbox")
        if args.pgboss:
            argv.extend(["--node-ingress", "--node-baseline-dir", str(frozen), "--compression", "none"])
        if case.get("backlog"):
            argv.extend(["--backlog", str(case["backlog"])])
        with (output / f"{case['name']}.log").open("w") as log:
            result = subprocess.run(argv, env=env, stdout=log, stderr=subprocess.STDOUT)
        if result.returncode:
            raise SystemExit(f"FAILED {case['name']}; retained log: {output / (case['name'] + '.log')}")
        data = json.loads((output / f"{case['name']}.json").read_text())
        if any(data[key] for key in ("missing", "corrupt", "phantoms")) or not data["source_ack_complete"]:
            raise SystemExit(f"FAILED oracle: {case['name']}")
        print(f"PASS {case['name']}: {data['committed']} committed; latency={data['latency_tx_start_to_decode_ms']}", flush=True)
    subprocess.run(["python3", "benchmarks/summarize.py", str(output)], check=True)


if __name__ == "__main__":
    main()
