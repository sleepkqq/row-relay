#!/usr/bin/env python3
"""Summarize measured cgroup CPU and memory, without relabelling memory as RSS."""

import hashlib
import json
from pathlib import Path
import statistics
import sys


def main():
    root = Path(sys.argv[1])
    manifest = json.loads((root / "manifest.json").read_text())
    rows = []
    hashes = {}
    for case in manifest["cases"]:
        path = root / (case["name"] + ".json")
        if not path.exists():
            continue
        hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
        data = json.loads(path.read_text())
        row = {"case": case["name"], "mode": data["mode"], "rate": data["rate"],
               "business_outbox": data.get("business_outbox", False), "prepared_backlog": data.get("prepared_backlog", 0),
               "node_ingress": data.get("node_ingress", False),
               "correct": not any(data[k] for k in ("missing", "corrupt", "phantoms")) and data["source_ack_complete"],
               "committed": data["committed"], "duplicates": data["duplicates"],
               "producer_elapsed_s": data["producer_elapsed_s"],
               "measured_events_per_s": data["measured_committed"] / data["producer_elapsed_s"],
               "latency_ms": data["latency_tx_start_to_decode_ms"],
               "write_tx_ms": data["write_tx_ms"], "admission_delay_ms": data["admission_delay_ms"],
               "queue_mib": data["queue_bytes"] / 2**20,
                "dead_tuples_estimated": data["dead_tuples_estimated"], "recovery_s": data["recovery_s"]}
        if row["prepared_backlog"]:
            row["backlog_through_source_ack_s"] = data["producer_elapsed_s"] + data["recovery_s"]
            row["backlog_through_source_ack_events_per_s"] = row["prepared_backlog"] / row["backlog_through_source_ack_s"]
        for name, samples in data["samples"].items():
            if name == "rowrelay-lab-postgres-1":
                key = "postgres"
            elif name == "rowrelay-lab-kafka-1":
                key = "kafka"
            else:
                key = "relay"
            first, last = samples[0], samples[-1]
            cpu = (last["cpu_seconds"] - first["cpu_seconds"]) / (last["elapsed_s"] - first["elapsed_s"]) * 100
            row[key] = dict(cpu_pct_one_core=cpu, memory_peak_mib=max(s["memory_mib"] for s in samples),
                            anonymous_peak_mib=max(s["anonymous_mib"] for s in samples))
            rss = [s["process_rss_mib"] for s in samples if "process_rss_mib" in s]
            if rss:
                row[key]["process_rss_peak_mib"] = max(rss)
        row["relay_memory_peak_since_start_mib"] = data.get("relay_memory_peak_since_start_mib")
        rows.append(row)
    missing = [case["name"] for case in manifest["cases"] if case["name"] + ".json" not in hashes]
    summary = {"complete": not missing and all(row["correct"] for row in rows),
               "missing_cases": missing, "cases": rows, "results_sha256": hashes, "steady_medians": {}}
    for mode in sorted({row["mode"] for row in rows}):
        selected = [r for r in rows if r["mode"] == mode and r["case"].startswith("steady-")]
        if selected:
            summary["steady_medians"][mode] = {
                "runs": len(selected), "relay_cpu_pct": statistics.median(r["relay"]["cpu_pct_one_core"] for r in selected),
                "relay_memory_peak_mib": statistics.median(r["relay"]["memory_peak_mib"] for r in selected),
                "postgres_cpu_pct": statistics.median(r["postgres"]["cpu_pct_one_core"] for r in selected),
                "latency_p99_ms": statistics.median(r["latency_ms"]["p99"] for r in selected),
            }
    (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
