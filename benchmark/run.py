"""Phase 13: measure replication against erasure coding, and write a CSV.

Four numbers decide which storage mode is worth using:

  write           what it costs to get an object in
  read            what a healthy read costs
  degraded read   what a read costs once shards are missing and the data has
                  to be solved for rather than simply concatenated
  overhead        how many bytes the cluster actually stores per byte stored

Replication wins the read columns and loses the overhead column; erasure coding
does the reverse. The point of the benchmark is to put figures on that trade
rather than assert it.

Every measured read is checked against the bytes that were written, because a
benchmark that is quietly timing corrupted reads measures nothing.

Usage, from the repo root with the cluster up:

    python benchmark/run.py
    python benchmark/run.py --sizes 64KB,256KB,1MB --reps 7
    python benchmark/run.py --out benchmark/results.csv
"""

import argparse
import csv
import hashlib
import os
import random
import statistics
import sys
import time

import requests

COORDINATOR = os.environ.get("LATTICE_COORDINATOR", "http://localhost:9700")
DEFAULT_OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.csv")

# Matches common/erasure.py. Imported rather than hard-coded where the
# benchmark can see the package, so the two can't drift apart.
try:
    from common.erasure import DATA_SHARDS, PARITY_SHARDS, TOTAL_SHARDS
except ImportError:  # running from outside the repo root
    DATA_SHARDS, PARITY_SHARDS, TOTAL_SHARDS = 4, 2, 6

REPLICATION_FACTOR = 3
SEED = 20260920

CSV_COLUMNS = [
    "mode",
    "size_bytes",
    "operation",
    "rep",
    "seconds",
    "throughput_mb_s",
    "nodes_touched",
    "bytes_stored",
    "overhead_ratio",
    "verified",
]


# ------------------------------------------------------------------- helpers


def parse_size(text):
    """Accept 4096, 64KB, 1MB, 1.5MB."""
    text = text.strip().upper()
    for suffix, factor in (("KB", 1024), ("MB", 1024 * 1024), ("B", 1)):
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * factor)
    return int(text)


def human_size(size):
    if size >= 1024 * 1024:
        return f"{size / (1024 * 1024):g}MB"
    if size >= 1024:
        return f"{size / 1024:g}KB"
    return f"{size}B"


def payload_of(size, tag):
    """Deterministic bytes, so runs are comparable across machines."""
    generator = random.Random(f"{SEED}:{tag}:{size}")
    return bytes(generator.getrandbits(8) for _ in range(size))


def throughput(size, seconds):
    if seconds <= 0:
        return 0.0
    return (size / (1024 * 1024)) / seconds


class Coordinator:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def health(self):
        response = self.session.get(f"{self.base_url}/health", timeout=30)
        response.raise_for_status()
        return response.json()

    def put(self, name, payload, mode):
        started = time.perf_counter()
        response = self.session.put(
            f"{self.base_url}/objects/{name}",
            files={"file": (name, payload)},
            headers={"X-Storage-Mode": mode},
            timeout=300,
        )
        elapsed = time.perf_counter() - started
        response.raise_for_status()
        return elapsed, response.json()

    def get(self, name, simulate_missing=0):
        params = {"simulate_missing": simulate_missing} if simulate_missing else {}
        started = time.perf_counter()
        response = self.session.get(
            f"{self.base_url}/objects/{name}", params=params, timeout=300
        )
        elapsed = time.perf_counter() - started
        response.raise_for_status()
        return elapsed, response

    def placement(self, name):
        response = self.session.get(f"{self.base_url}/objects/{name}/placement", timeout=30)
        response.raise_for_status()
        return response.json()


# ---------------------------------------------------------------- the measure


def stored_bytes(mode, size, place):
    """How many bytes the cluster is really holding for this object.

    Taken from the object's actual placement rather than from the nominal
    factor, so an object that is still missing a shard reports the smaller
    figure it genuinely occupies.
    """
    if mode == "ec":
        shard_size = -(-size // DATA_SHARDS)  # ceil
        return len(place["shards"]) * shard_size, len(place["shards"])
    copies = len(place["stored_on"])
    return copies * size, copies


def measure(coordinator, mode, size, reps, warmup, verbose):
    """Run one (mode, size) cell and return its rows."""
    # A fixed name per cell, so re-running the benchmark overwrites its own
    # objects instead of filling the cluster with a new set every time.
    name = f"bench-{mode}-{human_size(size)}.bin"
    payload = payload_of(size, mode)
    expected = hashlib.sha256(payload).digest()
    header_mode = "ec" if mode == "ec" else "replication"

    rows = []

    for _ in range(warmup):
        coordinator.put(name, payload, header_mode)
        coordinator.get(name)

    place = coordinator.placement(name)
    total_bytes, touched = stored_bytes(mode, size, place)
    overhead = total_bytes / size if size else 0.0

    def record(operation, rep, seconds, verified, nodes):
        rows.append(
            {
                "mode": mode,
                "size_bytes": size,
                "operation": operation,
                "rep": rep,
                "seconds": round(seconds, 6),
                "throughput_mb_s": round(throughput(size, seconds), 4),
                "nodes_touched": nodes,
                "bytes_stored": total_bytes,
                "overhead_ratio": round(overhead, 4),
                "verified": verified,
            }
        )

    for rep in range(1, reps + 1):
        seconds, _ = coordinator.put(name, payload, header_mode)
        record("write", rep, seconds, True, touched)

        seconds, response = coordinator.get(name)
        verified = hashlib.sha256(response.content).digest() == expected
        record("read", rep, seconds, verified, touched)
        if not verified:
            print(f"  ! {mode} {human_size(size)} read did not match what was written")

        if mode == "ec":
            # Two shards ignored on purpose: the most degraded read the 4+2
            # scheme can still serve, and the one that costs the most to solve.
            seconds, response = coordinator.get(name, simulate_missing=PARITY_SHARDS)
            verified = hashlib.sha256(response.content).digest() == expected
            record("degraded_read", rep, seconds, verified, DATA_SHARDS)
            if response.headers.get("X-Degraded-Read") != "true":
                print(f"  ! {mode} {human_size(size)} degraded read was not degraded")
            if not verified:
                print(f"  ! {mode} {human_size(size)} degraded read did not match")

        if verbose:
            print(f"    rep {rep}/{reps} done")

    return rows


def summarise(rows):
    """Median seconds per (mode, size, operation)."""
    grouped = {}
    for row in rows:
        key = (row["mode"], row["size_bytes"], row["operation"])
        grouped.setdefault(key, []).append(row)

    summary = {}
    for key, entries in grouped.items():
        times = [e["seconds"] for e in entries]
        summary[key] = {
            "median": statistics.median(times),
            "min": min(times),
            "throughput": throughput(key[1], statistics.median(times)),
            "overhead": entries[0]["overhead_ratio"],
            "bytes_stored": entries[0]["bytes_stored"],
        }
    return summary


def print_report(summary, sizes, modes):
    operations = ["write", "read", "degraded_read"]

    print()
    print("=" * 78)
    print("  LATTICE  --  replication vs erasure coding")
    print("=" * 78)
    print()
    print(f"{'size':>8}  {'mode':<12} {'write':>10} {'read':>10} "
          f"{'degraded':>10} {'stored':>10} {'overhead':>9}")
    print("-" * 78)

    for size in sizes:
        for mode in modes:
            cells = []
            for operation in operations:
                entry = summary.get((mode, size, operation))
                cells.append(f"{entry['median'] * 1000:8.1f}ms" if entry else f"{'-':>10}")
            entry = summary.get((mode, size, "read"))
            stored = human_size(entry["bytes_stored"]) if entry else "-"
            overhead = f"{entry['overhead']:.2f}x" if entry else "-"
            print(f"{human_size(size):>8}  {mode:<12} {cells[0]} {cells[1]} {cells[2]} "
                  f"{stored:>10} {overhead:>9}")
        print()

    print("Read the columns as:")
    print("  write           object in, all copies or shards durable")
    print("  read            healthy read; replication serves it from one node")
    print(f"  degraded        EC read with {PARITY_SHARDS} shards ignored, so the data is")
    print(f"                  solved for from {DATA_SHARDS} of {TOTAL_SHARDS}")
    print("  overhead        bytes the cluster stores per byte of object")
    print()

    for size in sizes:
        rep = summary.get(("replication", size, "read"))
        ec = summary.get(("ec", size, "read"))
        degraded = summary.get(("ec", size, "degraded_read"))
        if not (rep and ec):
            continue
        print(f"At {human_size(size)}: EC stores {ec['overhead']:.2f}x against "
              f"{rep['overhead']:.2f}x, and costs "
              f"{ec['median'] / rep['median']:.1f}x on a healthy read"
              + (f", {degraded['median'] / rep['median']:.1f}x degraded." if degraded else "."))
    print()
    print("Reconstruction here is pure Python over GF(256), so the degraded column")
    print("is dominated by CPU rather than by the network. A production code would")
    print("use SIMD tables; the shape of the trade-off is what this shows.")


# ---------------------------------------------------------------------- main


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark replication against erasure coding, to CSV."
    )
    parser.add_argument("--coordinator", default=COORDINATOR, help=f"default {COORDINATOR}")
    parser.add_argument(
        "--sizes",
        default="16KB,64KB,256KB",
        help="comma separated object sizes (default: 16KB,64KB,256KB)",
    )
    parser.add_argument("--reps", type=int, default=5, help="measurements per cell (default 5)")
    parser.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="unmeasured write+read passes before timing (default 1)",
    )
    parser.add_argument(
        "--modes",
        default="replication,ec",
        help="which modes to measure (default: both)",
    )
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"CSV path (default {DEFAULT_OUT})")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    sizes = [parse_size(s) for s in args.sizes.split(",") if s.strip()]
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    coordinator = Coordinator(args.coordinator)

    try:
        state = coordinator.health()
    except requests.RequestException as e:
        sys.exit(
            f"Can't reach the coordinator at {args.coordinator} ({e}).\n"
            "Start the cluster first:  docker compose up -d --build"
        )

    if state["healthy"] < TOTAL_SHARDS:
        print(f"! only {state['healthy']}/8 nodes healthy -- EC needs {TOTAL_SHARDS} "
              "to place a full stripe, and the figures below will reflect that")

    print(f"coordinator {args.coordinator}, {state['healthy']}/8 nodes healthy")
    print(f"{args.reps} reps per cell, {args.warmup} warmup pass(es), seed {SEED}")

    rows = []
    for size in sizes:
        for mode in modes:
            print(f"  measuring {mode:<12} {human_size(size):>7} ...", end="", flush=True)
            started = time.perf_counter()
            rows += measure(coordinator, mode, size, args.reps, args.warmup, args.verbose)
            print(f" {time.perf_counter() - started:5.1f}s")

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print_report(summarise(rows), sizes, modes)
    print(f"{len(rows)} measurements written to {args.out}")
    print("The dashboard picks this file up on its Benchmark tab.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
