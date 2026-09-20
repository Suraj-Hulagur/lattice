"""Phase 12: a deterministic failure demo.

Stops real containers and narrates what the cluster does about it, so the
behaviour the earlier phases built can be watched rather than described:

  replication   a replica is lost, the repair pass puts the third copy back
  ec            shards are lost, reads keep working, repair rebuilds them
  verification  a suspected node is only declared failed once peers agree

Determinism comes from fixing everything the demo controls: payloads come from
a seeded generator, object names are constant, and the nodes that get stopped
are read back from the object's own placement rather than picked at random. The
same command tells the same story every time.

Usage, from the repo root with the cluster up:

    python chaos/demo.py                    # all three scenarios, in order
    python chaos/demo.py --scenario ec
    python chaos/demo.py --restore          # just bring every node back up
"""

import argparse
import hashlib
import os
import random
import shutil
import subprocess
import sys
import time

import requests

COORDINATOR = os.environ.get("LATTICE_COORDINATOR", "http://localhost:9700")

# Fixed so two runs produce byte-identical objects, and therefore the same ring
# placement and the same nodes being stopped.
SEED = 20260920
PAYLOAD_SIZE = 64 * 1024

# The health sweep runs every 5s, and a failure is suspected and confirmed
# within one sweep, so nothing here should need more than a few of them.
STATE_TIMEOUT = 60.0
REPAIR_TIMEOUT = 60.0

COMPOSE = None


# ------------------------------------------------------------------ narration


class Colour:
    """ANSI codes, blanked out when the output is not a terminal."""

    enabled = sys.stdout.isatty()

    @classmethod
    def _wrap(cls, code, text):
        return f"\033[{code}m{text}\033[0m" if cls.enabled else text

    @classmethod
    def bold(cls, text):
        return cls._wrap("1", text)

    @classmethod
    def green(cls, text):
        return cls._wrap("32", text)

    @classmethod
    def yellow(cls, text):
        return cls._wrap("33", text)

    @classmethod
    def red(cls, text):
        return cls._wrap("31", text)

    @classmethod
    def dim(cls, text):
        return cls._wrap("2", text)


def banner(title):
    print()
    print(Colour.bold("=" * 72))
    print(Colour.bold(f"  {title}"))
    print(Colour.bold("=" * 72))


def step(text):
    print()
    print(f"{Colour.bold('>')} {text}")


def ok(text):
    print(f"  {Colour.green('OK')}   {text}")


def info(text):
    print(f"  {Colour.dim('..')}   {text}")


def warn(text):
    print(f"  {Colour.yellow('!!')}   {text}")


def fail(text):
    print(f"  {Colour.red('XX')}   {text}")


# -------------------------------------------------------------- docker compose


def compose_command():
    """`docker compose` on current Docker, `docker-compose` on older installs."""
    if shutil.which("docker"):
        probe = subprocess.run(
            ["docker", "compose", "version"], capture_output=True, text=True
        )
        if probe.returncode == 0:
            return ["docker", "compose"]
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    sys.exit(
        "Neither `docker compose` nor `docker-compose` is available.\n"
        "This demo stops real containers, so it needs the cluster running."
    )


def compose(*args):
    result = subprocess.run(COMPOSE + list(args), capture_output=True, text=True)
    if result.returncode != 0:
        fail(f"docker compose {' '.join(args)} failed: {result.stderr.strip()}")
    return result


def stop_node(node_id):
    # A short grace period: uvicorn exits on SIGTERM straight away, and waiting
    # the default 10 seconds for every node makes the demo drag.
    compose("stop", "-t", "2", node_id)
    info(f"container {node_id} stopped")


def start_node(node_id):
    compose("start", node_id)


# --------------------------------------------------------------- coordinator


def get(path, **params):
    response = requests.get(f"{COORDINATOR}{path}", params=params, timeout=30)
    response.raise_for_status()
    return response


def health():
    return get("/health").json()


def placement(name):
    return get(f"/objects/{name}/placement").json()


def upload(name, payload, mode="replication"):
    response = requests.put(
        f"{COORDINATOR}/objects/{name}",
        files={"file": (name, payload)},
        headers={"X-Storage-Mode": mode},
        timeout=120,
    )
    response.raise_for_status()
    return response.json()


def download(name, simulate_missing=0):
    params = {"simulate_missing": simulate_missing} if simulate_missing else {}
    response = requests.get(f"{COORDINATOR}/objects/{name}", params=params, timeout=120)
    response.raise_for_status()
    return response


def payload_for(name):
    """Deterministic bytes: same object name and seed, same content."""
    generator = random.Random(f"{SEED}:{name}")
    return bytes(generator.getrandbits(8) for _ in range(PAYLOAD_SIZE))


def digest(data):
    return hashlib.sha256(data).hexdigest()[:12]


def wait_for(description, predicate, timeout):
    """Poll until `predicate()` is true. Returns the wait in seconds, or None."""
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        try:
            if predicate():
                return time.monotonic() - started
        except requests.RequestException:
            pass
        time.sleep(0.5)
    warn(f"timed out after {timeout:.0f}s waiting for {description}")
    return None


def node_state(node_id):
    for state, members in health()["by_state"].items():
        if node_id in members:
            return state
    return "unknown"


def restore_all(quiet=False):
    """Start every node container and wait for the coordinator to notice."""
    for index in range(1, 9):
        start_node(f"node{index}")
    elapsed = wait_for("all 8 nodes healthy", lambda: health()["healthy"] == 8, STATE_TIMEOUT)
    if elapsed is not None and not quiet:
        ok(f"all 8 nodes healthy again after {elapsed:.1f}s")
    return elapsed


def require_cluster():
    try:
        state = health()
    except requests.RequestException as e:
        sys.exit(
            f"Can't reach the coordinator at {COORDINATOR} ({e}).\n"
            "Start the cluster first:  docker compose up -d --build"
        )
    down = [n for s, ns in state["by_state"].items() if s != "healthy" for n in ns]
    if down:
        warn(f"{len(down)} node(s) are not healthy: {', '.join(sorted(down))}")
        step("bringing the cluster back to full strength before starting")
        restore_all(quiet=True)
    return health()


# ------------------------------------------------------- scenario: replication


def scenario_replication():
    banner("SCENARIO 1  --  a replicated object loses a node")

    name = "chaos-replicated.bin"
    payload = payload_for(name)

    step(f"writing {name} ({len(payload) // 1024} KiB, sha {digest(payload)}) as 3 copies")
    written = upload(name, payload, mode="replication")
    holders = written["replicas"]
    ok(f"stored on {', '.join(holders)}")

    victim, survivors = holders[0], holders[1:]

    step(f"stopping {victim}, one of the three nodes holding it")
    stop_node(victim)

    elapsed = wait_for(
        f"{victim} to be confirmed FAILED",
        lambda: node_state(victim) == "failed",
        STATE_TIMEOUT,
    )
    if elapsed is not None:
        ok(f"coordinator confirmed {victim} FAILED after {elapsed:.1f}s")
        info("suspected on the first missed probe, failed once its peers agreed")

    step("the object is still readable from the surviving copies")
    read = download(name)
    if read.content == payload:
        ok(f"read {digest(read.content)} from {read.headers.get('X-Served-By', '?')}, bytes match")
    else:
        fail("read did not match what was written")

    step("waiting for the repair pass to restore the third copy")
    elapsed = wait_for(
        "replication factor back to 3",
        lambda: placement(name)["fully_protected"],
        REPAIR_TIMEOUT,
    )
    after = placement(name)
    if elapsed is not None:
        ok(f"back to {after['live_replicas']} live copies after {elapsed:.1f}s")
    rebuilt = [n for n in after["stored_on"] if n not in survivors]
    info(f"now on {', '.join(after['stored_on'])}; new copy on {', '.join(rebuilt) or 'none'}")
    info("repair copies a whole object, which is what erasure coding avoids")

    step("reading again, now that repair has moved a copy")
    read = download(name)
    if read.content == payload:
        ok(f"served by {read.headers.get('X-Served-By', '?')}, bytes still match")
    else:
        fail("read did not match after repair")

    return [victim]


# ---------------------------------------------------------------- scenario: EC


def scenario_ec():
    banner("SCENARIO 2  --  an erasure coded object loses two nodes")

    name = "chaos-coded.bin"
    payload = payload_for(name)
    kib = len(payload) // 1024

    step(f"writing {name} ({kib} KiB, sha {digest(payload)}) as 4+2 shards")
    written = upload(name, payload, mode="ec")
    shards = written["shards"]
    for shard in shards:
        info(f"{shard['shard']} -> {shard['node']}")
    ok(f"6 shards on 6 nodes: {kib * 6 // 4} KiB stored for a {kib} KiB object "
       f"(1.5x, against 3x for replication)")

    step("a full read uses the four data shards as they are")
    read = download(name)
    ok(f"degraded={read.headers.get('X-Degraded-Read')}, "
       f"shards={read.headers.get('X-Shards-Available')}/6")

    victims = [shards[0]["node"], shards[1]["node"]]
    step(f"stopping {victims[0]} and {victims[1]}, two of the six shard holders")
    for victim in victims:
        stop_node(victim)

    step("reading immediately: any four surviving shards can solve for the data")
    read = download(name)
    if read.content == payload:
        ok(f"read {digest(read.content)}, byte-identical to the original")
    else:
        fail("degraded read did not match what was written")
    if read.headers.get("X-Degraded-Read") == "true":
        ok(f"degraded read from {read.headers.get('X-Shards-Available')}/6 shards, "
           f"rebuilt without {read.headers.get('X-Shards-Missing-List', '?')}")
    else:
        # Repair runs after every sweep, so a slow host can heal the object
        # before this read lands. Worth saying rather than pretending otherwise.
        info("repair got there first, so this read was already a full read")

    step("waiting for the repair pass to rebuild the missing shards")
    elapsed = wait_for(
        "all 6 shards on live nodes",
        lambda: placement(name)["fully_protected"],
        REPAIR_TIMEOUT,
    )
    after = placement(name)
    if elapsed is not None:
        ok(f"back to {after['live_shards']}/6 shards after {elapsed:.1f}s")
    original_nodes = [s["node"] for s in shards]
    for label, node in after["shards"].items():
        info(f"{label} -> {node}" + ("   (rebuilt)" if node not in original_nodes else ""))
    info("rebuilding a shard reads 4 shards, not a whole second copy of the object")

    step("a guaranteed degraded read for comparison, via ?simulate_missing=2")
    read = download(name, simulate_missing=2)
    if read.content == payload:
        ok(f"ignored two shards deliberately, still reconstructed {digest(read.content)}")
    else:
        fail("simulated degraded read did not match")

    return victims


# ------------------------------------------------------- scenario: corroboration


def scenario_verification():
    banner("SCENARIO 3  --  a failure is only believed once peers agree")

    victim = "node5"
    step(f"stopping {victim} and watching how the verdict is reached")
    stop_node(victim)

    elapsed = wait_for(
        f"{victim} to leave the healthy set",
        lambda: node_state(victim) in ("suspected", "failed"),
        STATE_TIMEOUT,
    )
    if elapsed is not None:
        ok(f"{victim} left the healthy set after {elapsed:.1f}s")

    wait_for(
        f"a recorded verdict for {victim}",
        lambda: victim in health().get("verdicts", {}),
        STATE_TIMEOUT,
    )

    verdict = health().get("verdicts", {}).get(victim)
    if verdict:
        info(f"coordinator asked {', '.join(verdict['asked']) or 'nobody'} to ping {victim}")
        names = {True: "reachable", False: "unreachable", None: "no answer"}
        for peer, vote in verdict["votes"].items():
            info(f"{peer} says {victim} is {names[vote]}")
        ok(f"verdict: {verdict['basis']}")
        info("this is what stops the coordinator evicting a healthy node over its")
        info("own network trouble: one peer reaching it keeps it merely SUSPECTED")
    else:
        warn("no verdict recorded; the node may have failed via the miss-count fallback")

    info(f"{victim} is now {node_state(victim).upper()}")
    return [victim]


# ---------------------------------------------------------------------- main


SCENARIOS = {
    "replication": scenario_replication,
    "ec": scenario_ec,
    "verification": scenario_verification,
}


def main():
    global COORDINATOR, COMPOSE

    parser = argparse.ArgumentParser(
        description="Deterministic container-stopping failure demo for LATTICE."
    )
    parser.add_argument(
        "--scenario",
        choices=["all", *SCENARIOS],
        default="all",
        help="which failure to demonstrate (default: all three, in order)",
    )
    parser.add_argument("--coordinator", default=COORDINATOR, help=f"default {COORDINATOR}")
    parser.add_argument(
        "--restore",
        action="store_true",
        help="start every node container, wait for a healthy cluster, and exit",
    )
    parser.add_argument(
        "--keep-down",
        action="store_true",
        help="leave stopped containers stopped, for poking at the cluster afterwards",
    )
    args = parser.parse_args()

    COORDINATOR = args.coordinator.rstrip("/")
    COMPOSE = compose_command()

    if args.restore:
        banner("RESTORING THE CLUSTER")
        restore_all()
        return 0

    banner("LATTICE FAILURE DEMO")
    state = require_cluster()
    info(f"coordinator at {COORDINATOR}, {state['healthy']}/8 nodes healthy")
    info(f"seed {SEED}, {PAYLOAD_SIZE // 1024} KiB payloads: this run is reproducible")

    chosen = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    stopped = []
    try:
        for scenario in chosen:
            stopped += SCENARIOS[scenario]()
    finally:
        banner("CLEANING UP")
        if args.keep_down:
            warn(f"leaving {', '.join(stopped) or 'nothing'} stopped (--keep-down)")
            info("bring them back with:  python chaos/demo.py --restore")
        else:
            restore_all()
            info("a recovered node still holds the objects it had before it went")
            info("down, but the placement index no longer points at them")

    print()
    ok("demo complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
