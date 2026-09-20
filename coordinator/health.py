"""Failure detection: background probes that keep node state current.

A node that misses a single probe is only SUSPECTED -- one dropped packet or a
slow response shouldn't take it out of the write set for long. It is declared
FAILED once it misses FAILED_THRESHOLD probes in a row. Placement treats both
the same way (neither is HEALTHY, so neither takes new writes); the distinction
is there so the repair pass (phase 8) can tell a blip from a node that is
really gone.
"""

import asyncio

import httpx

from common.models import NodeState
from coordinator.addressing import node_endpoint

PROBE_INTERVAL = 5.0  # seconds between sweeps
PROBE_TIMEOUT = 2.0  # per-node ping timeout
FAILED_THRESHOLD = 3  # consecutive misses before SUSPECTED -> FAILED

# node_id -> consecutive failed probes. Reset to 0 on any successful ping.
_misses = {}


def miss_count(node_id: str) -> int:
    return _misses.get(node_id, 0)


async def probe_node(node, client: httpx.AsyncClient) -> NodeState:
    """Ping one node and advance its state. Returns the state it ended in."""
    url = f"http://{node_endpoint(node)}/ping"
    try:
        response = await client.get(url, timeout=PROBE_TIMEOUT)
        reachable = response.status_code == 200
    except Exception:
        reachable = False

    if reachable:
        _misses[node.id] = 0
        if node.state != NodeState.HEALTHY:
            print(f"[HEALTH] {node.id} recovered ({node.state.value} -> healthy)")
            node.state = NodeState.HEALTHY
        return node.state

    misses = _misses.get(node.id, 0) + 1
    _misses[node.id] = misses

    if misses >= FAILED_THRESHOLD:
        if node.state != NodeState.FAILED:
            print(f"[HEALTH] {node.id} missed {misses} probes -> FAILED")
            node.state = NodeState.FAILED
    elif node.state == NodeState.HEALTHY:
        print(f"[HEALTH] {node.id} missed a probe -> SUSPECTED")
        node.state = NodeState.SUSPECTED

    return node.state


async def sweep(nodes: dict, client: httpx.AsyncClient):
    """Probe every node once, in parallel."""
    await asyncio.gather(*(probe_node(node, client) for node in nodes.values()))


async def health_check_loop(nodes: dict, after_sweep=None):
    """Probe the cluster forever. Cancelled on coordinator shutdown.

    `after_sweep` is an optional coroutine run once per sweep, after node state
    has been refreshed -- that's where the handoff pass hangs. Running it every
    sweep rather than only on a recovery edge means a delivery that fails
    (because the holder is itself down) is simply retried next time.
    """
    print(f"[HEALTH] probing {len(nodes)} nodes every {PROBE_INTERVAL}s")
    async with httpx.AsyncClient() as client:
        while True:
            try:
                await sweep(nodes, client)
                if after_sweep is not None:
                    await after_sweep()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # A detector that dies takes the whole cluster view with it.
                print(f"[HEALTH] sweep failed, continuing: {e}")
            await asyncio.sleep(PROBE_INTERVAL)
