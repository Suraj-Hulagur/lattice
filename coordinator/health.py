"""Failure detection, corroborated by other nodes.

A node that stops answering the coordinator is only SUSPECTED. Before calling
it FAILED, the coordinator asks up to VERIFY_PEERS other healthy nodes to ping
the suspect themselves. Only if they all agree it is unreachable does the node
become FAILED.

The point is that a missed ping says as much about the coordinator's link as
about the node. A suspect its peers can still reach is alive and stays
SUSPECTED, so the coordinator doesn't evict a healthy node from the write set
over its own connectivity problem.
"""

import asyncio

import httpx

from common.models import NodeState
from coordinator.addressing import node_endpoint

PROBE_INTERVAL = 5.0  # seconds between sweeps
PROBE_TIMEOUT = 2.0  # per-node ping timeout
VERIFY_PEERS = 3  # how many other nodes are asked to corroborate
VERIFY_TIMEOUT = 4.0  # allow for the peer's own 2s ping plus overhead

# Fallback only: if nobody can be asked (every other node is down too), a node
# is declared FAILED after this many consecutive missed probes.
FAILED_THRESHOLD = 3

# node_id -> consecutive failed probes. Reset to 0 on any successful ping.
_misses = {}

# node_id -> the last verification result, for /health to expose.
_verdicts = {}


def miss_count(node_id: str) -> int:
    return _misses.get(node_id, 0)


def last_verdict(node_id: str):
    return _verdicts.get(node_id)


async def probe_node(node, client: httpx.AsyncClient) -> NodeState:
    """Ping one node directly. Sets HEALTHY or SUSPECTED, never FAILED.

    Promotion to FAILED is left to verify_failure(), which needs a second
    opinion first.
    """
    url = f"http://{node_endpoint(node)}/ping"
    try:
        response = await client.get(url, timeout=PROBE_TIMEOUT)
        reachable = response.status_code == 200
    except Exception:
        reachable = False

    if reachable:
        _misses[node.id] = 0
        _verdicts.pop(node.id, None)
        if node.state != NodeState.HEALTHY:
            print(f"[HEALTH] {node.id} recovered ({node.state.value} -> healthy)")
            node.state = NodeState.HEALTHY
        return node.state

    _misses[node.id] = _misses.get(node.id, 0) + 1
    if node.state == NodeState.HEALTHY:
        print(f"[HEALTH] {node.id} missed a probe -> SUSPECTED")
        node.state = NodeState.SUSPECTED
    return node.state


async def _ask_peer(peer, suspect, client: httpx.AsyncClient):
    """Have `peer` ping `suspect`. True/False is its vote, None means it didn't answer."""
    # The peer pings across the cluster network, so it needs the suspect's real
    # service address -- not whatever this coordinator uses to reach it.
    url = f"http://{node_endpoint(peer)}/verify"
    try:
        response = await client.get(
            url, params={"target": suspect.address}, timeout=VERIFY_TIMEOUT
        )
        if response.status_code != 200:
            return None
        return bool(response.json().get("reachable"))
    except Exception:
        return None


async def verify_failure(suspect, nodes: dict, client: httpx.AsyncClient) -> dict:
    """Ask other healthy nodes whether they can reach `suspect`.

    Returns a verdict dict describing what the peers said and what was decided.
    """
    peers = [
        node
        for node in nodes.values()
        if node.id != suspect.id and node.state == NodeState.HEALTHY
    ][:VERIFY_PEERS]

    if not peers:
        # Nobody left to ask. Fall back to the miss count so a cluster-wide
        # outage still eventually reports FAILED rather than SUSPECTED forever.
        confirmed = _misses.get(suspect.id, 0) >= FAILED_THRESHOLD
        return {
            "suspect": suspect.id,
            "asked": [],
            "votes": {},
            "confirmed": confirmed,
            "basis": "no peers available, fell back to miss count",
        }

    votes = await asyncio.gather(*(_ask_peer(peer, suspect, client) for peer in peers))
    by_peer = {peer.id: vote for peer, vote in zip(peers, votes)}
    responded = {peer_id: vote for peer_id, vote in by_peer.items() if vote is not None}

    if not responded:
        confirmed = _misses.get(suspect.id, 0) >= FAILED_THRESHOLD
        basis = "no peer answered, fell back to miss count"
    elif any(responded.values()):
        # Someone reached it, so the node is alive from where they stand.
        confirmed = False
        basis = "a peer reached the suspect; keeping it SUSPECTED"
    else:
        confirmed = True
        basis = f"all {len(responded)} responding peer(s) agree it is unreachable"

    return {
        "suspect": suspect.id,
        "asked": [peer.id for peer in peers],
        "votes": by_peer,
        "confirmed": confirmed,
        "basis": basis,
    }


async def verify_suspects(nodes: dict, client: httpx.AsyncClient) -> list:
    """Run verification for every currently SUSPECTED node."""
    suspects = [node for node in nodes.values() if node.state == NodeState.SUSPECTED]
    verdicts = []
    for suspect in suspects:
        verdict = await verify_failure(suspect, nodes, client)
        _verdicts[suspect.id] = verdict
        verdicts.append(verdict)

        reachable_by = [p for p, v in verdict["votes"].items() if v is True]
        if verdict["confirmed"]:
            print(
                f"[VERIFY] {suspect.id} confirmed unreachable by "
                f"{verdict['asked'] or 'nobody'} -> FAILED"
            )
            suspect.state = NodeState.FAILED
        elif reachable_by:
            print(
                f"[VERIFY] {suspect.id} still reachable from {reachable_by}; "
                "coordinator link is the likely problem, staying SUSPECTED"
            )
        else:
            print(f"[VERIFY] {suspect.id} inconclusive ({verdict['basis']})")
    return verdicts


async def sweep(nodes: dict, client: httpx.AsyncClient):
    """One full pass: probe everything, then corroborate any suspects."""
    await asyncio.gather(*(probe_node(node, client) for node in nodes.values()))
    await verify_suspects(nodes, client)


async def health_check_loop(nodes: dict, after_sweep=None):
    """Probe the cluster forever. Cancelled on coordinator shutdown.

    `after_sweep` is an optional coroutine run once per sweep, after node state
    has been refreshed -- that's where the handoff pass hangs. Running it every
    sweep rather than only on a recovery edge means a delivery that fails
    (because the holder is itself down) is simply retried next time.
    """
    print(
        f"[HEALTH] probing {len(nodes)} nodes every {PROBE_INTERVAL}s, "
        f"confirming failures with {VERIFY_PEERS} peers"
    )
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
