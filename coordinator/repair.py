"""Replica repair: restore the replication factor after permanent failures.

When a node stays FAILED, the hinted handoff can't deliver -- the owner is
gone for good.  The repair sweep scans the placement index, finds objects
that have dropped below the replication factor, reads a surviving copy from
a healthy holder, and writes it to a new healthy node that doesn't already
have a copy.

This runs after each health sweep, alongside the handoff pass.
"""

import httpx

from common.models import NodeState
from coordinator.addressing import node_endpoint
from coordinator.placement import repair_candidates


async def repair_replicas(
    nodes: dict,
    placement_index: dict,
    ring,
    replication_factor: int,
    client: httpx.AsyncClient,
):
    """Scan all replicated objects and re-replicate any that are under-replicated.

    `client` is the coordinator's shared HTTP client: building a fresh one per
    sweep costs far more than the repair itself.
    """
    failed_nodes = {nid for nid, n in nodes.items() if n.state == NodeState.FAILED}
    if not failed_nodes:
        return []

    healthy_nodes = {nid for nid, n in nodes.items() if n.state == NodeState.HEALTHY}
    if not healthy_nodes:
        print("[REPAIR] No healthy nodes available for repair")
        return []

    repairs = []

    # Iterate over a snapshot of the index so mutations don't break the loop
    for object_name, holders in list(placement_index.items()):
        # Filter out failed nodes from the holder list
        alive = [h for h in holders if h not in failed_nodes]
        lost = [h for h in holders if h in failed_nodes]

        if not lost:
            continue  # all holders are healthy, nothing to do

        if not alive:
            print(f"[REPAIR] {object_name}: ALL replicas lost, cannot repair")
            continue

        needed = replication_factor - len(alive)
        if needed <= 0:
            # Still have enough replicas even after removing failed nodes
            placement_index[object_name] = alive
            continue

        # Pick healthy nodes that don't already hold a copy, ring-preferred
        # first so the repaired replica lands as close to its ideal home as
        # the current cluster allows.
        candidates = repair_candidates(ring, object_name, nodes, exclude=set(alive))
        if not candidates:
            print(f"[REPAIR] {object_name}: no spare healthy nodes for repair")
            placement_index[object_name] = alive
            continue

        # Read from any surviving holder. A node can be in `alive` and still
        # not answer -- SUSPECTED nodes are kept there deliberately, because
        # a copy that might be readable beats giving up on the object.
        source = None
        payload = None
        for holder in alive:
            source_url = f"http://{node_endpoint(nodes[holder])}/data/{object_name}"
            try:
                resp = await client.get(source_url)
            except Exception as e:
                print(f"[REPAIR] {object_name}: error reading from {holder}: {e}")
                continue
            if resp.status_code == 200:
                source, payload = holder, resp.content
                break
            print(f"[REPAIR] {object_name}: {holder} returned {resp.status_code}")

        if payload is None:
            print(f"[REPAIR] {object_name}: no surviving holder could serve a copy")
            continue

        # Write to as many candidates as needed
        new_holders = []
        for target in candidates[:needed]:
            target_url = f"http://{node_endpoint(nodes[target])}/data/{object_name}"
            try:
                put_resp = await client.put(
                    target_url, files={"file": (object_name, payload)}
                )
                if put_resp.status_code == 200:
                    new_holders.append(target)
                    print(f"[REPAIR] {object_name}: repaired {source} -> {target}")
                else:
                    print(f"[REPAIR] {object_name}: write to {target} returned {put_resp.status_code}")
            except Exception as e:
                print(f"[REPAIR] {object_name}: write to {target} failed: {e}")

        # Update placement index
        placement_index[object_name] = alive + new_holders
        if new_holders:
            repairs.append({
                "object": object_name,
                "lost_on": lost,
                "repaired_to": new_holders,
                "total_replicas": len(alive) + len(new_holders),
            })

    if repairs:
        print(f"[REPAIR] Completed {len(repairs)} repair(s)")
    return repairs
