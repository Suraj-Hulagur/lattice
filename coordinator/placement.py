"""Placement policy: decides which nodes hold which object."""

from common.models import NodeState

REPLICATION_FACTOR = 3

# A write is accepted once this many replicas are durable. The missing replica
# is restored later by the repair pass (phase 8).
WRITE_QUORUM = 2


def plan_write(ring, object_name: str, nodes: dict, factor: int = REPLICATION_FACTOR):
    """Work out where a write should go, and what is standing in for what.

    Returns (targets, handoffs). `targets` is the `factor` healthy nodes to
    write to. `handoffs` pairs each stand-in with the node it is covering for,
    as (holder, intended_owner) -- the ring wanted the owner, but it wasn't
    healthy, so the holder took the replica on its behalf.

    Walking past unhealthy nodes (rather than stopping at them) is what lets a
    write still reach full replication while part of the cluster is down; the
    handoff pairs are what let those replicas find their way home later.
    """
    preference = ring.get_nodes(object_name, count=len(nodes))
    ideal = preference[:factor]
    healthy = [node_id for node_id in preference if nodes[node_id].state == NodeState.HEALTHY]
    targets = healthy[:factor]

    displaced = [node_id for node_id in ideal if node_id not in targets]
    standins = [node_id for node_id in targets if node_id not in ideal]

    # zip() stops at the shorter list, which is what we want: if the cluster is
    # too degraded to find a stand-in for every displaced owner, the replicas we
    # couldn't place simply aren't owed back to anyone.
    return targets, list(zip(standins, displaced))


def select_replicas(ring, object_name: str, nodes: dict, factor: int = REPLICATION_FACTOR) -> list:
    """Just the write targets, for callers that don't care about handoffs."""
    targets, _ = plan_write(ring, object_name, nodes, factor)
    return targets


def read_order(ring, object_name: str, nodes: dict) -> list:
    """Nodes to try on a GET, best first.

    Healthy nodes come first in ring order; the rest are kept as a last resort
    because a node marked suspected/failed may still answer.
    """
    preference = ring.get_nodes(object_name, count=len(nodes))
    healthy = [n for n in preference if nodes[n].state == NodeState.HEALTHY]
    degraded = [n for n in preference if nodes[n].state != NodeState.HEALTHY]
    return healthy + degraded
