"""Placement policy: decides which nodes hold which object."""

from common.models import NodeState

REPLICATION_FACTOR = 3

# A write is accepted once this many replicas are durable. The missing replica
# is restored later by the repair pass (phase 8).
WRITE_QUORUM = 2


def select_replicas(ring, object_name: str, nodes: dict, factor: int = REPLICATION_FACTOR) -> list:
    """Pick `factor` distinct healthy nodes for an object.

    We ask the ring for a preference list covering every node, then take the
    healthy ones in ring order. Walking past unhealthy nodes (rather than
    stopping at them) is what lets a write still succeed while part of the
    cluster is down.
    """
    preference = ring.get_nodes(object_name, count=len(nodes))
    healthy = [node_id for node_id in preference if nodes[node_id].state == NodeState.HEALTHY]
    return healthy[:factor]


def read_order(ring, object_name: str, nodes: dict) -> list:
    """Nodes to try on a GET, best first.

    Healthy nodes come first in ring order; the rest are kept as a last resort
    because a node marked suspected/failed may still answer.
    """
    preference = ring.get_nodes(object_name, count=len(nodes))
    healthy = [n for n in preference if nodes[n].state == NodeState.HEALTHY]
    degraded = [n for n in preference if nodes[n].state != NodeState.HEALTHY]
    return healthy + degraded
