"""Hinted handoff bookkeeping.

When the ring wants an object on a node that isn't healthy, the write goes to
the next healthy node instead and we leave a hint here: "this holder is keeping
`object_name` on behalf of `intended_owner`". Once the owner comes back, the
handoff pass in the coordinator ships the replica over and clears the hint.

Without hints a diverted replica is indistinguishable from a correctly placed
one, so the cluster would drift permanently out of ring order after any outage.
"""

# (object_name, intended_owner) -> holder
_hints = {}


def record(object_name: str, holder: str, intended_owner: str):
    """Note that `holder` is keeping a replica owed to `intended_owner`."""
    # A node never holds a hint for itself; that would make the handoff pass
    # try to copy an object onto the node it already lives on.
    if holder == intended_owner:
        return
    _hints[(object_name, intended_owner)] = holder


def drop(object_name: str, intended_owner: str):
    _hints.pop((object_name, intended_owner), None)


def for_owner(owner: str) -> list:
    """Every (object_name, holder) currently owed to `owner`."""
    return [
        (object_name, holder)
        for (object_name, intended_owner), holder in _hints.items()
        if intended_owner == owner
    ]


def owners() -> list:
    """Distinct nodes that are owed at least one replica."""
    return list({intended_owner for _, intended_owner in _hints})


def count() -> int:
    return len(_hints)


def snapshot() -> list:
    """JSON-friendly view of the whole hint table."""
    return [
        {"object_name": object_name, "intended_owner": intended_owner, "held_by": holder}
        for (object_name, intended_owner), holder in sorted(_hints.items())
    ]
