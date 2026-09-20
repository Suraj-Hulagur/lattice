"""How the coordinator reaches a storage node."""

import os


def node_endpoint(node) -> str:
    """The host:port to talk to `node` on.

    Inside docker-compose every node is its own service on port 8000. Outside
    it there is only ever one node reachable on localhost, so local runs
    collapse onto it -- replication can't be exercised that way, but the
    endpoints still work.
    """
    if os.environ.get("DOCKER_ENV"):
        return node.address
    return "localhost:8000"
