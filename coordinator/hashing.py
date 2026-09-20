import hashlib
import bisect

class ConsistentHashRing:
    def __init__(self, virtual_nodes=100):
        self.virtual_nodes = virtual_nodes
        self.ring = {}
        self.sorted_keys = []

    def _hash(self, key: str) -> int:
        # Simple md5 hash converted to integer
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node_id: str):
        for i in range(self.virtual_nodes):
            v_node_key = f"{node_id}#{i}"
            key_hash = self._hash(v_node_key)
            self.ring[key_hash] = node_id
            bisect.insort(self.sorted_keys, key_hash)

    def remove_node(self, node_id: str):
        for i in range(self.virtual_nodes):
            v_node_key = f"{node_id}#{i}"
            key_hash = self._hash(v_node_key)
            if key_hash in self.ring:
                del self.ring[key_hash]
                self.sorted_keys.remove(key_hash)

    def get_nodes(self, object_name: str, count: int = 3) -> list:
        """Walk the ring clockwise and return up to `count` DISTINCT physical nodes.

        Several virtual nodes on the ring map back to the same physical node, so
        we skip duplicates -- otherwise all 3 'replicas' could land on one box.
        """
        if not self.ring:
            return []

        key_hash = self._hash(object_name)

        # Find the first node with a hash greater than or equal to the object's hash
        idx = bisect.bisect_left(self.sorted_keys, key_hash)

        # If we went past the end of the ring, wrap around to the first node
        if idx == len(self.sorted_keys):
            idx = 0

        preference = []
        total = len(self.sorted_keys)
        for offset in range(total):
            node_id = self.ring[self.sorted_keys[(idx + offset) % total]]
            if node_id not in preference:
                preference.append(node_id)
                if len(preference) == count:
                    break
        return preference

    def get_node(self, object_name: str) -> str:
        """The primary node for an object (first entry of the preference list)."""
        preference = self.get_nodes(object_name, count=1)
        return preference[0] if preference else None
