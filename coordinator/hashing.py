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

    def get_node(self, object_name: str) -> str:
        if not self.ring:
            return None
        
        key_hash = self._hash(object_name)
        
        # Find the first node with a hash greater than or equal to the object's hash
        idx = bisect.bisect_left(self.sorted_keys, key_hash)
        
        # If we went past the end of the ring, wrap around to the first node
        if idx == len(self.sorted_keys):
            idx = 0
            
        return self.ring[self.sorted_keys[idx]]
