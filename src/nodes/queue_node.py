import hashlib
import time
from typing import Dict, Any, List

class ConsistentHashRing:
    def __init__(self, nodes: List[str], replicas=3):
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []
        for node in nodes:
            self.add_node(node)

    def add_node(self, node: str):
        for i in range(self.replicas):
            key = self._hash(f"{node}:{i}")
            self.ring[key] = node
            self.sorted_keys.append(key)
        self.sorted_keys.sort()

    def get_node(self, key: str) -> str:
        if not self.ring:
            return None
        h = self._hash(key)
        for k in self.sorted_keys:
            if h <= k:
                return self.ring[k]
        return self.ring[self.sorted_keys[0]]

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)


class QueueNode:
    def __init__(self, node_id: str, all_nodes: List[str]):
        self.node_id = node_id
        self.ring = ConsistentHashRing(all_nodes)
        self.queues: Dict[str, List[Any]] = {}

    def enqueue(self, queue_name: str, message: Any) -> bool:
        target_node = self.ring.get_node(queue_name)
        if target_node == self.node_id:
            if queue_name not in self.queues:
                self.queues[queue_name] = []
            self.queues[queue_name].append({"msg": message, "ts": time.time()})
            return True
        return False # Forward to target_node in actual app

    def dequeue(self, queue_name: str) -> Any:
        target_node = self.ring.get_node(queue_name)
        if target_node == self.node_id:
            if queue_name in self.queues and self.queues[queue_name]:
                return self.queues[queue_name].pop(0)
        return None
