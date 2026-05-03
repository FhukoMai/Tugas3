from collections import OrderedDict
import time

class LRUCache:
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key: str):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key: str, value: any):
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)
            
    def delete(self, key: str):
        if key in self.cache:
            del self.cache[key]

class MESICacheNode:
    def __init__(self, node_id: str, capacity: int = 100):
        self.node_id = node_id
        self.cache = LRUCache(capacity)
        # States: M (Modified), E (Exclusive), S (Shared), I (Invalid)
        self.states = {}

    def read(self, key: str):
        state = self.states.get(key, 'I')
        if state in ['M', 'E', 'S']:
            return self.cache.get(key)
        # If 'I', in a real system we would broadcast a read request to peers
        return None

    def write(self, key: str, value: any):
        state = self.states.get(key, 'I')
        self.cache.put(key, value)
        if state in ['M', 'E']:
            self.states[key] = 'M'
        elif state == 'S':
            # Broadcast invalidate to others
            self.states[key] = 'M'
        elif state == 'I':
            # Broadcast read-invalidate to others
            self.states[key] = 'M'

    def handle_bus_read(self, key: str):
        state = self.states.get(key, 'I')
        if state == 'M':
            # Write back to memory, change to S
            self.states[key] = 'S'
            return self.cache.get(key)
        elif state == 'E':
            self.states[key] = 'S'
        return None

    def handle_bus_invalidate(self, key: str):
        self.states[key] = 'I'
        self.cache.delete(key)
