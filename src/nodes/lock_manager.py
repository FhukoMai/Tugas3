import time
from typing import Dict, Any

class LockManager:
    def __init__(self):
        self.locks: Dict[str, Dict[str, Any]] = {}

    def apply_command(self, command: dict):
        action = command.get("action")
        lock_id = command.get("lock_id")
        client_id = command.get("client_id")
        lock_type = command.get("lock_type", "exclusive")
        
        if action == "acquire":
            if lock_id not in self.locks:
                self.locks[lock_id] = {
                    "type": lock_type,
                    "owners": [client_id],
                    "timestamp": time.time()
                }
                return True
            else:
                current_lock = self.locks[lock_id]
                if current_lock["type"] == "shared" and lock_type == "shared":
                    if client_id not in current_lock["owners"]:
                        current_lock["owners"].append(client_id)
                    return True
                return False
                
        elif action == "release":
            if lock_id in self.locks:
                current_lock = self.locks[lock_id]
                if client_id in current_lock["owners"]:
                    current_lock["owners"].remove(client_id)
                    if not current_lock["owners"]:
                        del self.locks[lock_id]
                    return True
            return False
            
    def get_lock_status(self, lock_id: str):
        return self.locks.get(lock_id, None)

    def check_deadlock(self):
        # Placeholder for deadlock detection logic
        # In a real distributed system, we would build a wait-for graph
        pass
