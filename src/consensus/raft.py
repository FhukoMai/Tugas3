import asyncio
import time
import random
import logging
from typing import List, Dict, Any, Callable
from src.communication.message_passing import messenger
from src.utils.config import settings

logger = logging.getLogger(__name__)

class RaftNode:
    def __init__(self, node_id: str, peers: List[str], apply_callback: Callable):
        self.node_id = node_id
        self.peers = peers
        self.apply_callback = apply_callback # Called when an entry is committed
        
        # Persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = [] # List of {"term": int, "command": any}
        
        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        self.state = "FOLLOWER"
        self.leader_id = None
        
        # Volatile state on leaders
        self.next_index = {p: 1 for p in peers}
        self.match_index = {p: 0 for p in peers}
        
        # Timers
        self.election_timeout = random.uniform(1.5, 3.0) # 1.5s to 3s
        self.last_heartbeat = time.time()
        self.running = False
        self.loop_task = None
        
    async def start(self):
        self.running = True
        self.loop_task = asyncio.create_task(self.run_loop())
        logger.info(f"Raft node {self.node_id} started as FOLLOWER")
        
    async def stop(self):
        self.running = False
        if self.loop_task:
            self.loop_task.cancel()

    async def run_loop(self):
        while self.running:
            try:
                now = time.time()
                if self.state in ["FOLLOWER", "CANDIDATE"]:
                    if now - self.last_heartbeat > self.election_timeout:
                        await self.start_election()
                elif self.state == "LEADER":
                    await self.send_heartbeats()
                    await asyncio.sleep(0.5) # Heartbeat interval
            except Exception as e:
                logger.error(f"Error in Raft loop: {e}")
            await asyncio.sleep(0.1)

    async def start_election(self):
        self.state = "CANDIDATE"
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(1.5, 3.0)
        logger.info(f"{self.node_id} starting election for term {self.current_term}")
        
        votes = 1
        
        last_log_index = len(self.log)
        last_log_term = self.log[-1]["term"] if self.log else 0
        
        tasks = []
        for peer in self.peers:
            payload = {
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term
            }
            tasks.append(messenger.send_message(peer, "raft/request_vote", payload))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        if self.state != "CANDIDATE":
            return
            
        for res in results:
            if isinstance(res, dict) and res.get("vote_granted"):
                votes += 1
                
        if votes > (len(self.peers) + 1) // 2:
            self.become_leader()

    def become_leader(self):
        self.state = "LEADER"
        self.leader_id = self.node_id
        for peer in self.peers:
            self.next_index[peer] = len(self.log) + 1
            self.match_index[peer] = 0
        logger.info(f"{self.node_id} became LEADER for term {self.current_term}")
        
    async def send_heartbeats(self):
        tasks = []
        for peer in self.peers:
            prev_log_index = self.next_index[peer] - 1
            prev_log_term = self.log[prev_log_index - 1]["term"] if prev_log_index > 0 else 0
            entries = self.log[prev_log_index:]
            
            payload = {
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries,
                "leader_commit": self.commit_index
            }
            tasks.append(self.send_append_entries(peer, payload))
        await asyncio.gather(*tasks)
        
    async def send_append_entries(self, peer: str, payload: dict):
        res = await messenger.send_message(peer, "raft/append_entries", payload)
        if isinstance(res, dict) and not res.get("error"):
            if res.get("term", 0) > self.current_term:
                self.current_term = res.get("term")
                self.state = "FOLLOWER"
                self.voted_for = None
                return
                
            if res.get("success"):
                self.next_index[peer] = payload["prev_log_index"] + len(payload["entries"]) + 1
                self.match_index[peer] = self.next_index[peer] - 1
                self.update_commit_index()
            else:
                self.next_index[peer] = max(1, self.next_index[peer] - 1)

    def update_commit_index(self):
        for n in range(self.commit_index + 1, len(self.log) + 1):
            if self.log[n-1]["term"] != self.current_term:
                continue
            match_count = 1
            for peer in self.peers:
                if self.match_index[peer] >= n:
                    match_count += 1
            if match_count > (len(self.peers) + 1) // 2:
                self.commit_index = n
                self.apply_logs()

    def apply_logs(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            command = self.log[self.last_applied - 1]["command"]
            logger.info(f"Node {self.node_id} applying command at index {self.last_applied}: {command}")
            if self.apply_callback:
                self.apply_callback(command)

    def handle_request_vote(self, term: int, candidate_id: str, last_log_index: int, last_log_term: int):
        if term > self.current_term:
            self.current_term = term
            self.state = "FOLLOWER"
            self.voted_for = None
            
        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}
            
        my_last_log_index = len(self.log)
        my_last_log_term = self.log[-1]["term"] if self.log else 0
        
        log_ok = (last_log_term > my_last_log_term) or (last_log_term == my_last_log_term and last_log_index >= my_last_log_index)
        
        if (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
            self.voted_for = candidate_id
            self.last_heartbeat = time.time()
            return {"term": self.current_term, "vote_granted": True}
            
        return {"term": self.current_term, "vote_granted": False}

    def handle_append_entries(self, term: int, leader_id: str, prev_log_index: int, prev_log_term: int, entries: list, leader_commit: int):
        if term > self.current_term:
            self.current_term = term
            self.state = "FOLLOWER"
            self.voted_for = None
            
        if term < self.current_term:
            return {"term": self.current_term, "success": False}
            
        self.last_heartbeat = time.time()
        self.leader_id = leader_id
        
        if prev_log_index > len(self.log):
            return {"term": self.current_term, "success": False}
            
        if prev_log_index > 0 and self.log[prev_log_index - 1]["term"] != prev_log_term:
            self.log = self.log[:prev_log_index - 1]
            return {"term": self.current_term, "success": False}
            
        for i, entry in enumerate(entries):
            idx = prev_log_index + i
            if idx < len(self.log):
                if self.log[idx]["term"] != entry["term"]:
                    self.log = self.log[:idx]
                    self.log.append(entry)
            else:
                self.log.append(entry)
                
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))
            self.apply_logs()
            
        return {"term": self.current_term, "success": True}

    async def propose(self, command: dict) -> bool:
        if self.state != "LEADER":
            return False
            
        self.log.append({"term": self.current_term, "command": command})
        target_index = len(self.log)
        
        # Trigger an immediate heartbeat to replicate faster
        asyncio.create_task(self.send_heartbeats())
        
        # Wait until it's committed (with a timeout)
        timeout = time.time() + 2.0
        while self.commit_index < target_index and time.time() < timeout:
            await asyncio.sleep(0.05)
            
        return self.commit_index >= target_index
