import asyncio
import logging

logger = logging.getLogger(__name__)

class PBFTNode:
    # Simplified PBFT Implementation for Bonus A
    def __init__(self, node_id: str, peers: list, apply_callback):
        self.node_id = node_id
        self.peers = peers
        self.apply_callback = apply_callback
        
        self.view = 0
        self.primary = peers[0] if peers else node_id
        self.seq_num = 0
        
        # Logs
        self.pre_prepares = {}
        self.prepares = {}
        self.commits = {}
        
    async def propose(self, command):
        if self.node_id != self.primary:
            return False
            
        self.seq_num += 1
        # Broadcast pre-prepare
        # For this simplified version, we apply locally if no peers
        if not self.peers or len(self.peers) == 0:
            if self.apply_callback:
                self.apply_callback(command)
        return True

    def handle_pre_prepare(self, view, seq, digest):
        if view == self.view:
            self.pre_prepares[seq] = digest
            # Broadcast prepare
            
    def handle_prepare(self, view, seq, digest, node_id):
        if seq not in self.prepares:
            self.prepares[seq] = []
        self.prepares[seq].append(node_id)
        
        if len(self.prepares[seq]) > 2 * (len(self.peers) // 3):
            # Broadcast commit
            pass

    def handle_commit(self, view, seq, digest, node_id):
        if seq not in self.commits:
            self.commits[seq] = []
        self.commits[seq].append(node_id)
        
        if len(self.commits[seq]) > 2 * (len(self.peers) // 3):
            # Apply to state machine
            pass
