import os
from pydantic_settings import BaseSettings
from typing import List

class Settings(BaseSettings):
    node_id: str = "node1"
    port: int = 8000
    peers: str = "" # Comma separated URLs
    secret_key: str = "supersecretkey_for_jwt"
    consensus_algo: str = "raft"
    
    @property
    def peer_list(self) -> List[str]:
        if not self.peers:
            return []
        return [p.strip() for p in self.peers.split(",") if p.strip()]

settings = Settings()
