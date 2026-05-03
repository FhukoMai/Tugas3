import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from typing import Dict, Any

from src.utils.config import settings
from src.utils.security import create_token, verify_token, require_role
from src.utils import metrics
from src.consensus.raft import RaftNode
from src.consensus.pbft import PBFTNode
from src.nodes.lock_manager import LockManager
from src.nodes.queue_node import QueueNode
from src.nodes.cache_node import MESICacheNode
from src.communication.message_passing import messenger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

lock_manager = LockManager()

def apply_to_state_machine(command: dict):
    metrics.CONSENSUS_STATE_CHANGES.labels(state="applied").inc()
    if command.get("target") == "lock":
        lock_manager.apply_command(command)

if settings.consensus_algo == "raft":
    consensus_node = RaftNode(settings.node_id, settings.peer_list, apply_to_state_machine)
else:
    consensus_node = PBFTNode(settings.node_id, settings.peer_list, apply_to_state_machine)

queue_node = QueueNode(settings.node_id, [settings.node_id] + settings.peer_list)
cache_node = MESICacheNode(settings.node_id)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info(f"Starting node {settings.node_id} on port {settings.port}")
    if isinstance(consensus_node, RaftNode):
        await consensus_node.start()
    yield
    # Shutdown
    if isinstance(consensus_node, RaftNode):
        await consensus_node.stop()
    await messenger.close()

app = FastAPI(title=f"Distributed Sync Node: {settings.node_id}", lifespan=lifespan)

@app.middleware("http")
async def record_metrics(request: Request, call_next):
    with metrics.REQUEST_LATENCY.labels(method=request.method, endpoint=request.url.path).time():
        response = await call_next(request)
        metrics.REQUEST_COUNT.labels(method=request.method, endpoint=request.url.path, http_status=response.status_code).inc()
        return response

@app.get("/metrics")
async def get_metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/auth/login")
async def login(username: str):
    # Simplified login
    role = "admin" if username == "admin" else "user"
    token = create_token(username, role)
    return {"access_token": token, "token_type": "bearer"}

# --- Raft RPCs ---
@app.post("/raft/request_vote")
async def raft_request_vote(payload: dict):
    if not isinstance(consensus_node, RaftNode):
        return {"error": "Not running Raft"}
    res = consensus_node.handle_request_vote(
        payload["term"], payload["candidate_id"],
        payload["last_log_index"], payload["last_log_term"]
    )
    return res

@app.post("/raft/append_entries")
async def raft_append_entries(payload: dict):
    if not isinstance(consensus_node, RaftNode):
        return {"error": "Not running Raft"}
    res = consensus_node.handle_append_entries(
        payload["term"], payload["leader_id"],
        payload["prev_log_index"], payload["prev_log_term"],
        payload["entries"], payload["leader_commit"]
    )
    return res

@app.get("/raft/state")
async def get_raft_state():
    if not isinstance(consensus_node, RaftNode):
        return {"error": "Not running Raft"}
    return {
        "node_id": consensus_node.node_id,
        "state": consensus_node.state,
        "term": consensus_node.current_term,
        "commit_index": consensus_node.commit_index,
        "last_applied": consensus_node.last_applied,
        "leader_id": consensus_node.leader_id,
        "log_len": len(consensus_node.log)
    }

@app.get("/debug/locks")
async def debug_locks():
    return lock_manager.locks

# --- Lock API ---
@app.post("/lock/acquire", dependencies=[Depends(verify_token)])
async def acquire_lock(lock_id: str, client_id: str, lock_type: str = "exclusive"):
    metrics.LOCK_REQUESTS.labels(status="acquire").inc()
    command = {"target": "lock", "action": "acquire", "lock_id": lock_id, "client_id": client_id, "lock_type": lock_type}
    if hasattr(consensus_node, 'propose'):
        success = await consensus_node.propose(command)
        if success:
            return {"status": "processing"}
        return {"status": "redirect", "leader": getattr(consensus_node, 'leader_id', 'unknown')}
    return {"error": "Consensus not supported"}

@app.post("/lock/release", dependencies=[Depends(verify_token)])
async def release_lock(lock_id: str, client_id: str):
    metrics.LOCK_REQUESTS.labels(status="release").inc()
    command = {"target": "lock", "action": "release", "lock_id": lock_id, "client_id": client_id}
    if hasattr(consensus_node, 'propose'):
        success = await consensus_node.propose(command)
        if success:
            return {"status": "processing"}
        return {"status": "redirect", "leader": getattr(consensus_node, 'leader_id', 'unknown')}
    return {"error": "Consensus not supported"}

@app.get("/lock/status/{lock_id}")
async def get_lock_status(lock_id: str):
    return {"lock": lock_manager.get_lock_status(lock_id)}

# --- Queue API ---
@app.post("/queue/enqueue", dependencies=[Depends(require_role("admin"))])
async def queue_enqueue(queue_name: str, payload: dict):
    metrics.QUEUE_OPERATIONS.labels(operation="enqueue", status="received").inc()
    success = queue_node.enqueue(queue_name, payload)
    if success:
        return {"status": "enqueued"}
    # In a real app we might proxy this to the correct node
    target = queue_node.ring.get_node(queue_name)
    return {"status": "redirect", "target": target}

@app.get("/queue/dequeue")
async def queue_dequeue(queue_name: str):
    metrics.QUEUE_OPERATIONS.labels(operation="dequeue", status="received").inc()
    msg = queue_node.dequeue(queue_name)
    if msg:
        return {"status": "success", "message": msg}
    target = queue_node.ring.get_node(queue_name)
    if target != settings.node_id:
        return {"status": "redirect", "target": target}
    return {"status": "empty"}

# --- Cache API ---
@app.get("/cache/{key}")
async def cache_read(key: str):
    val = cache_node.read(key)
    if val is not None:
        metrics.CACHE_HITS.labels(status="hit").inc()
        return {"key": key, "value": val}
        
    # Simulate MESI Bus Read: ask peers if they have it
    for peer in settings.peer_list:
        res = await messenger.send_message(peer, "cache/bus/read", {"key": key})
        if isinstance(res, dict) and res.get("value") is not None:
            cache_node.states[key] = 'S'
            cache_node.cache.put(key, res.get("value"))
            metrics.CACHE_HITS.labels(status="bus_hit").inc()
            return {"key": key, "value": res.get("value"), "source": peer}

    metrics.CACHE_HITS.labels(status="miss").inc()
    return {"error": "not found"}

@app.post("/cache/{key}")
async def cache_write(key: str, payload: dict):
    cache_node.write(key, payload.get("value"))
    
    # Simulate MESI Bus Invalidate: tell peers to drop cache
    for peer in settings.peer_list:
        asyncio.create_task(messenger.send_message(peer, "cache/bus/invalidate", {"key": key}))
        
    return {"status": "written"}

@app.post("/cache/bus/invalidate")
async def cache_bus_invalidate(payload: dict):
    cache_node.handle_bus_invalidate(payload.get("key"))
    return {"status": "invalidated"}

@app.post("/cache/bus/read")
async def cache_bus_read(payload: dict):
    val = cache_node.handle_bus_read(payload.get("key"))
    return {"value": val}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.nodes.base_node:app", host="0.0.0.0", port=settings.port, reload=True)
