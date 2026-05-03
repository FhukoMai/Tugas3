from prometheus_client import Counter, Histogram

REQUEST_COUNT = Counter(
    "request_count", "App Request Count",
    ["method", "endpoint", "http_status"]
)

REQUEST_LATENCY = Histogram(
    "request_latency_seconds", "Request latency",
    ["method", "endpoint"]
)

LOCK_REQUESTS = Counter("lock_requests", "Lock Requests", ["status"])
QUEUE_OPERATIONS = Counter("queue_operations", "Queue Operations", ["operation", "status"])
CACHE_HITS = Counter("cache_hits", "Cache Hits", ["status"])
CONSENSUS_STATE_CHANGES = Counter("consensus_state_changes", "Consensus State Changes", ["state"])
