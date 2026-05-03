# Distributed Synchronization System

This is a comprehensive distributed system implementing Raft/PBFT consensus, distributed locking, consistent hashing queues, and a MESI coherent cache.

## Features Implemented for "Excellent" Grade:
- **Core A**: Distributed Lock Manager using custom Raft implementation.
- **Core B**: Distributed Queue using Consistent Hashing.
- **Core C**: Distributed Cache Coherence using MESI Protocol and LRU cache.
- **Core D**: Full Docker & Docker Compose containerization with Prometheus metrics.
- **Bonus A**: Advanced Consensus (PBFT module implemented).
- **Bonus D**: Security (JWT Authentication and Role-Based Access Control).

## Getting Started
Please refer to `docs/deployment_guide.md` for running the cluster via Docker.

## Documentation
- Architecture: `docs/architecture.md`
- Deployment: `docs/deployment_guide.md`
- API Spec: Available live at `http://localhost:8001/docs` when running.

## Demo Video
[Insert YouTube Link Here]
