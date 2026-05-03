## Sistem Sinkronisasi Terdistribusi
Tugas sistem terdistribusi yang mengimplementasikan konsensus Raft/PBFT, distributed locking, antrean consistent hashing, dan cache.

## Fitur yang Diimplementasikan

1. Core A: Manajer Kunci Terdistribusi (Distributed Lock Manager) menggunakan implementasi Raft kustom.

2. Core B: Antrean Terdistribusi menggunakan Consistent Hashing.

3. Core C: Koherensi Cache Terdistribusi menggunakan Protokol MESI dan cache LRU.

4. Core D: Kontainerisasi penuh dengan Docker & Docker Compose beserta metrik Prometheus.

5. Bonus A: Konsensus Lanjutan (modul PBFT diimplementasikan).

6. Bonus D: Keamanan (Autentikasi JWT dan Kontrol Akses Berbasis Peran / Role-Based Access Control).

## Documentation
- Architecture: `docs/architecture.md`
- Deployment: `docs/deployment_guide.md`

## Demo Video
[Insert YouTube Link Here]
