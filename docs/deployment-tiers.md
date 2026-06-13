# Deployment Tiers

## Solo

Single `pelicanqd` process with embedded sled storage. No replication. Suitable for:

- Local development
- CI/CD pipelines
- Low-throughput production (loss of the node is acceptable)

## Flock

Multi-node cluster using Raft consensus. Every queue is replicated across N nodes. Automatic leader election and failover. Suitable for:

- High-availability production
- Geographic distribution
- Rolling upgrades
