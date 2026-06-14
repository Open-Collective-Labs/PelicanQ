# System Overview

PelicanQ is a message queue daemon written in Rust. It serves messages through multiple protocols (HTTP, gRPC, MQTT) backed by a single embeddied engine with optional Raft-based clustering.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                     pelicanqd                             │
│                                                          │
│  ┌──────────────────┐  ┌──────────────────┐             │
│  │   HTTP Server     │  │   gRPC Server     │  ┌──────┐ │
│  │   (axum 0.7)      │  │   (tonic 0.11)    │  │ MQTT │ │
│  │   Port: 7070      │  │   Port: 7072      │  │ 1883 │ │
│  └────────┬─────────┘  └────────┬─────────┘  └──┬───┘ │
│           │                     │                │      │
│           └─────────┬───────────┴────────────────┘      │
│                     │                                    │
│            ┌────────▼────────┐                           │
│            │    AppEngine     │                           │
│            │   (Solo/Flock)   │                           │
│            └────────┬────────┘                           │
│                     │                                    │
│            ┌────────▼────────┐                           │
│            │  QueueManager   │                           │
│            │   (sled store)  │                           │
│            └─────────────────┘                           │
└──────────────────────────────────────────────────────────┘
```

## Components

- **HTTP Server** — Axum-based REST API for queue operations.
- **gRPC Server** — Tonic-based gRPC API with full RPC set including streaming consume.
- **MQTT Listener** — MQTT 3.1.1 protocol bridge using rumqttd.
- **AppEngine** — Dispatches to Solo (local) or Flock (Raft) implementation.
- **QueueManager** — Core engine managing sled-backed queue storage.

## Key Design Decisions

- **Shared state**: All protocol handlers share the same `Arc<AppState>` containing the engine. Messages published via any protocol are instantly available via all others.
- **Dual mode**: Solo mode for simplicity, Flock mode for HA — same binary, same API.
- **sled storage**: Embedded database eliminates the need for external dependencies like PostgreSQL or Cassandra.
