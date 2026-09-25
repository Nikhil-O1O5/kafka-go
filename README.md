# kafka-go

A Go project for learning Kafka patterns, built incrementally from basic producer-consumer to CDC with Debezium.

## What this covers

| Phase | Pattern | Key concept |
|-------|---------|-------------|
| 1 | Basic producer-consumer | Kafka fundamentals |
| 2 | Exactly-once semantics | Manual offset tracking, in-memory state map |
| 3 | Multi-partition | Per-partition state, isolated commit tracking |
| 4 | Transactional outbox | Reliable DB → Kafka bridge, HTTP API |
| 5 | CDC with Debezium | WAL-based event capture, Protobuf + Schema Registry |

## Architecture

```
POST /orders
    │
    ▼
Postgres (orders table)
    │                       │
    │ outbox poller          │ Debezium reads WAL
    ▼                       ▼
local_topic             cdc.public.orders
    │                       │
    ▼                       ▼
KafkaConsumer           CDCConsumer
(EventService)          (CDCService)
```

Two parallel flows run simultaneously:
- **Outbox flow** — HTTP insert → outbox table → poller produces to `local_topic` → consumer deduplicates via `events` table
- **CDC flow** — HTTP insert → Postgres WAL → Debezium → `cdc.public.orders` → consumer deserializes Protobuf via Schema Registry

## Stack

- **Go** — producer, consumer, HTTP API
- **Kafka** — message broker (3 partitions on `local_topic`)
- **Postgres** — primary store with `wal_level=logical` for CDC
- **Debezium** — Kafka Connect connector watching Postgres WAL
- **Schema Registry** — Protobuf schema contract between Debezium and Go consumer
- **Zookeeper** — Kafka coordination

## Prerequisites

- Docker + Docker Compose
- Go 1.22+
- `protoc` + `protoc-gen-go` (for regenerating proto)

## Running

```bash
# Start infrastructure
docker compose -f kafka.yaml up -d

# Build and run
make app

# Or run directly
go run ./cmd/main.go
```

## Create an order

```bash
curl -X POST http://localhost:8080/orders -d '{"item": "book"}'
```

You should see both flows fire in the logs:
- `event inserted` — outbox path processed the Kafka message
- `cdc: order created` — Debezium picked up the WAL change

## Makefile targets

```bash
make app          # build and run
make build-app    # build only
make proto-gen    # regenerate Go code from proto/orders.proto
make test-app-race # run tests with race detector
```

## Key files

```
cmd/main.go                          # wires everything together
internal/
  consumer/consumer.go               # outbox Kafka consumer (manual offset tracking)
  consumer/cdc_consumer.go           # CDC consumer (Schema Registry + Protobuf)
  debezium/connector.go              # registers Debezium connector on startup
  service/outbox_service.go          # polls outbox table, produces to Kafka
  service/cdc_service.go             # handles CDC envelope events
  repo/db.go                         # Postgres connection + schema
proto/orders.proto                   # Protobuf schema matching Debezium's envelope
kafka.yaml                           # Docker Compose for full stack
Dockerfile.debezium                  # Debezium image with Confluent Protobuf JARs
```

## Exactly-once semantics (outbox consumer)

The outbox consumer tracks per-partition offset state in memory. Each message is marked complete after processing. A background ticker commits the highest contiguous completed offset every 15 seconds — so an in-flight slow message blocks commits for everything after it, preventing gaps.

## CDC schema contract

Debezium auto-registers a Protobuf schema in Schema Registry when it first captures a change. The schema in `proto/orders.proto` must match what Debezium registers exactly — field numbers, nesting, and message names. Every Kafka message carries a 5-byte Schema Registry header (magic byte + schema ID) that the consumer uses to look up and validate the schema before deserializing.
