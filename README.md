# Avito Backend Course

## Overview

Homework assignments for the course Backend development in the Master's program [Machine Learning in a Digital Product](https://www.hse.ru/en/ma/mldp/) (HSE University, Faculty of Computer Science & Avito)

## Project Structure

```
avito-backend-course/
├── migrations/
│   ├── V1__create_users_and_items.sql       # users and advertisements tables
│   ├── V2__create_moderation_results.sql    # moderation_results table
│   └── V4__rename_tables.sql                # rename sellers→users, ads→advertisements
├── src/
│   ├── api/
│   │   ├── moderation.py   # API endpoints
│   │   └── routers.py
│   ├── clients/
│   │   └── kafka.py        # Kafka producer (KafkaModerationProducer)
│   ├── ml/
│   │   └── model.py        # LogisticRegression model
│   ├── repositories/
│   │   ├── db.py                    # asyncpg pool management
│   │   ├── items.py                 # advertisements repository
│   │   ├── moderation_results.py    # moderation_results repository
│   │   └── users.py                 # users repository
│   ├── schemas/
│   │   ├── ad.py           # AdRequest / AdResponse
│   │   └── moderation.py   # AsyncModerationResponse / ModerationResultResponse
│   ├── services/
│   │   └── moderation.py   # ModerationService
│   ├── workers/
│   │   └── moderation_worker.py  # Kafka consumer with retry and DLQ
│   └── main.py
├── tests/
│   ├── test_moderation.py           # unit tests
│   └── test_kafka_integration.py    # integration tests (require running Kafka)
├── docker-compose.yaml
└── pyproject.toml
```

## Setup

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync --all-groups

# Install pre-commit hooks
pre-commit install
```

## Running the Project

### 1. Start infrastructure (PostgreSQL + Kafka/Redpanda)

```bash
docker compose up -d
```

### 2. Apply migrations

```bash
uv run pgmigrate -c "postgresql://avito:avito@localhost:5432/avito" -t latest migrate
```

### 3. Start the API server

```bash
DATABASE_URL=postgresql://avito:avito@localhost:5432/avito \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
uv run fastapi dev src/main.py
```

### 4. Start the moderation worker (separate terminal)

```bash
DATABASE_URL=postgresql://avito:avito@localhost:5432/avito \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
uv run python -m src.workers.moderation_worker
```

### Environment variables

| Variable | Value |
|---|---|
| `DATABASE_URL` | `postgresql://avito:avito@localhost:5432/avito` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |

### Useful links

- API: http://localhost:8000
- Swagger UI: http://localhost:8000/docs
- Redpanda Console (Kafka UI): http://localhost:8080

## API Endpoints

### POST /moderation/predict

Synchronous prediction — all ad data passed in the request body, no DB required.

**Request:**
```json
{
  "seller_id": 1,
  "is_verified_seller": false,
  "item_id": 1,
  "name": "iPhone 13",
  "description": "Good condition",
  "category": 1,
  "images_qty": 3
}
```

**Response:**
```json
{
  "is_violation": false,
  "probability": 0.12
}
```

### POST /moderation/simple_predict?item_id=1

Synchronous prediction — fetches ad data from DB by `item_id`.

**Response:**
```json
{
  "is_violation": false,
  "probability": 0.12
}
```

### POST /moderation/async_predict?item_id=1

Async prediction — sends the ad to Kafka for background processing by the worker.

**Response:**
```json
{
  "task_id": 42,
  "status": "pending",
  "message": "Moderation request accepted"
}
```

### GET /moderation/moderation_result/{task_id}

Returns the result of an async moderation task.

**Response:**
```json
{
  "task_id": 42,
  "status": "completed",
  "is_violation": false,
  "probability": 0.12,
  "error_message": null
}
```

## Async Moderation Flow

```
Client
  │
  ├─ POST /async_predict?item_id=1
  │       │
  │       ├─ creates moderation_results record (status=pending)
  │       └─ sends message to Kafka topic "moderation"
  │
Worker (moderation_worker.py)
  │
  ├─ consumes message from "moderation" topic
  ├─ fetches item from DB
  ├─ runs ML model
  ├─ updates moderation_results (status=completed/failed)
  │
  └─ on temporary error: retries up to 3 times (via "moderation" topic)
     on permanent error:  sends to DLQ topic "moderation_dlq"
  │
  ▼
Client
  └─ GET /moderation_result/{task_id}  →  { status: "completed", ... }
```

## Testing

```bash
# Unit tests
uv run pytest tests/test_moderation.py -v

# Integration tests (require running Kafka on localhost:9092)
uv run pytest tests/test_kafka_integration.py -v
```
