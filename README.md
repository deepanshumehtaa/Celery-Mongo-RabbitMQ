# Background Task Processor (Celery, RabbitMQ & MongoDB)

A highly structured, production-ready Python background task processing server built using `uv`. It utilizes **Celery** as the task manager, **RabbitMQ** as the message broker with multiple priority queues, and **MongoDB** as the storage engine (tracking task states, storing responses, and enforcing distributed locks).

---

## Key Features

1. **Priority Queues**:
   - **`high_priority`**: Heavy/time-critical operations.
   - **`default`**: Standard background tasks.
   - **`low_priority`**: Non-urgent tasks (e.g., archiving, analytics).
   - Tasks are dispatched dynamically using a prefetch limit (`worker_prefetch_multiplier=1`) to prevent workers from hogging tasks and ensure higher-priority tasks are picked up first.
2. **Task State & Response Logging**:
   - Every task state change (`STARTED`, `RETRYING`, `SUCCESS`, `FAILED`, `SKIPPED`) is logged automatically to the MongoDB `task_logs` collection.
   - Task output results are stored in a separate collection (`task_responses`) for clean isolation of task state history and actual payloads.
3. **Idempotency & Duplicate Prevention (Distributed Lock)**:
   - Utilizes MongoDB-based distributed locking (`task_locks` collection).
   - Generates a stable, unique lock key based on the task name and its serialized input arguments.
   - Restricts concurrent execution of identical tasks across all worker instances.
   - Automatically detects and re-acquires expired locks.
   - Performs an idempotency check: if the task (or a duplicate) has already run to success, the worker skips execution and returns the cached result.
4. **Reliable Retries with Exponential Backoff**:
   - Captures failures and schedules up to 3 retries.
   - Uses exponential backoff: \(2^{\text{attempt}}\) seconds delay (e.g., 2s, 4s, 8s).
   - Tracks retry attempts and updates status to `RETRYING` in MongoDB.

---

## Directory Structure

```
Celery-Mongo-RabbitMQ/
├── pyproject.toml              # UV project setup & dependencies
├── .env.example                # Configuration template
├── .env                        # Local configurations
├── README.md                   # Documentation
├── main.py                     # Producer script to dispatch and verify tasks
├── test_app.py                 # Fully mock-based unit tests
└── celery_app/
    ├── __init__.py             # Package exports (celery_app)
    ├── celery.py               # Celery app instance and lifecycle hooks
    ├── config.py               # Pydantic Settings configuration loading
    ├── database.py             # Singleton MongoDB client utility
    ├── lock.py                 # Distributed lock context manager
    └── tasks.py                # Base task subclass and task definitions
```

---

## Setup & Running

### 1. Prerequisites
Ensure you have `uv` installed. You will also need running instances of **RabbitMQ** and **MongoDB**.

If you use Docker, you can quickly spin them up:
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
docker run -d --name mongodb -p 27017:27017 -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=admin_password mongo:latest
```

### 2. Configure Environment
Copy `.env.example` to `.env` and update credentials:
```bash
cp .env.example .env
```

### 3. Install Dependencies
Sync project dependencies using `uv`:
```bash
uv sync
```

### 4. Running the Celery Worker
Start the Celery worker and bind it to consume from all three priority queues:
```bash
uv run celery -A celery_app.celery worker -Q high_priority,default,low_priority --loglevel=info
```

### 5. Running the Client Verification Script
In a separate terminal, execute the client script to dispatch priority tasks, duplicate tasks, and a failing task. It will poll MongoDB to verify state transitions and responses:
```bash
uv run python main.py
```

### 6. Monitoring Tasks with Flower
Start Flower to monitor workers and task execution progress in real-time:
```bash
uv run celery -A celery_app.celery flower
```
Once started, open your web browser and navigate to:
[http://localhost:5555](http://localhost:5555)

### 7. Interactive Enqueue & Unittest Options
Launch `test_app.py` interactively to select queue priorities (`high_priority`, `default`, `low_priority`), enter custom parameter payloads, or execute the test suite:
```bash
uv run python test_app.py
```
Or run automated tests non-interactively:
```bash
uv run python test_app.py --auto
```

### 8. Clearing MongoDB Collections
To wipe all documents from all 3 MongoDB collections (`task_logs`, `task_responses`, `task_locks`), run:
```bash
uv run python clear_db.py
```

### 9. Triggering Custom Tasks via CLI
Use the `trigger.py` utility to dynamically queue custom tasks to specific queues:

* **High Priority Task**:
  ```bash
  uv run python trigger.py --task high --data '{"userId": 1, "action": "export"}'
  ```
* **Default Task**:
  ```bash
  uv run python trigger.py --task default --data '{"status": "active"}'
  ```
* **Low Priority Task**:
  ```bash
  uv run python trigger.py --task low --data '{"clean": "all"}'
  ```
* **Failing Task (testing retries)**:
  ```bash
  uv run python trigger.py --task fail --fail-until 3
  ```



