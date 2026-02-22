# PostgreSQL vs ClickHouse benchmark (hl7_messages)

Benchmark comparison for the `hl7_messages` table: `patient_id` (string), `message_type` (string), `message` (JSON). The load driver targets **1000 inserts per second** using dynamic patient generation and in-memory sample messages. It is **multithreaded**: a producer enqueues batches at the target rate and a pool of worker threads performs inserts in parallel. Each worker uses one connection from a **prewarmed** connection pool; pool size is set by `--workers`.

## Table schema

- **patient_id** – string  
- **message_type** – string  
- **message** – JSON (JSONB in Postgres, String in ClickHouse)  
- **created_at** – optional timestamp (auto in both)

## Project layout

- `main.py` – entry point (argparse + run)
- `benchmark/` – config, Postgres/ClickHouse backends, runner
- `benchmark/patient_generator.py` – dynamic patient and message payload generation

## Quick start

### 1. Start databases and load-runner

```bash
docker compose up -d --build
```

- **PostgreSQL**: host `postgres`, port 5432 (user/password: `benchmark` / `benchmark`, db: `benchmark`). Resource limits: 2 CPUs, 2 GB memory.
- **ClickHouse**: host `clickhouse`, port **9000** (native TCP; user/password: `benchmark` / `benchmark`, db: `benchmark`). Resource limits: 2 CPUs, 2 GB memory.
- **load-runner**: Python container with repo mounted at `/app`; dependencies installed in image.

### 2. Run the load driver

**From inside the load-runner container** (hosts `postgres` / `clickhouse` resolve automatically):

```bash
# One-off run
docker compose run --rm load-runner python main.py --database postgres --duration 60

# Or exec into the long-running container
docker compose exec load-runner python main.py --database clickhouse --duration 30 --batch-size 200
```

**From your local machine** (connect to localhost; set `BENCHMARK_HOST` so the driver uses `localhost`):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
BENCHMARK_HOST=localhost python main.py --database postgres --duration 60
```

### 3. Parameters

| Option                        | Default | Description                                                                 |
|-------------------------------|---------|-----------------------------------------------------------------------------|
| `--database`                  | (required) | `postgres` or `clickhouse`                                              |
| `--duration`                  | 60      | Run duration in seconds                                                    |
| `--batch-size`                | 100     | Rows per batch                                                             |
| `--workers`                   | 5       | Number of worker threads; each worker uses one connection from the pool               |
| `--patient-count`             | 1000    | Number of patient IDs to generate for load                                |

Target rate is fixed at 1000 rows/sec. Host and port are derived from `--database` (service names `postgres` / `clickhouse` when running in Docker; set `BENCHMARK_HOST=localhost` for local runs). Username and password are hardcoded as `benchmark` / `benchmark`.

## Stopping

```bash
docker compose down
```
