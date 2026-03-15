"""Load runner: async (asyncio) implementation."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from aiolimiter import AsyncLimiter

from .config import INSERTION_SENTINEL, QUERY_SENTINEL


def _insert_limiter(rows_per_sec: float, burst: int) -> AsyncLimiter:
    """Rate limiter for inserts matching Go's rate.Limiter(rate.Limit(rows_per_sec), burst).
    aiolimiter.AsyncLimiter(max_rate, time_period): allows max_rate per time_period, burst = max_rate.
    So max_rate=burst, time_period=burst/rows_per_sec gives rate=rows_per_sec and burst=burst.
    """
    if rows_per_sec <= 0 or burst <= 0:
        raise ValueError("rows_per_sec and burst must be positive")
    return AsyncLimiter(burst, burst / rows_per_sec)


def _query_limiter(queries_per_sec: float) -> AsyncLimiter:
    """Rate limiter for queries: queries_per_sec acquisitions per second (burst = queries_per_sec)."""
    if queries_per_sec <= 0:
        raise ValueError("queries_per_sec must be positive")
    return AsyncLimiter(queries_per_sec, 1.0)


from .producer import (
    SyncCounter,
    async_run_batch_producer,
    async_run_producer,
    build_one_batch,
)
from .progress import async_run_progress_logger

logger = logging.getLogger(__name__)

Record = tuple[str, str, str, bool]
Batch = list[Record]


def _worker_for_database(database: str):
    """Return the Worker class for the given database (postgres or clickhouse)."""
    if database == "postgres":
        from . import postgres
        return postgres.PostgresWorker
    from . import clickhouse
    return clickhouse.ClickHouseWorker


async def get_max_patient_counter_from_db_async(database: str, pgbouncer_enabled: bool = False) -> int:
    import os
    if database == "postgres":
        from .postgres import backend as pg_backend
        host = os.environ.get("POSTGRES_HOST") or "localhost"
        port = int(os.environ.get("POSTGRES_PORT") or "5432")
        db_name = "postgres1" if pgbouncer_enabled else "postgres"
        return await pg_backend.get_max_patient_counter_standalone(host, port, database=db_name)
    from .clickhouse import backend as ch_backend
    host = os.environ.get("CLICKHOUSE_HOST") or "clickhouse"
    port = int(os.environ.get("CLICKHOUSE_PORT") or "9000")
    return await ch_backend.get_max_patient_counter_standalone(host, port)


async def ensure_schema_from_db_async(database: str, pgbouncer_enabled: bool = False) -> None:
    import os
    if database == "postgres":
        from .postgres import backend as pg_backend
        host = os.environ.get("POSTGRES_HOST") or "localhost"
        port = int(os.environ.get("POSTGRES_PORT") or "5432")
        db_name = "postgres1" if pgbouncer_enabled else "postgres"
        await pg_backend.init_schema_standalone(host, port, database=db_name)
        return
    from .clickhouse import backend as ch_backend
    host = os.environ.get("CLICKHOUSE_HOST") or "clickhouse"
    port = int(os.environ.get("CLICKHOUSE_PORT") or "9000")
    await ch_backend.init_schema_standalone(host, port)


async def async_run_load(
    database: str,
    duration_sec: float,
    batch_size: int,
    workers: int,
    target_rps: int,
    queries_per_record: int,
    query_delay_sec: float = 0.0,
    producer_tasks: int = 2,
    total_records: int | None = None,
    ignore_select_errors: bool = False,
    duplicate_ratio: float = 0.25,
    shutdown_event: asyncio.Event | None = None,
    pgbouncer_enabled: bool = False,
) -> None:
    """Async load: asyncio queues, async workers and producers, single process. shutdown_event set on Ctrl+C for smooth exit."""
    num_workers = workers
    insertion_queue: asyncio.Queue[Batch | None] = asyncio.Queue(maxsize=max(num_workers * 32, target_rps * 4))
    query_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max(num_workers * 4, batch_size * num_workers * 4, target_rps * 4))
    inserted_lock = asyncio.Lock()
    inserted_shared: list[float] = [0, 0, 0, 0.0, 0, 0, 0]
    queries_lock = asyncio.Lock()
    queries_shared: list[float] = [0, 0.0, 0.0]
    progress_stop = asyncio.Event()
    run_start = time.perf_counter()

    use_fixed_count = total_records is not None
    if use_fixed_count and total_records <= 0:
        raise ValueError("total_records must be >= 1 when set")

    logger.info(
        "Connecting to %s (workers=%d, producers=%d, batch_size=%d, duration=%.1fs, target_rps=%d, queries_per_record=%d, query_delay=%.0fms, duplicate_ratio=%.2f)",
        database, num_workers, producer_tasks, batch_size, duration_sec, target_rps, queries_per_record, query_delay_sec * 1000, duplicate_ratio,
    )

    # Query rate limiter: cap query rate at target_rps * queries_per_record (tokens per second).
    target_query_rps = target_rps * queries_per_record if queries_per_record > 0 else 0
    query_rate_limiter: AsyncLimiter | None = None
    if target_query_rps > 0:
        query_rate_limiter = _query_limiter(target_query_rps)

    Worker = _worker_for_database(database)
    worker_ctx = Worker()
    await worker_ctx.setup_async(num_workers, target_rps, init_schema=True, pgbouncer_enabled=pgbouncer_enabled)
    worker_inst = worker_ctx.make_worker_async(
        insertion_queue, query_queue, inserted_lock, inserted_shared, batch_size, queries_per_record,
        pgbouncer_enabled=pgbouncer_enabled,
    )
    run_query_workers = queries_per_record > 0

    progress_task = asyncio.create_task(
        async_run_progress_logger(
            inserted_lock, inserted_shared, progress_stop,
            queries_lock, queries_shared,
            interval_sec=5.0,
        )
    )
    insert_tasks = [asyncio.create_task(worker_inst.run()) for _ in range(num_workers)]
    query_tasks: list[asyncio.Task[None]] = []
    if run_query_workers:
        if database == "postgres":
            from . import postgres
            for _ in range(num_workers):
                query_tasks.append(asyncio.create_task(
                    postgres.run_query_worker_postgres_async(
                        query_queue, worker_ctx.select_pool,
                        queries_lock, queries_shared,
                        queries_per_record, query_delay_sec, query_rate_limiter, ignore_select_errors,
                    )
                ))
        else:
            from . import clickhouse
            for _ in range(num_workers):
                query_tasks.append(asyncio.create_task(
                    clickhouse.run_query_worker_clickhouse_async(
                        query_queue, worker_ctx._resources_async.client_queue,
                        queries_lock, queries_shared,
                        queries_per_record, query_delay_sec, query_rate_limiter, ignore_select_errors,
                    )
                ))

    if use_fixed_count:
        next_id = SyncCounter(0)  # will set below
        max_counter = await worker_ctx.get_max_patient_counter_async()
        patient_start_base = max(0, max_counter + 1)
        next_id.value = patient_start_base
        logger.info("Fixed-count mode: producing %d records (base %d)", total_records, patient_start_base)
        producer_task = asyncio.create_task(
            async_run_producer(
                insertion_queue, num_workers, target_rps, batch_size,
                patient_start_base, next_id, num_workers, total_records, duplicate_ratio,
            )
        )
        if shutdown_event is not None:
            done, _ = await asyncio.wait(
                [producer_task, asyncio.create_task(shutdown_event.wait())],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not producer_task.done():
                producer_task.cancel()
                try:
                    await producer_task
                except asyncio.CancelledError:
                    pass
                logger.info("Shutdown requested (Ctrl+C), draining...")
                for _ in range(num_workers):
                    await insertion_queue.put(INSERTION_SENTINEL)
        else:
            await producer_task
    else:
        max_counter = await worker_ctx.get_max_patient_counter_async()
        patient_start_base = max(0, max_counter + 1)
        next_id = SyncCounter(patient_start_base)
        logger.info("Producers using atomic counter starting at %d (max in DB: %d)", patient_start_base, max_counter)
        producer_stop = shutdown_event if shutdown_event is not None else asyncio.Event()
        pre_built: list[Batch] = [
            build_one_batch(batch_size, patient_start_base, next_id, duplicate_ratio)
            for _ in range(producer_tasks)
        ]
        triggers: list[asyncio.Queue[None]] = [asyncio.Queue(maxsize=1) for _ in range(producer_tasks)]
        triggers[0].put_nowait(None)

        async def stop_after_duration() -> None:
            await asyncio.sleep(duration_sec)
            producer_stop.set()

        insert_rate_limiter = _insert_limiter(target_rps, batch_size)

        async def batch_producer(i: int) -> None:
            await async_run_batch_producer(
                insertion_queue, pre_built[i], batch_size, patient_start_base, next_id,
                triggers[i], triggers[(i + 1) % producer_tasks],
                producer_stop, duplicate_ratio,
                insert_rate_limiter,
            )

        duration_task = asyncio.create_task(stop_after_duration())
        producer_tasks_list = [asyncio.create_task(batch_producer(i)) for i in range(producer_tasks)]
        # Wait for either duration or Ctrl+C (shutdown_event)
        if shutdown_event is not None:
            done, _ = await asyncio.wait(
                [duration_task, asyncio.create_task(producer_stop.wait())],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not duration_task.done():
                duration_task.cancel()
                try:
                    await duration_task
                except asyncio.CancelledError:
                    pass
                logger.info("Shutdown requested (Ctrl+C), draining...")
        else:
            await duration_task
        await asyncio.gather(*producer_tasks_list)
        for _ in range(num_workers):
            await insertion_queue.put(INSERTION_SENTINEL)

    # Wait for insert workers to drain queue and exit
    await asyncio.gather(*insert_tasks)
    progress_stop.set()
    await asyncio.wait_for(asyncio.shield(progress_task), timeout=2.0)

    if run_query_workers:
        for _ in range(num_workers):
            await query_queue.put(QUERY_SENTINEL)
        await asyncio.gather(*query_tasks)

    await worker_ctx.teardown_async()

    run_end = time.perf_counter()
    total_inserted_final = int(inserted_shared[0])
    originals_final = int(inserted_shared[1])
    duplicates_final = int(inserted_shared[2])
    total_insert_latency_sec = inserted_shared[3]
    insert_statements_final = int(inserted_shared[4])
    queries_final = int(queries_shared[0])
    total_query_latency_sec = queries_shared[1]
    queries_failed_final = int(queries_shared[2])
    elapsed = run_end - run_start
    actual_rps = total_inserted_final / elapsed if elapsed > 0 else 0
    avg_insert_latency_ms = (total_insert_latency_sec / total_inserted_final * 1000.0) if total_inserted_final > 0 else 0.0
    avg_query_latency_ms = (total_query_latency_sec / queries_final * 1000.0) if queries_final > 0 else 0.0

    logger.info(
        "Run finished: %d rows inserted (%d original, %d duplicate) in %.2fs (%.1f rows/sec, target %d)",
        total_inserted_final, originals_final, duplicates_final, elapsed, actual_rps, target_rps,
    )
    print(f"Database: {database}")
    print(f"Duration: {elapsed:.2f}s | Workers: {num_workers} | Rows inserted: {total_inserted_final} ({originals_final} original, {duplicates_final} duplicate) | Insert statements: {insert_statements_final}")
    print(f"Actual insert rate: {actual_rps:.1f} rows/sec (target {target_rps})")
    if total_inserted_final > 0:
        print(f"Insert latency: avg {avg_insert_latency_ms:.2f} ms/row")
    if queries_final > 0:
        actual_query_rps = queries_final / elapsed if elapsed > 0 else 0
        print(f"Actual query rate: {actual_query_rps:.1f} queries/sec")
        print(f"Queries: {queries_final} executed, {queries_failed_final} failed | Query latency: avg {avg_query_latency_ms:.2f} ms per SELECT")
