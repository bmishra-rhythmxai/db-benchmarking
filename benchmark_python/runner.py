"""Load runner: sync (threading) and async (asyncio) implementations."""
from __future__ import annotations

import asyncio
import logging
import queue
import threading
import time
from multiprocessing import Value
from typing import Any

from .config import INSERTION_SENTINEL, QUERY_SENTINEL
from .producer import (
    SyncCounter,
    async_run_batch_producer,
    async_run_producer,
    build_one_batch,
    run_batch_producer,
    run_producer,
)
from .progress import async_run_progress_logger, run_progress_logger, run_progress_reporter

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


def get_max_patient_counter_from_db(database: str) -> int:
    """Single connection, read max patient counter, close. No pool or schema init. Used by multiprocessing parent."""
    import os
    if database == "postgres":
        from .postgres import backend as pg_backend
        host = os.environ.get("POSTGRES_HOST") or "localhost"
        port = 5432
        return pg_backend.get_max_patient_counter_standalone(host, port)
    from .clickhouse import backend as ch_backend
    host = os.environ.get("CLICKHOUSE_HOST") or "clickhouse"
    port = int(os.environ.get("CLICKHOUSE_PORT") or "9000")
    return ch_backend.get_max_patient_counter_standalone(host, port)


def ensure_schema_from_db(database: str) -> None:
    """Run schema init once (single connection). Call from parent before spawning children to avoid deadlock."""
    import os
    if database == "postgres":
        from .postgres import backend as pg_backend
        host = os.environ.get("POSTGRES_HOST") or "localhost"
        port = 5432
        pg_backend.init_schema_standalone(host, port)
        return
    from .clickhouse import backend as ch_backend
    host = os.environ.get("CLICKHOUSE_HOST") or "clickhouse"
    port = int(os.environ.get("CLICKHOUSE_PORT") or "9000")
    ch_backend.init_schema_standalone(host, port)


async def get_max_patient_counter_from_db_async(database: str) -> int:
    import os
    if database == "postgres":
        from .postgres import backend_async as pg_backend
        host = os.environ.get("POSTGRES_HOST") or "localhost"
        port = 5432
        return await pg_backend.get_max_patient_counter_standalone(host, port)
    from .clickhouse import backend as ch_backend
    host = os.environ.get("CLICKHOUSE_HOST") or "clickhouse"
    port = int(os.environ.get("CLICKHOUSE_PORT") or "9000")
    return await asyncio.get_running_loop().run_in_executor(
        None,
        lambda: ch_backend.get_max_patient_counter_standalone(host, port),
    )


async def ensure_schema_from_db_async(database: str) -> None:
    import os
    if database == "postgres":
        from .postgres import backend_async as pg_backend
        host = os.environ.get("POSTGRES_HOST") or "localhost"
        port = 5432
        await pg_backend.init_schema_standalone(host, port)
        return
    from .clickhouse import backend as ch_backend
    host = os.environ.get("CLICKHOUSE_HOST") or "clickhouse"
    port = int(os.environ.get("CLICKHOUSE_PORT") or "9000")
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: ch_backend.init_schema_standalone(host, port))


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
) -> None:
    """Async load: asyncio queues, async workers and producers, single process."""
    num_workers = workers
    insertion_queue: asyncio.Queue[Batch | None] = asyncio.Queue(maxsize=max(num_workers * 32, target_rps * 4))
    query_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max(num_workers * 4, batch_size * num_workers * 4, target_rps * 4))
    inserted_lock = asyncio.Lock()
    inserted_shared: list[float] = [0, 0, 0, 0.0, 0, 0]
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

    Worker = _worker_for_database(database)
    worker_ctx = Worker()
    await worker_ctx.setup_async(num_workers, target_rps, init_schema=True)
    worker_inst = worker_ctx.make_worker_async(
        insertion_queue, query_queue, inserted_lock, inserted_shared, batch_size, queries_per_record
    )
    run_query_workers = queries_per_record > 0

    async def get_pending() -> int:
        return insertion_queue.qsize()

    progress_task = asyncio.create_task(
        async_run_progress_logger(
            inserted_lock, inserted_shared, progress_stop,
            queries_lock, queries_shared,
            interval_sec=5.0,
            get_pending=get_pending,
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
                        query_queue, worker_ctx.select_pool_async,
                        queries_lock, queries_shared,
                        queries_per_record, query_delay_sec, ignore_select_errors,
                    )
                ))
        else:
            from . import clickhouse
            for _ in range(num_workers):
                query_tasks.append(asyncio.create_task(
                    clickhouse.run_query_worker_clickhouse_async(
                        query_queue, worker_ctx._resources_async.client_queue,
                        worker_ctx._resources_async.loop,
                        queries_lock, queries_shared,
                        queries_per_record, query_delay_sec, ignore_select_errors,
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
        await producer_task
    else:
        max_counter = await worker_ctx.get_max_patient_counter_async()
        patient_start_base = max(0, max_counter + 1)
        next_id = SyncCounter(patient_start_base)
        logger.info("Producers using atomic counter starting at %d (max in DB: %d)", patient_start_base, max_counter)
        producer_stop = asyncio.Event()
        pre_built: list[Batch] = [
            build_one_batch(batch_size, patient_start_base, next_id, duplicate_ratio)
            for _ in range(producer_tasks)
        ]
        triggers: list[asyncio.Queue[None]] = [asyncio.Queue(maxsize=1) for _ in range(producer_tasks)]
        triggers[0].put_nowait(None)

        async def stop_after_duration() -> None:
            await asyncio.sleep(duration_sec)
            producer_stop.set()

        async def batch_producer(i: int) -> None:
            await async_run_batch_producer(
                insertion_queue, pre_built[i], batch_size, patient_start_base, next_id,
                triggers[i], triggers[(i + 1) % producer_tasks],
                producer_stop, duplicate_ratio,
            )

        duration_task = asyncio.create_task(stop_after_duration())
        producer_tasks_list = [asyncio.create_task(batch_producer(i)) for i in range(producer_tasks)]
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


def run_load(
    database: str,
    duration_sec: float,
    batch_size: int,
    workers: int,
    target_rps: int,
    queries_per_record: int,
    query_delay_sec: float = 0.0,
    producer_threads: int = 1,
    total_records: int | None = None,
    progress_queue: Any = None,
    process_index: int | None = None,
    start_barrier: Any = None,
    ignore_select_errors: bool = False,
    next_id: Any = None,
    patient_start_base: int | None = None,
    duplicate_ratio: float = 0.25,
) -> None:
    num_workers = workers
    # Queue holds full batches; size so producer and workers don't block.
    insertion_queue_max = max(num_workers * 8, num_workers * 32, target_rps * 4)
    insertion_queue: queue.Queue[Batch | None] = queue.Queue(maxsize=insertion_queue_max)
    # Insert workers push batch_size MRNs per flush; if query_queue is small, _flush() blocks on put()
    # and insert workers stall, capping throughput. Size so all workers can push at least 2 batches.
    query_queue_max = max(num_workers * 4, batch_size * num_workers * 4, target_rps * 4)
    query_queue: queue.Queue = queue.Queue(maxsize=query_queue_max)

    inserted_lock = threading.Lock()
    inserted_shared: list[float] = [0, 0, 0, 0.0, 0, 0]  # [total, originals, duplicates, total_insert_latency_sec, insert_statements, in_process]
    queries_lock = threading.Lock()
    queries_shared: list[float] = [0, 0.0, 0.0]  # [count, total_latency_sec, failed_count]
    progress_stop = threading.Event()
    run_start = time.perf_counter()

    use_fixed_count = total_records is not None
    if use_fixed_count and (next_id is None or patient_start_base is None):
        raise ValueError("next_id and patient_start_base are required when total_records is set")

    logger.info(
        "Connecting to %s (workers=%d, producers=%d, batch_size=%d, duration=%.1fs, target_rps=%d, queries_per_record=%d, query_delay=%.0fms, duplicate_ratio=%.2f)",
        database, num_workers, producer_threads, batch_size, duration_sec, target_rps, queries_per_record, query_delay_sec * 1000, duplicate_ratio,
    )

    Worker = _worker_for_database(database)
    worker_ctx = Worker()
    # When running as multiprocessing child, parent already ran schema init; skip to avoid deadlock
    skip_schema = progress_queue is not None and process_index is not None
    worker_ctx.setup(num_workers, target_rps, init_schema=not skip_schema)
    worker_inst = worker_ctx.make_worker(
        insertion_queue, query_queue, inserted_lock, inserted_shared, batch_size, queries_per_record
    )
    query_worker_run = worker_ctx.make_query_worker(
        query_queue, queries_lock, queries_shared, queries_per_record, query_delay_sec, ignore_select_errors
    )
    run_query_workers = queries_per_record > 0

    if use_fixed_count:
        logger.info("Fixed-count mode: producing %d records using atomic counter (base %d)", total_records, patient_start_base)

    # Wait for all processes to finish pool setup and prewarm before any start the load generator
    if start_barrier is not None:
        start_barrier.wait()

    use_aggregated_progress = progress_queue is not None and process_index is not None
    def get_pending() -> int:
        try:
            return insertion_queue.qsize()  # number of batches
        except (NotImplementedError, AttributeError):
            return 0

    if use_aggregated_progress:
        progress_thread = threading.Thread(
            target=run_progress_reporter,
            args=(process_index, progress_queue, inserted_lock, inserted_shared, queries_lock, queries_shared, insertion_queue, progress_stop),
            daemon=True,
        )
    else:
        progress_thread = threading.Thread(
            target=run_progress_logger,
            args=(inserted_lock, inserted_shared, progress_stop, queries_lock, queries_shared),
            kwargs={"interval_sec": 5.0, "get_pending": get_pending},
            daemon=True,
        )

    insert_worker_threads = [
        threading.Thread(target=worker_inst.run, daemon=True)
        for _ in range(num_workers)
    ]
    query_worker_threads = [
        threading.Thread(target=query_worker_run, daemon=True)
        for _ in range(num_workers)
    ] if run_query_workers else []

    for w in insert_worker_threads:
        w.start()
    for w in query_worker_threads:
        w.start()

    # Start progress only when warming up is done and producer is about to start
    progress_thread.start()

    if use_fixed_count:
        # One producer per process: produce exactly total_records at target_rps using shared atomic counter (full batches).
        run_producer(
            duration_sec=0.0,
            insertion_queue=insertion_queue,
            num_workers=num_workers,
            target_rps=target_rps,
            batch_size=batch_size,
            patient_start_base=patient_start_base,
            next_id=next_id,
            sentinels=num_workers,
            max_records=total_records,
            duplicate_ratio=duplicate_ratio,
        )
    else:
        # Duration-based: pre-build one batch per producer in order (before any threads), then round-robin: signal -> put batch -> signal next -> build next.
        max_counter = worker_ctx.get_max_patient_counter()
        local_base = max(0, max_counter + 1)
        local_next_id: Any = Value("q", local_base)
        logger.info("Producers using atomic counter starting at %d (max in DB: %d)", local_base, max_counter)

        pre_built: list[Batch] = [
            build_one_batch(batch_size, local_base, local_next_id, duplicate_ratio)
            for _ in range(producer_threads)
        ]

        producer_stop = threading.Event()

        def stop_after_duration() -> None:
            time.sleep(duration_sec)
            producer_stop.set()

        duration_thread = threading.Thread(target=stop_after_duration, daemon=True)
        duration_thread.start()

        triggers: list[queue.Queue[None]] = [queue.Queue(maxsize=1) for _ in range(producer_threads)]
        triggers[0].put(None)

        def batch_producer_target(producer_index: int) -> None:
            run_batch_producer(
                insertion_queue=insertion_queue,
                initial_batch=pre_built[producer_index],
                batch_size=batch_size,
                patient_start_base=local_base,
                next_id=local_next_id,
                recv_trigger=triggers[producer_index],
                send_trigger=triggers[(producer_index + 1) % producer_threads],
                stop_event=producer_stop,
                duplicate_ratio=duplicate_ratio,
            )

        producer_threads_list = [
            threading.Thread(target=batch_producer_target, args=(i,), daemon=True)
            for i in range(producer_threads)
        ]
        for p in producer_threads_list:
            p.start()
        for p in producer_threads_list:
            p.join()

        for _ in range(num_workers):
            insertion_queue.put(INSERTION_SENTINEL)

    insertion_queue.join()

    progress_stop.set()
    if use_aggregated_progress:
        # Send final stats so parent has latest combined totals
        with inserted_lock:
            ins = (inserted_shared[0], inserted_shared[1], inserted_shared[2], inserted_shared[3], inserted_shared[4], inserted_shared[5])
        try:
            queue_len = insertion_queue.qsize()
        except (NotImplementedError, AttributeError):
            queue_len = 0
        with queries_lock:
            q = (queries_shared[0], queries_shared[1], queries_shared[2])
        progress_queue.put((process_index, (*ins, queue_len, q[0], q[1], q[2])))
    progress_thread.join(timeout=1.0)

    for w in insert_worker_threads:
        w.join()

    if run_query_workers:
        for _ in range(num_workers):
            query_queue.put(QUERY_SENTINEL)
        for w in query_worker_threads:
            w.join()

    worker_ctx.teardown()

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
