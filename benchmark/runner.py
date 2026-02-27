"""Multithreaded load runner: threading here; workers provide setup, .run(), teardown."""
from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any

from .config import INSERTION_SENTINEL, QUERY_SENTINEL
from .producer import run_producer
from .progress import run_progress_logger, run_progress_reporter

logger = logging.getLogger(__name__)

Record = tuple[str, str, str, bool]


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


def run_load(
    database: str,
    duration_sec: float,
    batch_size: int,
    batch_wait_sec: float,
    workers: int,
    patient_count: int,
    target_rps: int,
    queries_per_record: int,
    query_delay_sec: float = 0.0,
    producer_threads: int = 1,
    total_records: int | None = None,
    patient_start: int | None = None,
    progress_queue: Any = None,
    process_index: int | None = None,
    start_barrier: Any = None,
) -> None:
    num_workers = workers
    # Queue must hold enough for workers to fill batches without producer blocking; otherwise
    # producer blocks and workers block on long get() until batch_wait_sec. Use generous headroom.
    insertion_queue_max = max(num_workers * 8, batch_size * num_workers * 2, target_rps * 4)
    insertion_queue: queue.Queue[Record | None] = queue.Queue(maxsize=insertion_queue_max)
    # Insert workers push batch_size MRNs per flush; if query_queue is small, _flush() blocks on put()
    # and insert workers stall, capping throughput. Size so all workers can push at least 2 batches.
    query_queue_max = max(num_workers * 4, batch_size * num_workers * 4, target_rps * 4)
    query_queue: queue.Queue = queue.Queue(maxsize=query_queue_max)

    inserted_lock = threading.Lock()
    inserted_shared: list[float] = [0, 0, 0, 0.0]  # [total, originals, duplicates, total_insert_latency_sec]
    queries_lock = threading.Lock()
    queries_shared: list[float] = [0, 0.0]  # [count, total_latency_sec]
    progress_stop = threading.Event()
    run_start = time.perf_counter()

    use_fixed_count = total_records is not None
    if use_fixed_count and patient_start is None:
        raise ValueError("patient_start is required when total_records is set")

    logger.info(
        "Connecting to %s (workers=%d, producers=%d, batch_size=%d, batch_wait_sec=%.1f, duration=%.1fs, target_rps=%d, queries_per_record=%d, query_delay=%.0fms)",
        database, num_workers, producer_threads, batch_size, batch_wait_sec, duration_sec, target_rps, queries_per_record, query_delay_sec * 1000,
    )

    Worker = _worker_for_database(database)
    worker_ctx = Worker()
    # When running as multiprocessing child, parent already ran schema init; skip to avoid deadlock
    skip_schema = progress_queue is not None and process_index is not None
    worker_ctx.setup(num_workers, target_rps, init_schema=not skip_schema)
    worker_inst = worker_ctx.make_worker(
        insertion_queue, query_queue, inserted_lock, inserted_shared, batch_size, batch_wait_sec, queries_per_record
    )
    query_worker_run = worker_ctx.make_query_worker(
        query_queue, queries_lock, queries_shared, queries_per_record, query_delay_sec
    )
    run_query_workers = queries_per_record > 0

    if use_fixed_count:
        patient_start_base = patient_start  # type: ignore[assignment]
        logger.info("Fixed-count mode: producing %d records starting at patient counter %d", total_records, patient_start_base)
    else:
        max_counter = worker_ctx.get_max_patient_counter()
        patient_start_base = max(0, max_counter + 1)
        patient_start_stride = 10_000_000
        logger.info("Producers starting from patient counter %d (max in DB: %d)", patient_start_base, max_counter)

    # Wait for all processes to finish pool setup and prewarm before any start the load generator
    if start_barrier is not None:
        start_barrier.wait()

    use_aggregated_progress = progress_queue is not None and process_index is not None
    if use_aggregated_progress:
        progress_thread = threading.Thread(
            target=run_progress_reporter,
            args=(process_index, progress_queue, inserted_lock, inserted_shared, queries_lock, queries_shared, progress_stop),
            daemon=True,
        )
    else:
        progress_thread = threading.Thread(
            target=run_progress_logger,
            args=(inserted_lock, inserted_shared, progress_stop, queries_lock, queries_shared),
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
        # One producer per process: produce exactly total_records at target_rps
        run_producer(
            duration_sec=0.0,
            patient_count=patient_count,
            insertion_queue=insertion_queue,
            num_workers=num_workers,
            target_rps=target_rps,
            patient_start=patient_start_base,
            sentinels=num_workers,
            max_records=total_records,
        )
    else:
        # Split target_rps across producer threads so total ≈ target_rps
        per_producer = target_rps // producer_threads
        remainder = target_rps % producer_threads

        def producer_target(producer_index: int) -> None:
            rps = per_producer + (1 if producer_index < remainder else 0)
            if rps <= 0:
                return
            start = patient_start_base + producer_index * patient_start_stride
            run_producer(
                duration_sec,
                patient_count,
                insertion_queue,
                num_workers,
                target_rps=rps,
                patient_start=start,
                sentinels=0,
            )

        producer_threads_list = [
            threading.Thread(target=producer_target, args=(i,), daemon=True)
            for i in range(producer_threads)
        ]
        for p in producer_threads_list:
            p.start()
        for p in producer_threads_list:
            p.join()

        # Single producer puts sentinels; multi-producer: we put them here once
        for _ in range(num_workers):
            insertion_queue.put(INSERTION_SENTINEL)

    insertion_queue.join()

    progress_stop.set()
    if use_aggregated_progress:
        # Send final stats so parent has latest combined totals
        with inserted_lock:
            ins = (inserted_shared[0], inserted_shared[1], inserted_shared[2], inserted_shared[3])
        with queries_lock:
            q = (queries_shared[0], queries_shared[1])
        progress_queue.put((process_index, (*ins, q[0], q[1])))
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
    queries_final = int(queries_shared[0])
    total_query_latency_sec = queries_shared[1]
    elapsed = run_end - run_start
    actual_rps = total_inserted_final / elapsed if elapsed > 0 else 0
    avg_insert_latency_ms = (total_insert_latency_sec / total_inserted_final * 1000.0) if total_inserted_final > 0 else 0.0
    avg_query_latency_ms = (total_query_latency_sec / queries_final * 1000.0) if queries_final > 0 else 0.0

    logger.info(
        "Run finished: %d rows inserted (%d original, %d duplicate) in %.2fs (%.1f rows/sec, target %d)",
        total_inserted_final, originals_final, duplicates_final, elapsed, actual_rps, target_rps,
    )

    print(f"Database: {database}")
    print(f"Duration: {elapsed:.2f}s | Workers: {num_workers} | Rows inserted: {total_inserted_final} ({originals_final} original, {duplicates_final} duplicate)")
    print(f"Actual rate: {actual_rps:.1f} rows/sec (target {target_rps})")
    if total_inserted_final > 0:
        print(f"Insert latency: avg {avg_insert_latency_ms:.2f} ms/row")
    if queries_final > 0:
        print(f"Queries: {queries_final} executed (avg latency {avg_query_latency_ms:.2f} ms)")
