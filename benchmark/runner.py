"""Multithreaded load runner: threading here; workers provide setup, .run(), teardown."""
from __future__ import annotations

import logging
import queue
import threading
import time

from .config import QUERY_SENTINEL
from .producer import run_producer
from .progress import run_progress_logger

logger = logging.getLogger(__name__)

Record = tuple[str, str, str, bool]


def _worker_for_database(database: str):
    """Return the Worker class for the given database (postgres or clickhouse)."""
    if database == "postgres":
        from . import postgres
        return postgres.PostgresWorker
    from . import clickhouse
    return clickhouse.ClickHouseWorker


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
) -> None:
    num_workers = workers
    insertion_queue: queue.Queue[Record | None] = queue.Queue(maxsize=num_workers * 2)
    query_queue: queue.Queue = queue.Queue(maxsize=num_workers * 4)

    inserted_lock = threading.Lock()
    inserted_shared: list[float] = [0, 0, 0, 0.0]  # [total, originals, duplicates, total_insert_latency_sec]
    queries_lock = threading.Lock()
    queries_shared: list[float] = [0, 0.0]  # [count, total_latency_sec]
    progress_stop = threading.Event()
    run_start = time.perf_counter()

    logger.info(
        "Connecting to %s (workers=%d, batch_size=%d, batch_wait_sec=%.1f, duration=%.1fs, target_rps=%d, queries_per_record=%d, query_delay=%.0fms)",
        database, num_workers, batch_size, batch_wait_sec, duration_sec, target_rps, queries_per_record, query_delay_sec * 1000,
    )

    Worker = _worker_for_database(database)
    worker_ctx = Worker()
    worker_ctx.setup(num_workers, target_rps)
    worker_inst = worker_ctx.make_worker(
        insertion_queue, query_queue, inserted_lock, inserted_shared, batch_size, batch_wait_sec
    )
    query_worker_run = worker_ctx.make_query_worker(
        query_queue, queries_lock, queries_shared, queries_per_record, query_delay_sec
    )
    max_counter = worker_ctx.get_max_patient_counter()
    patient_start = max(0, max_counter + 1)
    logger.info("Producer starting from patient counter %d (max in DB: %d)", patient_start, max_counter)

    progress_thread = threading.Thread(
        target=run_progress_logger,
        args=(inserted_lock, inserted_shared, progress_stop, queries_lock, queries_shared),
        daemon=True,
    )
    progress_thread.start()

    insert_worker_threads = [
        threading.Thread(target=worker_inst.run, daemon=True)
        for _ in range(num_workers)
    ]
    query_worker_threads = [
        threading.Thread(target=query_worker_run, daemon=True)
        for _ in range(num_workers)
    ]

    for w in insert_worker_threads:
        w.start()
    for w in query_worker_threads:
        w.start()

    prod = threading.Thread(
        target=run_producer,
        args=(duration_sec, patient_count, insertion_queue, num_workers, target_rps, patient_start),
        daemon=True,
    )

    prod.start()
    prod.join()

    insertion_queue.join()

    progress_stop.set()
    progress_thread.join(timeout=1.0)

    for w in insert_worker_threads:
        w.join()

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
