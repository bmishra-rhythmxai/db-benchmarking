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

Batch = list[tuple[str, str, str]]


def run_load(
    database: str,
    duration_sec: float,
    batch_size: int,
    workers: int,
    patient_count: int,
    target_rps: int,
    queries_per_record: int,
    query_delay_sec: float = 0.0,
) -> None:
    num_workers = workers
    insertion_queue: queue.Queue[Batch | None] = queue.Queue(maxsize=num_workers * 2)
    query_queue: queue.Queue = queue.Queue(maxsize=num_workers * 4)

    inserted_lock = threading.Lock()
    inserted_shared: list[int] = [0]
    queries_lock = threading.Lock()
    queries_shared: list[int] = [0]
    progress_stop = threading.Event()
    run_start = time.perf_counter()

    logger.info(
        "Connecting to %s (workers=%d, batch_size=%d, duration=%.1fs, target_rps=%d, queries_per_record=%d, query_delay=%.0fms)",
        database, num_workers, batch_size, duration_sec, target_rps, queries_per_record, query_delay_sec * 1000,
    )

    if database == "postgres":
        from . import postgres

        Worker = postgres.PostgresWorker
        resources = Worker.setup(num_workers, target_rps)
        worker_inst = Worker.make_worker(
            insertion_queue, query_queue, resources, inserted_lock, inserted_shared
        )
        query_worker_run = Worker.make_query_worker(
            query_queue, resources, queries_lock, queries_shared, queries_per_record, query_delay_sec
        )
    else:
        from . import clickhouse

        Worker = clickhouse.ClickHouseWorker
        resources = Worker.setup(num_workers, target_rps)
        worker_inst = Worker.make_worker(
            insertion_queue, query_queue, resources, inserted_lock, inserted_shared
        )
        query_worker_run = Worker.make_query_worker(
            query_queue, resources, queries_lock, queries_shared, queries_per_record, query_delay_sec
        )

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
        args=(duration_sec, batch_size, patient_count, insertion_queue, num_workers, target_rps),
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

    Worker.teardown(resources)

    run_end = time.perf_counter()
    total_inserted_final = inserted_shared[0]
    elapsed = run_end - run_start
    actual_rps = total_inserted_final / elapsed if elapsed > 0 else 0
    logger.info(
        "Run finished: %d rows in %.2fs (%.1f rows/sec, target %d)",
        total_inserted_final, elapsed, actual_rps, target_rps,
    )
    print(f"Database: {database}")
    print(f"Duration: {elapsed:.2f}s | Workers: {num_workers} | Rows: {total_inserted_final}")
    print(f"Actual rate: {actual_rps:.1f} rows/sec (target {target_rps})")
