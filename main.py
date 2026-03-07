#!/usr/bin/env python3
from __future__ import annotations

"""
HL7 messages load benchmark: PostgreSQL or ClickHouse at 1000 inserts/sec.
Uses dynamic patient generation and in-memory sample messages.
Multiprocessing: N processes each with 1 producer; total records = duration * rows_per_second divided uniformly.
"""
import argparse
import logging
import multiprocessing
import threading

from benchmark.runner import ensure_schema_from_db, get_max_patient_counter_from_db, run_load
from benchmark.progress import run_aggregated_progress_logger
from multiprocessing import Value

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _run_load_process(
    database: str,
    duration_sec: float,
    batch_size: int,
    batch_wait_sec: float,
    workers: int,
    rows_per_second: int,
    queries_per_record: int,
    query_delay_sec: float,
    processes: int,
    total_records: int,
    next_id: Value,
    patient_start_base: int,
    process_index: int,
    progress_queue: multiprocessing.Queue,
    start_barrier: multiprocessing.Barrier,
    ignore_select_errors: bool = False,
) -> None:
    """Target for one child process: run_load with fixed total_records and shared atomic counter (always 2+ processes)."""
    records_this_process = total_records // processes + (1 if process_index < total_records % processes else 0)
    rps_this_process = rows_per_second // processes + (1 if process_index < rows_per_second % processes else 0)
    if records_this_process <= 0:
        return
    run_load(
        database=database,
        duration_sec=duration_sec,
        batch_size=batch_size,
        batch_wait_sec=batch_wait_sec,
        workers=workers,
        target_rps=rps_this_process,
        queries_per_record=queries_per_record,
        query_delay_sec=query_delay_sec,
        producer_threads=1,
        total_records=records_this_process,
        progress_queue=progress_queue,
        process_index=process_index,
        start_barrier=start_barrier,
        ignore_select_errors=ignore_select_errors,
        next_id=next_id,
        patient_start_base=patient_start_base,
    )


def main() -> None:
    p = argparse.ArgumentParser(
        description="HL7 messages load driver — multiprocessing, 1 producer per process, fixed total records",
    )
    p.add_argument("--database", choices=["postgres", "clickhouse"], required=True)
    p.add_argument("--duration", type=float, default=60.0, help="Run duration in seconds (total records = duration * rows-per-second)")
    p.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Max rows per batch; consumer flushes on batch_size or batch-wait-sec, whichever first",
    )
    p.add_argument(
        "--batch-wait-sec",
        type=float,
        default=1.0,
        help="Max seconds to wait before flushing a partial batch (default: 1.0)",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Number of worker threads per process; each worker uses one connection from the pool",
    )
    p.add_argument(
        "--rows-per-second",
        type=int,
        default=1000,
        help="Target insert rate (rows/sec) across all processes; divided by --processes per process",
    )
    p.add_argument(
        "--processes",
        type=int,
        default=4,
        help="Number of processes (minimum 2); total records divided uniformly, 1 producer per process",
    )
    p.add_argument(
        "--queries-per-record",
        type=int,
        default=10,
        help="Number of primary-key queries to run per inserted record",
    )
    p.add_argument(
        "--query-delay",
        type=float,
        default=0.0,
        help="Fixed delay in milliseconds to wait before querying for each record (0 = no delay)",
    )
    p.add_argument(
        "--ignore-select-errors",
        action="store_true",
        help="Do not log when primary-key query returns != 1 row (avoids console slowdown)",
    )
    args = p.parse_args()

    if args.workers < 1:
        p.error("--workers must be >= 1")
    if args.processes < 2:
        p.error("--processes must be >= 2")
    if args.batch_wait_sec <= 0:
        p.error("--batch-wait-sec must be > 0")

    total_records = int(args.duration * args.rows_per_second)
    if total_records <= 0:
        p.error("total records (duration * rows-per-second) must be >= 1")

    # Single connection to compute max patient counter; shared atomic counter for all processes
    max_counter = get_max_patient_counter_from_db(args.database)
    patient_start_base = max(0, max_counter + 1)
    next_id = Value("q", patient_start_base)

    logger.info(
        "Total records %d over %d processes (1 producer per process); atomic counter from %d",
        total_records, args.processes, patient_start_base,
    )

    ensure_schema_from_db(args.database)

    progress_queue = multiprocessing.Queue()
    progress_stop = threading.Event()
    start_barrier = multiprocessing.Barrier(args.processes + 1)
    progress_thread = threading.Thread(
        target=run_aggregated_progress_logger,
        args=(progress_queue, args.processes, progress_stop),
        kwargs={"interval_sec": 5.0, "batch_size": args.batch_size},
        daemon=True,
    )

    procs = [
        multiprocessing.Process(
            target=_run_load_process,
            args=(
                args.database,
                args.duration,
                args.batch_size,
                args.batch_wait_sec,
                args.workers,
                args.rows_per_second,
                args.queries_per_record,
                args.query_delay / 1000.0,
                args.processes,
                total_records,
                next_id,
                patient_start_base,
                i,
                progress_queue,
                start_barrier,
                args.ignore_select_errors,
            ),
        )
        for i in range(args.processes)
    ]
    for proc in procs:
        proc.start()
    start_barrier.wait()
    progress_thread.start()
    for proc in procs:
        proc.join()

    progress_stop.set()
    progress_thread.join(timeout=6.0)
    failed = [i for i, proc in enumerate(procs) if proc.exitcode != 0]
    if failed:
        raise SystemExit(f"Process(es) {failed} exited with non-zero status")
    logger.info("All %d processes finished", args.processes)


if __name__ == "__main__":
    main()
