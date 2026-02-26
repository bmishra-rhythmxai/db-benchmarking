#!/usr/bin/env python3
"""
HL7 messages load benchmark: PostgreSQL or ClickHouse at 1000 inserts/sec.
Uses dynamic patient generation and in-memory sample messages.
Multithreaded: producer enqueues records at target rate; workers batch (1s or batch_size) and insert in parallel.
"""
import argparse
import logging

from benchmark.runner import run_load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(
        description="HL7 messages load driver — 1000 inserts/sec, multithreaded, dynamic patient data",
    )
    p.add_argument("--database", choices=["postgres", "clickhouse"], required=True)
    p.add_argument("--duration", type=float, default=60.0, help="Run duration in seconds")
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
        help="Number of worker threads; each worker uses one connection from the pool",
    )
    p.add_argument(
        "--patient-count",
        type=int,
        default=1000,
        help="Number of patient IDs to generate for load",
    )
    p.add_argument(
        "--rows-per-second",
        type=int,
        default=1000,
        help="Target insert rate (rows/sec); producer enqueues records at this rate",
    )
    p.add_argument(
        "--producers",
        type=int,
        default=1,
        help="Number of producer threads to generate load (default 1); use more if a single producer cannot sustain target RPS",
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
    args = p.parse_args()

    if args.workers < 1:
        p.error("--workers must be >= 1")
    if args.producers < 1:
        p.error("--producers must be >= 1")
    if args.batch_wait_sec <= 0:
        p.error("--batch-wait-sec must be > 0")

    run_load(
        database=args.database,
        duration_sec=args.duration,
        batch_size=args.batch_size,
        batch_wait_sec=args.batch_wait_sec,
        workers=args.workers,
        patient_count=args.patient_count,
        target_rps=args.rows_per_second,
        queries_per_record=args.queries_per_record,
        query_delay_sec=args.query_delay / 1000.0,
        producer_threads=args.producers,
    )


if __name__ == "__main__":
    main()
