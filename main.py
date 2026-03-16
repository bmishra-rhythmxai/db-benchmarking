#!/usr/bin/env python3
from __future__ import annotations

"""
HL7 messages load benchmark: PostgreSQL or ClickHouse (async, single process).
Uses asyncio for producers, insert workers, and query workers; no multiprocessing or threads.
"""
import argparse
import asyncio
import logging
import signal
from datetime import datetime

from benchmark_python.runner import (
    run_load,
    ensure_schema_from_db,
    get_max_patient_counter_from_db,
)


class MillisFormatter(logging.Formatter):
    """Format log timestamps with millisecond precision (match benchmark-go)."""

    def __init__(self, fmt=None, datefmt=None, style="%"):
        super().__init__(fmt, datefmt, style)

    def formatTime(self, record, datefmt=None):
        ct = datetime.fromtimestamp(record.created)
        datefmt = datefmt or self.datefmt or "%Y-%m-%d %H:%M:%S"
        t = ct.strftime(datefmt)
        msecs = getattr(record, "msecs", (record.created % 1) * 1000)
        return t + f".{int(msecs):03d}"


def _configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(
        MillisFormatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    logging.basicConfig(level=logging.INFO, handlers=[handler])


_configure_logging()
logger = logging.getLogger(__name__)


async def main_async() -> None:
    p = argparse.ArgumentParser(
        description="HL7 messages load driver — async, single process, configurable producers",
    )
    p.add_argument("--database", choices=["postgres", "clickhouse"], required=True)
    p.add_argument("--pgbouncer-enabled", action="store_true", help="Use PgBouncer with postgres1/postgres2 aliases and pipeline mode for inserts (postgres only)")
    p.add_argument("--duration", type=float, default=60.0, help="Run duration in seconds (total records = duration * rows-per-second)")
    p.add_argument("--batch-size", type=int, default=100, help="Rows per batch; producers emit full batches only")
    p.add_argument("--duplicate-ratio", type=float, default=0.25, help="Ratio of duplicate records (0-1, default 0.25)")
    p.add_argument("--workers", type=int, default=5, help="Number of insert/query worker coroutines")
    p.add_argument("--rows-per-second", type=int, default=1000, help="Target insert rate (rows/sec)")
    p.add_argument("--producers", type=int, default=2, help="Number of producer coroutines (minimum 2 for duration-based)")
    p.add_argument("--queries-per-record", type=int, default=10, help="Primary-key queries per inserted record")
    p.add_argument("--query-delay", type=float, default=0.0, help="Fixed delay in ms before querying each record (0 = no delay)")
    p.add_argument("--ignore-select-errors", action="store_true", help="Do not log when primary-key query returns != 1 row")
    args = p.parse_args()

    if args.workers < 1:
        p.error("--workers must be >= 1")
    if args.producers < 2:
        p.error("--producers must be >= 2")
    if not (0 <= args.duplicate_ratio <= 1):
        p.error("--duplicate-ratio must be between 0 and 1")

    total_records = int(args.duration * args.rows_per_second)
    if total_records <= 0:
        p.error("total records (duration * rows-per-second) must be >= 1")

    logger.info(
        "Total records %d (duration-based); %d producer coroutines, %d workers",
        total_records, args.producers, args.workers,
    )

    shutdown_event = asyncio.Event()
    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, shutdown_event.set)
    except (NotImplementedError, OSError):
        # add_signal_handler not supported on this platform (e.g. Windows)
        pass

    pgbouncer_enabled = args.pgbouncer_enabled
    await ensure_schema_from_db(args.database, pgbouncer_enabled=pgbouncer_enabled)
    await run_load(
        database=args.database,
        duration_sec=args.duration,
        batch_size=args.batch_size,
        workers=args.workers,
        target_rps=args.rows_per_second,
        queries_per_record=args.queries_per_record,
        query_delay_sec=args.query_delay / 1000.0,
        producer_tasks=args.producers,
        total_records=None,  # duration-based: run for duration_sec
        ignore_select_errors=args.ignore_select_errors,
        duplicate_ratio=args.duplicate_ratio,
        shutdown_event=shutdown_event,
        pgbouncer_enabled=pgbouncer_enabled,
    )
    logger.info("Run finished.")


if __name__ == "__main__":
    asyncio.run(main_async())
