"""PostgreSQL insert worker and query worker (asyncpg)."""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

from ..base_worker import (
    BaseAsyncInsertWorker,
    _mrns_from_batch,
    _split_originals_duplicates,
)
from ..config import QUERY_SENTINEL
from . import backend

logger = logging.getLogger(__name__)

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432
# When pgbouncer is enabled, connect to pgbouncer (not Postgres directly).
DEFAULT_PGBOUNCER_HOST = "pgbouncer"
DEFAULT_PGBOUNCER_PORT = 6432


class PostgresWorker(BaseAsyncInsertWorker):
    """PostgreSQL worker: context (setup/teardown/pools) and insert worker in one. Uses parameterized INSERT; when pgbouncer_enabled, query hint /* pgbouncer.database = db */ prepended to INSERT in one statement."""

    def __init__(self) -> None:
        self.insert_pool = None
        self.select_pool = None
        self.pgbouncer_enabled = False
        # BaseAsyncInsertWorker attributes (set in make_worker before run() is used)
        self.insertion_queue = None
        self.query_queue = None
        self.inserted_lock = None
        self.inserted_shared = None
        self.batch_size = 0
        self.queries_per_record = 1

    async def setup(
        self,
        num_workers: int,
        target_rps: int,
        init_schema: bool = True,
        pgbouncer_enabled: bool = False,
    ) -> PostgresWorker:
        """Create asyncpg pools, prewarm, optionally init schema. Returns self."""
        if self.insert_pool is not None:
            raise RuntimeError("PostgresWorker.setup() already called")
        self.pgbouncer_enabled = pgbouncer_enabled
        if pgbouncer_enabled:
            host = os.environ.get("POSTGRES_PGBOUNCER_HOST") or DEFAULT_PGBOUNCER_HOST
            port = int(os.environ.get("POSTGRES_PGBOUNCER_PORT") or str(DEFAULT_PGBOUNCER_PORT))
            database = "postgres1"
        else:
            host = os.environ.get("POSTGRES_HOST") or DEFAULT_HOST
            port = int(os.environ.get("POSTGRES_PORT") or str(DEFAULT_PORT))
            database = None
        logger.info(
            "Creating PostgreSQL pools at %s:%d (%d insert + %d select)%s ...",
            host, port, num_workers, num_workers,
            " (pgbouncer alternate postgres1/postgres2 by batch index)" if pgbouncer_enabled else "",
        )
        skip_session_set = pgbouncer_enabled
        pool_kw: dict[str, Any] = {"database": database} if database else {}
        self.insert_pool = await backend.create_pool(host, port, num_workers, **pool_kw)
        await backend.prewarm_pool(self.insert_pool, num_workers, skip_session_set=skip_session_set)
        self.select_pool = await backend.create_pool(host, port, num_workers, **pool_kw)
        await backend.prewarm_pool(self.select_pool, num_workers, skip_session_set=skip_session_set)
        if init_schema:
            conn = await self.insert_pool.acquire()
            try:
                await backend.init_schema(conn)
            finally:
                await self.insert_pool.release(conn)
        logger.info("Starting insertions (target %d rows/sec) ...", target_rps)
        return self

    async def teardown(self) -> None:
        if self.select_pool is not None:
            await self.select_pool.close()
            self.select_pool = None
        if self.insert_pool is not None:
            await self.insert_pool.close()
            self.insert_pool = None

    def make_worker(
        self,
        insertion_queue: asyncio.Queue[tuple[str, list] | None],
        query_queue: asyncio.Queue,
        inserted_lock: asyncio.Lock,
        inserted_shared: list[float],
        batch_size: int,
        queries_per_record: int = 1,
        pgbouncer_enabled: bool = False,
    ) -> PostgresWorker:
        """Set queue/state for run() and return self (this object is the insert worker)."""
        self.insertion_queue = insertion_queue
        self.query_queue = query_queue
        self.inserted_lock = inserted_lock
        self.inserted_shared = inserted_shared
        self.batch_size = batch_size
        self.queries_per_record = queries_per_record
        return self

    async def get_max_patient_counter(self) -> int:
        conn = await self.select_pool.acquire()
        try:
            return await backend.get_max_patient_counter(conn)
        finally:
            await self.select_pool.release(conn)

    async def get_connection(self) -> Any:
        return await self.insert_pool.acquire()

    async def release_connection(self, conn: Any) -> None:
        await self.insert_pool.release(conn)

    async def _flush(self, batch: list, query_hint: str = "") -> None:
        """When pgbouncer is enabled, use a separate connection per sub-batch (originals,
        then duplicates) so only one hint + INSERT runs per connection.
        query_hint is the prepared hint string set by the producer.
        """
        if not batch:
            return
        if not self.pgbouncer_enabled:
            await super()._flush(batch, query_hint)
            return
        originals, duplicates = _split_originals_duplicates(batch)
        async with self.inserted_lock:
            self.inserted_shared[5] += 1
        total_rows = 0
        total_latency_sec = 0.0
        n_statements = 0
        try:
            if originals:
                conn = await self.get_connection()
                try:
                    rows_db = [(r[0], r[1], r[2]) for r in originals]
                    t0 = time.perf_counter()
                    n, stmts, db_used = await self.insert_batch(conn, rows_db, query_hint)
                    total_rows += n
                    total_latency_sec += time.perf_counter() - t0
                    n_statements += stmts
                    if db_used:
                        async with self.inserted_lock:
                            if db_used == backend.PGBOUNCER_DB1:
                                self.inserted_shared[7] += n
                            else:
                                self.inserted_shared[8] += n
                finally:
                    await self.release_connection(conn)
            if duplicates:
                conn = await self.get_connection()
                try:
                    rows_db = [(r[0], r[1], r[2]) for r in duplicates]
                    t0 = time.perf_counter()
                    n, stmts, db_used = await self.insert_batch(conn, rows_db, query_hint)
                    total_rows += n
                    total_latency_sec += time.perf_counter() - t0
                    n_statements += stmts
                    if db_used:
                        async with self.inserted_lock:
                            if db_used == backend.PGBOUNCER_DB1:
                                self.inserted_shared[7] += n
                            else:
                                self.inserted_shared[8] += n
                finally:
                    await self.release_connection(conn)
            n_originals = len(originals)
            n_duplicates = len(duplicates)
            async with self.inserted_lock:
                self.inserted_shared[0] += total_rows
                self.inserted_shared[1] += n_originals
                self.inserted_shared[2] += n_duplicates
                self.inserted_shared[3] += total_latency_sec
                self.inserted_shared[4] += n_statements
            if self.queries_per_record > 0:
                insert_time = time.time()
                for mrn in _mrns_from_batch(batch):
                    await self.query_queue.put((mrn, insert_time))
        finally:
            async with self.inserted_lock:
                self.inserted_shared[5] -= 1
                if self.inserted_shared[5] < 0:
                    self.inserted_shared[5] = 0

    async def insert_batch(
        self, conn: Any, batch: list[tuple[str, str, str]], query_hint: str = ""
    ) -> tuple[int, int, str | None]:
        """Returns (rows_inserted, statement_count, db_used). query_hint is the prepared hint string."""
        if self.pgbouncer_enabled and query_hint:
            n = await backend.insert_batch_with_pgbouncer_hint(conn, batch, query_hint)
            db_used = backend.database_from_query_hint(query_hint)
            return n, 1, db_used
        n = await backend.insert_batch(conn, batch)
        return n, 1, None


async def run_query_worker_postgres(
    query_queue: asyncio.Queue,
    pool: Any,
    queries_lock: asyncio.Lock,
    queries_shared: list[float],
    queries_per_record: int,
    query_delay_sec: float,
    query_rate_limiter: Any,
    ignore_select_errors: bool,
) -> None:
    while True:
        item = await query_queue.get()
        if item is QUERY_SENTINEL:
            return
        mrn, insert_time = item
        if query_delay_sec > 0:
            deadline = insert_time + query_delay_sec
            sleep_sec = deadline - time.time()
            if sleep_sec > 0:
                await asyncio.sleep(sleep_sec)
        conn = await pool.acquire()
        try:
            total_latency_sec = 0.0
            failed = 0
            for _ in range(queries_per_record):
                if query_rate_limiter is not None:
                    await query_rate_limiter.acquire()
                t0 = time.perf_counter()
                rows = await backend.query_by_primary_key(conn, mrn)
                total_latency_sec += time.perf_counter() - t0
                if len(rows) != 1:
                    failed += 1
                    if not ignore_select_errors:
                        logger.error(
                            "Query by primary key returned %d rows for MEDICAL_RECORD_NUMBER=%s (expected 1)",
                            len(rows), mrn,
                        )
            async with queries_lock:
                queries_shared[0] += queries_per_record
                queries_shared[1] += total_latency_sec
                queries_shared[2] += failed
        finally:
            await pool.release(conn)
