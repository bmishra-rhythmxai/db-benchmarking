"""PostgreSQL insert worker and query worker (asyncpg)."""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

from ..base_worker import BaseAsyncInsertWorker
from ..config import QUERY_SENTINEL
from . import backend

logger = logging.getLogger(__name__)

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432
# When pgbouncer is enabled, connect to pgbouncer (not Postgres directly).
DEFAULT_PGBOUNCER_HOST = "pgbouncer"
DEFAULT_PGBOUNCER_PORT = 6432


class PostgresWorker:
    """PostgreSQL worker context: async setup/teardown and async insert workers (asyncpg)."""

    def __init__(self) -> None:
        self.insert_pool = None
        self.select_pool = None
        # PgBouncer flip-flop: first batch postgres1, then postgres2, then postgres1, ... (shared across workers)
        self._pgbouncer_use_db1: list[bool] = [True]  # mutable so workers can flip
        self._pgbouncer_flip_lock = asyncio.Lock()

    async def setup_async(
        self,
        num_workers: int,
        target_rps: int,
        init_schema: bool = True,
        pgbouncer_enabled: bool = False,
    ) -> PostgresWorker:
        """Create asyncpg pools, prewarm, optionally init schema. Returns self (pools stored on instance)."""
        if self.insert_pool is not None:
            raise RuntimeError("PostgresWorker.setup_async() already called")
        self.pgbouncer_enabled = pgbouncer_enabled
        if pgbouncer_enabled:
            host = os.environ.get("POSTGRES_PGBOUNCER_HOST") or DEFAULT_PGBOUNCER_HOST
            port = int(os.environ.get("POSTGRES_PGBOUNCER_PORT") or str(DEFAULT_PGBOUNCER_PORT))
        else:
            host = os.environ.get("POSTGRES_HOST") or DEFAULT_HOST
            port = int(os.environ.get("POSTGRES_PORT") or str(DEFAULT_PORT))
        if pgbouncer_enabled:
            db1 = "postgres1"
            logger.info(
                "Creating PostgreSQL pools at %s:%d (pgbouncer: %s, flip-flop postgres1/postgres2, %d insert + %d select) ...",
                host, port, db1, num_workers, num_workers,
            )
            self.insert_pool = await backend.create_pool(host, port, num_workers, database=db1)
            await backend.prewarm_pool(self.insert_pool, num_workers)
            self.select_pool = await backend.create_pool(host, port, num_workers, database=db1)
            await backend.prewarm_pool(self.select_pool, num_workers)
        else:
            logger.info(
                "Creating PostgreSQL connection pools at %s:%d (%d insert + %d select) ...",
                host, port, num_workers, num_workers,
            )
            self.insert_pool = await backend.create_pool(host, port, num_workers)
            await backend.prewarm_pool(self.insert_pool, num_workers)
            self.select_pool = await backend.create_pool(host, port, num_workers)
            await backend.prewarm_pool(self.select_pool, num_workers)
        if init_schema:
            async with self.insert_pool.acquire() as conn:
                await backend.init_schema(conn)
        logger.info("Starting insertions (target %d rows/sec) ...", target_rps)
        return self

    async def teardown_async(self) -> None:
        if self.select_pool is not None:
            await self.select_pool.close()
            self.select_pool = None
        if self.insert_pool is not None:
            await self.insert_pool.close()
            self.insert_pool = None

    def make_worker_async(
        self,
        insertion_queue: asyncio.Queue,
        query_queue: asyncio.Queue,
        inserted_lock: asyncio.Lock,
        inserted_shared: list[float],
        batch_size: int,
        queries_per_record: int = 1,
        pgbouncer_enabled: bool = False,
    ) -> PostgresAsyncWorker:
        return PostgresAsyncWorker(
            insertion_queue,
            query_queue,
            self.insert_pool,
            inserted_lock,
            inserted_shared,
            batch_size,
            queries_per_record,
            pgbouncer_enabled=pgbouncer_enabled,
            pgbouncer_flip_lock=getattr(self, "_pgbouncer_flip_lock", None),
            pgbouncer_use_db1_ref=getattr(self, "_pgbouncer_use_db1", None),
        )

    async def get_max_patient_counter_async(self) -> int:
        async with self.select_pool.acquire() as conn:
            return await backend.get_max_patient_counter(conn)


class PostgresAsyncWorker(BaseAsyncInsertWorker):
    """Async PostgreSQL insert worker. When pgbouncer_enabled, uses SET pgbouncer.database then INSERT (async)."""

    def __init__(
        self,
        insertion_queue: asyncio.Queue,
        query_queue: asyncio.Queue,
        insert_pool: Any,
        inserted_lock: asyncio.Lock,
        inserted_shared: list[float],
        batch_size: int,
        queries_per_record: int = 1,
        pgbouncer_enabled: bool = False,
        pgbouncer_flip_lock: asyncio.Lock | None = None,
        pgbouncer_use_db1_ref: list[bool] | None = None,
    ) -> None:
        self.insert_pool = insert_pool
        self.pgbouncer_enabled = pgbouncer_enabled
        self._pgbouncer_flip_lock = pgbouncer_flip_lock
        self._pgbouncer_use_db1_ref = pgbouncer_use_db1_ref  # shared [True]/[False]: True = next is postgres1
        super().__init__(insertion_queue, query_queue, inserted_lock, inserted_shared, batch_size, queries_per_record)

    async def get_connection(self) -> Any:
        return await self.insert_pool.acquire()

    async def release_connection(self, conn: Any) -> None:
        await self.insert_pool.release(conn)

    async def insert_batch(self, conn: Any, batch: list[tuple[str, str, str]]) -> tuple[int, int]:
        if self.pgbouncer_enabled and self._pgbouncer_flip_lock is not None and self._pgbouncer_use_db1_ref is not None:
            async with self._pgbouncer_flip_lock:
                use_db1 = self._pgbouncer_use_db1_ref[0]
                self._pgbouncer_use_db1_ref[0] = not use_db1
                db = backend.PGBOUNCER_DB1 if use_db1 else backend.PGBOUNCER_DB2
            return await backend.insert_batch_pgbouncer_set(conn, batch, db)
        n = await backend.insert_batch(conn, batch)
        return n, 1


async def run_query_worker_postgres_async(
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
        async with pool.acquire() as conn:
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
