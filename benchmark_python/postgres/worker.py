"""PostgreSQL insert worker and query worker (async, asyncpg)."""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

from ..base_worker import BaseAsyncInsertWorker
from ..config import QUERY_SENTINEL
from . import backend_async
from . import backend_pipeline

logger = logging.getLogger(__name__)

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432


class PostgresWorker:
    """PostgreSQL worker context: async setup/teardown and async insert workers (asyncpg)."""

    def __init__(self) -> None:
        self.insert_pool_async = None
        self.select_pool_async = None
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
        if self.insert_pool_async is not None:
            raise RuntimeError("PostgresWorker.setup_async() already called")
        host = os.environ.get("POSTGRES_HOST") or DEFAULT_HOST
        port = int(os.environ.get("POSTGRES_PORT") or DEFAULT_PORT)
        self.pgbouncer_enabled = pgbouncer_enabled
        if pgbouncer_enabled:
            db1 = "postgres1"
            logger.info(
                "Creating PostgreSQL pools at %s:%d (pgbouncer: %s, pipeline SET+INSERT, flip-flop postgres1/postgres2, %d insert + %d select) ...",
                host, port, db1, num_workers, num_workers,
            )
            self.insert_pool_async = await backend_async.create_pool(host, port, num_workers, database=db1)
            await backend_async.prewarm_pool(self.insert_pool_async, num_workers)
            self.select_pool_async = await backend_async.create_pool(host, port, num_workers, database=db1)
            await backend_async.prewarm_pool(self.select_pool_async, num_workers)
            loop = asyncio.get_running_loop()
            self._psycopg_pool = await loop.run_in_executor(
                None,
                lambda: backend_pipeline.create_psycopg_pool(host, port, num_workers, db1),
            )
        else:
            logger.info(
                "Creating PostgreSQL async connection pools at %s:%d (%d insert + %d select) ...",
                host, port, num_workers, num_workers,
            )
            self.insert_pool_async = await backend_async.create_pool(host, port, num_workers)
            await backend_async.prewarm_pool(self.insert_pool_async, num_workers)
            self.select_pool_async = await backend_async.create_pool(host, port, num_workers)
            await backend_async.prewarm_pool(self.select_pool_async, num_workers)
        if init_schema:
            async with self.insert_pool_async.acquire() as conn:
                await backend_async.init_schema(conn)
        logger.info("Starting insertions (target %d rows/sec) ...", target_rps)
        return self

    async def teardown_async(self) -> None:
        if self.select_pool_async is not None:
            await self.select_pool_async.close()
            self.select_pool_async = None
        if getattr(self, "_psycopg_pool", None) is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._psycopg_pool.close)
            self._psycopg_pool = None
        if self.insert_pool_async is not None:
            await self.insert_pool_async.close()
            self.insert_pool_async = None

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
            self.insert_pool_async,
            inserted_lock,
            inserted_shared,
            batch_size,
            queries_per_record,
            pgbouncer_enabled=pgbouncer_enabled,
            pgbouncer_flip_lock=getattr(self, "_pgbouncer_flip_lock", None),
            pgbouncer_use_db1_ref=getattr(self, "_pgbouncer_use_db1", None),
            psycopg_pool=getattr(self, "_psycopg_pool", None),
        )

    async def get_max_patient_counter_async(self) -> int:
        async with self.select_pool_async.acquire() as conn:
            return await backend_async.get_max_patient_counter(conn)


class PostgresAsyncWorker(BaseAsyncInsertWorker):
    """Async PostgreSQL insert worker. When pgbouncer_enabled, uses psycopg3 connection.pipeline() for SET+INSERT in one round-trip."""

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
        psycopg_pool: Any = None,
    ) -> None:
        self.insert_pool = insert_pool
        self.pgbouncer_enabled = pgbouncer_enabled
        self._pgbouncer_flip_lock = pgbouncer_flip_lock
        self._pgbouncer_use_db1_ref = pgbouncer_use_db1_ref  # shared [True]/[False]: True = next is postgres1
        self._psycopg_pool = psycopg_pool
        super().__init__(insertion_queue, query_queue, inserted_lock, inserted_shared, batch_size, queries_per_record)

    async def get_connection(self) -> Any:
        if self.pgbouncer_enabled and self._psycopg_pool is not None:
            return self._psycopg_pool  # pipeline path uses pool directly per batch
        return await self.insert_pool.acquire()

    async def release_connection(self, conn: Any) -> None:
        if self.pgbouncer_enabled and self._psycopg_pool is not None:
            return  # no-op; pipeline path gets conn from pool inside executor
        self.insert_pool.release(conn)

    async def insert_batch(self, conn: Any, batch: list[tuple[str, str, str]]) -> tuple[int, int]:
        if self.pgbouncer_enabled and self._psycopg_pool is not None and self._pgbouncer_flip_lock is not None and self._pgbouncer_use_db1_ref is not None:
            async with self._pgbouncer_flip_lock:
                use_db1 = self._pgbouncer_use_db1_ref[0]
                self._pgbouncer_use_db1_ref[0] = not use_db1
                db = backend_async.PGBOUNCER_DB1 if use_db1 else backend_async.PGBOUNCER_DB2
            loop = asyncio.get_running_loop()
            n = await loop.run_in_executor(
                None,
                lambda: backend_pipeline.insert_batch_pipeline_with_pool(conn, batch, db),
            )
            return n, 1
        n = await backend_async.insert_batch(conn, batch)
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
                rows = await backend_async.query_by_primary_key(conn, mrn)
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
