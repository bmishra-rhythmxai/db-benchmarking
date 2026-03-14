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

logger = logging.getLogger(__name__)

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432


class PostgresWorker:
    """PostgreSQL worker context: async setup/teardown and async insert workers (asyncpg)."""

    def __init__(self) -> None:
        self.insert_pool_async = None
        self.select_pool_async = None

    async def setup_async(self, num_workers: int, target_rps: int, init_schema: bool = True) -> PostgresWorker:
        """Create asyncpg pools, prewarm, optionally init schema. Returns self (pools stored on instance)."""
        if self.insert_pool_async is not None:
            raise RuntimeError("PostgresWorker.setup_async() already called")
        host = os.environ.get("POSTGRES_HOST") or DEFAULT_HOST
        port = DEFAULT_PORT
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
    ) -> PostgresAsyncWorker:
        return PostgresAsyncWorker(
            insertion_queue,
            query_queue,
            self.insert_pool_async,
            inserted_lock,
            inserted_shared,
            batch_size,
            queries_per_record,
        )

    async def get_max_patient_counter_async(self) -> int:
        async with self.select_pool_async.acquire() as conn:
            return await backend_async.get_max_patient_counter(conn)


class PostgresAsyncWorker(BaseAsyncInsertWorker):
    """Async PostgreSQL insert worker using asyncpg pool."""

    def __init__(
        self,
        insertion_queue: asyncio.Queue,
        query_queue: asyncio.Queue,
        insert_pool: Any,
        inserted_lock: asyncio.Lock,
        inserted_shared: list[float],
        batch_size: int,
        queries_per_record: int = 1,
    ) -> None:
        self.insert_pool = insert_pool
        super().__init__(insertion_queue, query_queue, inserted_lock, inserted_shared, batch_size, queries_per_record)

    async def get_connection(self) -> Any:
        return await self.insert_pool.acquire()

    async def release_connection(self, conn: Any) -> None:
        await self.insert_pool.release(conn)

    async def insert_batch(self, conn: Any, batch: list[tuple[str, str, str]]) -> int:
        return await backend_async.insert_batch(conn, batch)


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
