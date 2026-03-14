"""PostgreSQL insert worker and query worker: sync and async (setup, teardown, workers)."""
from __future__ import annotations

import asyncio
import logging
import os
import queue
import threading
import time
from typing import Any

from ..base_worker import BaseAsyncInsertWorker, BaseInsertWorker
from ..config import QUERY_SENTINEL
from . import backend
from . import backend_async

logger = logging.getLogger(__name__)

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432


def _run_query_worker_postgres(
    query_queue: queue.Queue,
    pool: Any,
    queries_lock: threading.Lock,
    queries_shared: list[float],
    queries_per_record: int,
    query_delay_sec: float = 0.0,
    ignore_select_errors: bool = False,
) -> None:
    """Query worker loop: get MRN from query_queue, run queries_per_record lookups, log if != 1 row (unless ignored). Stops on QUERY_SENTINEL."""
    while True:
        item = query_queue.get()
        if item is QUERY_SENTINEL:
            query_queue.task_done()
            return
        mrn, insert_time = item
        if query_delay_sec > 0:
            deadline = insert_time + query_delay_sec
            sleep_sec = deadline - time.time()
            if sleep_sec > 0:
                time.sleep(sleep_sec)
        conn = pool.getconn()
        try:
            total_latency_sec = 0.0
            failed = 0
            for _ in range(queries_per_record):
                t0 = time.perf_counter()
                rows = backend.query_by_primary_key(conn, mrn)
                total_latency_sec += time.perf_counter() - t0
                if len(rows) != 1:
                    failed += 1
                    if not ignore_select_errors:
                        logger.error(
                            "Query by primary key returned %d rows for MEDICAL_RECORD_NUMBER=%s (expected 1)",
                            len(rows), mrn,
                        )
            with queries_lock:
                queries_shared[0] += queries_per_record
                queries_shared[1] += total_latency_sec
                queries_shared[2] += failed
        finally:
            pool.putconn(conn)
        query_queue.task_done()


class PostgresWorker(BaseInsertWorker):
    """PostgreSQL insert worker using a connection pool. Pushes MRNs to query_queue for parallel query workers.
    Can be used as a context (create with Worker(), call setup()) or as an insert worker (from ctx.make_worker())."""

    def __init__(
        self,
        insertion_queue: queue.Queue | None = None,
        query_queue: queue.Queue | None = None,
        insert_pool: Any = None,
        inserted_lock: threading.Lock | None = None,
        inserted_shared: list[float] | None = None,
        batch_size: int | None = None,
        queries_per_record: int = 1,
    ) -> None:
        self.insert_pool = insert_pool
        self.select_pool = None  # set by setup(); query workers use this
        if insertion_queue is not None and query_queue is not None and insert_pool is not None and inserted_lock is not None and inserted_shared is not None and batch_size is not None:
            super().__init__(insertion_queue, query_queue, inserted_lock, inserted_shared, batch_size, queries_per_record)

    def get_connection(self) -> Any:
        return self.insert_pool.getconn()

    def release_connection(self, conn: Any) -> None:
        self.insert_pool.putconn(conn)

    def insert_batch(self, conn: Any, batch: list[tuple[str, str, str]]) -> int:
        return backend.insert_batch(conn, batch)

    def setup(self, num_workers: int, target_rps: int, init_schema: bool = True) -> PostgresWorker:
        """Create separate insert and select pools, prewarm, optionally init schema. Resources are stored on this instance. Returns self."""
        if self.insert_pool is not None:
            raise RuntimeError("PostgresWorker.setup() already called")
        host = os.environ.get("POSTGRES_HOST") or DEFAULT_HOST
        port = DEFAULT_PORT
        logger.info(
            "Creating PostgreSQL connection pools at %s:%d (%d insert + %d select connections) ...",
            host, port, num_workers, num_workers,
        )
        self.insert_pool = backend.create_pool(host, port, num_workers)
        backend.prewarm_pool(self.insert_pool, num_workers)
        self.select_pool = backend.create_pool(host, port, num_workers)
        backend.prewarm_pool(self.select_pool, num_workers)
        if init_schema:
            conn = self.insert_pool.getconn()
            try:
                backend.init_schema(conn)
            finally:
                self.insert_pool.putconn(conn)
        logger.info("Starting insertions (target %d rows/sec) ...", target_rps)
        return self

    def teardown(self) -> None:
        """Close all connections in both pools."""
        if self.select_pool is not None:
            self.select_pool.closeall()
            self.select_pool = None
        if self.insert_pool is not None:
            self.insert_pool.closeall()
            self.insert_pool = None

    def make_worker(
        self,
        insertion_queue: queue.Queue,
        query_queue: queue.Queue,
        inserted_lock: threading.Lock,
        inserted_shared: list[float],
        batch_size: int,
        queries_per_record: int = 1,
    ) -> PostgresWorker:
        """Return a PostgresWorker instance that shares this instance's insert pool (use .run as thread target)."""
        return PostgresWorker(
            insertion_queue, query_queue, self.insert_pool, inserted_lock, inserted_shared, batch_size, queries_per_record
        )

    def make_query_worker(
        self,
        query_queue: queue.Queue,
        queries_lock: threading.Lock,
        queries_shared: list[float],
        queries_per_record: int,
        query_delay_sec: float = 0.0,
        ignore_select_errors: bool = False,
    ):
        """Return a callable to run as query worker thread target (consumes query_queue, runs lookups)."""
        def run() -> None:
            _run_query_worker_postgres(
                query_queue, self.select_pool, queries_lock, queries_shared, queries_per_record, query_delay_sec, ignore_select_errors
            )
        return run

    def get_max_patient_counter(self) -> int:
        """Return the maximum patient ordinal in the database, or -1 if empty."""
        conn = self.select_pool.getconn()
        try:
            return backend.get_max_patient_counter(conn)
        finally:
            self.select_pool.putconn(conn)

    # --- Async worker (asyncpg) ---

    async def setup_async(self, num_workers: int, target_rps: int, init_schema: bool = True) -> PostgresWorker:
        """Create asyncpg pools, prewarm, optionally init schema. Returns self (pools stored on instance)."""
        if getattr(self, "insert_pool_async", None) is not None:
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
        if getattr(self, "select_pool_async", None) is not None:
            await self.select_pool_async.close()
            self.select_pool_async = None
        if getattr(self, "insert_pool_async", None) is not None:
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
