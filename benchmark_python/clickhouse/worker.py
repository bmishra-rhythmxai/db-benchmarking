"""ClickHouse insert worker and query worker: sync and async (executor for sync driver)."""
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

logger = logging.getLogger(__name__)

DEFAULT_HOST = "clickhouse"
DEFAULT_PORT = 9000


class _ClickHouseResources:
    """Holds client_queue for workers and clients list for teardown."""

    def __init__(self, client_queue: queue.Queue, clients: list) -> None:
        self.client_queue = client_queue
        self.clients = clients


def _run_query_worker_clickhouse(
    query_queue: queue.Queue,
    client_queue: queue.Queue,
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
        conn = client_queue.get()
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
            client_queue.put(conn)
        query_queue.task_done()


class ClickHouseWorker(BaseInsertWorker):
    """ClickHouse insert worker using a client queue. Pushes MRNs to query_queue for parallel query workers.
    Can be used as a context (create with Worker(), call setup()) or as an insert worker (from ctx.make_worker())."""

    def __init__(
        self,
        insertion_queue: queue.Queue | None = None,
        query_queue: queue.Queue | None = None,
        client_queue: queue.Queue | None = None,
        inserted_lock: Any = None,
        inserted_shared: list[float] | None = None,
        batch_size: int | None = None,
        queries_per_record: int = 1,
        _resources: _ClickHouseResources | None = None,
    ) -> None:
        self._resources = _resources
        self.client_queue = client_queue
        if insertion_queue is not None and query_queue is not None and client_queue is not None and inserted_lock is not None and inserted_shared is not None and batch_size is not None:
            super().__init__(insertion_queue, query_queue, inserted_lock, inserted_shared, batch_size, queries_per_record)

    def get_connection(self) -> Any:
        return self.client_queue.get()

    def release_connection(self, conn: Any) -> None:
        self.client_queue.put(conn)

    def insert_batch(self, conn: Any, batch: list[tuple[str, str, str]]) -> int:
        return backend.insert_batch(conn, batch)

    def setup(self, num_workers: int, target_rps: int, init_schema: bool = True) -> ClickHouseWorker:
        """Create client pool, prewarm, optionally init schema. Resources are stored on this instance. Returns self."""
        if self._resources is not None:
            raise RuntimeError("ClickHouseWorker.setup() already called")
        host = os.environ.get("CLICKHOUSE_HOST") or DEFAULT_HOST
        port = DEFAULT_PORT
        pool_size = num_workers * 2  # insert workers + query workers
        logger.info(
            "Creating ClickHouse connection pool at %s:%d (%d clients for %d insert + %d query workers) ...",
            host, port, pool_size, num_workers, num_workers,
        )
        ch_pool_list = backend.create_pool(host, port, pool_size)
        backend.prewarm_pool(ch_pool_list)
        if init_schema:
            backend.init_schema(ch_pool_list[0])
        logger.info("Starting insertions (target %d rows/sec) ...", target_rps)
        client_queue: queue.Queue = queue.Queue()
        for c in ch_pool_list:
            client_queue.put(c)
        self._resources = _ClickHouseResources(client_queue, ch_pool_list)
        return self

    def teardown(self) -> None:
        """Disconnect all clients."""
        if self._resources is not None:
            for c in self._resources.clients:
                c.disconnect()
            self._resources = None

    def make_worker(
        self,
        insertion_queue: queue.Queue,
        query_queue: queue.Queue,
        inserted_lock: Any,
        inserted_shared: list[float],
        batch_size: int,
        queries_per_record: int = 1,
    ) -> ClickHouseWorker:
        """Return a ClickHouseWorker instance that shares this instance's client queue (use .run as thread target)."""
        return ClickHouseWorker(
            insertion_queue,
            query_queue,
            self._resources.client_queue,
            inserted_lock,
            inserted_shared,
            batch_size,
            queries_per_record,
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
            _run_query_worker_clickhouse(
                query_queue, self._resources.client_queue, queries_lock, queries_shared, queries_per_record, query_delay_sec, ignore_select_errors
            )
        return run

    def get_max_patient_counter(self) -> int:
        """Return the maximum patient ordinal in the database, or -1 if empty."""
        client = self._resources.client_queue.get()
        try:
            return backend.get_max_patient_counter(client)
        finally:
            self._resources.client_queue.put(client)

    # --- Async worker (sync driver via run_in_executor) ---

    async def setup_async(self, num_workers: int, target_rps: int, init_schema: bool = True) -> ClickHouseWorker:
        if self._resources is not None:
            raise RuntimeError("ClickHouseWorker.setup_async() already called")
        host = os.environ.get("CLICKHOUSE_HOST") or DEFAULT_HOST
        port = DEFAULT_PORT
        pool_size = num_workers * 2
        loop = asyncio.get_running_loop()
        ch_pool_list = await loop.run_in_executor(
            None,
            lambda: backend.create_pool(host, port, pool_size),
        )
        await loop.run_in_executor(None, lambda: backend.prewarm_pool(ch_pool_list))
        if init_schema:
            await loop.run_in_executor(None, lambda: backend.init_schema(ch_pool_list[0]))
        logger.info("Starting insertions (target %d rows/sec) ...", target_rps)
        client_queue: asyncio.Queue[Any] = asyncio.Queue()
        for c in ch_pool_list:
            await client_queue.put(c)
        self._resources_async = _ClickHouseResourcesAsync(client_queue, ch_pool_list, loop)
        return self

    async def teardown_async(self) -> None:
        if getattr(self, "_resources_async", None) is not None:
            for c in self._resources_async.clients:
                await asyncio.get_running_loop().run_in_executor(None, c.disconnect)
            self._resources_async = None

    def make_worker_async(
        self,
        insertion_queue: asyncio.Queue,
        query_queue: asyncio.Queue,
        inserted_lock: asyncio.Lock,
        inserted_shared: list[float],
        batch_size: int,
        queries_per_record: int = 1,
    ) -> ClickHouseAsyncWorker:
        return ClickHouseAsyncWorker(
            insertion_queue,
            query_queue,
            self._resources_async.client_queue,
            self._resources_async.loop,
            inserted_lock,
            inserted_shared,
            batch_size,
            queries_per_record,
        )

    async def get_max_patient_counter_async(self) -> int:
        client = await self._resources_async.client_queue.get()
        try:
            return await self._resources_async.loop.run_in_executor(
                None,
                lambda: backend.get_max_patient_counter(client),
            )
        finally:
            await self._resources_async.client_queue.put(client)


class _ClickHouseResourcesAsync:
    def __init__(self, client_queue: asyncio.Queue, clients: list, loop: asyncio.AbstractEventLoop) -> None:
        self.client_queue = client_queue
        self.clients = clients
        self.loop = loop


class ClickHouseAsyncWorker(BaseAsyncInsertWorker):
    """Async ClickHouse worker: sync driver calls run in executor."""

    def __init__(
        self,
        insertion_queue: asyncio.Queue,
        query_queue: asyncio.Queue,
        client_queue: asyncio.Queue,
        loop: asyncio.AbstractEventLoop,
        inserted_lock: asyncio.Lock,
        inserted_shared: list[float],
        batch_size: int,
        queries_per_record: int = 1,
    ) -> None:
        self.client_queue = client_queue
        self.loop = loop
        super().__init__(insertion_queue, query_queue, inserted_lock, inserted_shared, batch_size, queries_per_record)

    async def get_connection(self) -> Any:
        return await self.client_queue.get()

    async def release_connection(self, conn: Any) -> None:
        await self.client_queue.put(conn)

    async def insert_batch(self, conn: Any, batch: list[tuple[str, str, str]]) -> int:
        return await self.loop.run_in_executor(None, lambda: backend.insert_batch(conn, batch))


async def run_query_worker_clickhouse_async(
    query_queue: asyncio.Queue,
    client_queue: asyncio.Queue,
    loop: asyncio.AbstractEventLoop,
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
            sleep_sec = insert_time + query_delay_sec - time.time()
            if sleep_sec > 0:
                await asyncio.sleep(sleep_sec)
        client = await client_queue.get()
        try:
            total_latency_sec = 0.0
            failed = 0
            for _ in range(queries_per_record):
                t0 = time.perf_counter()
                rows = await loop.run_in_executor(None, lambda: backend.query_by_primary_key(client, mrn))
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
            await client_queue.put(client)
