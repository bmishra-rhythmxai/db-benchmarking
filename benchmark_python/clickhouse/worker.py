"""ClickHouse insert worker and query worker (async, sync driver via run_in_executor)."""
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

DEFAULT_HOST = "clickhouse"
DEFAULT_PORT = 9000


class _ClickHouseResourcesAsync:
    def __init__(self, client_queue: asyncio.Queue, clients: list, loop: asyncio.AbstractEventLoop) -> None:
        self.client_queue = client_queue
        self.clients = clients
        self.loop = loop


class ClickHouseWorker:
    """ClickHouse worker context: async setup/teardown and async insert workers (sync driver in executor)."""

    def __init__(self) -> None:
        self._resources_async = None

    async def setup_async(self, num_workers: int, target_rps: int, init_schema: bool = True) -> ClickHouseWorker:
        if self._resources_async is not None:
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
        if self._resources_async is not None:
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

    async def insert_batch(self, conn: Any, batch: list[tuple[str, str, str]]) -> tuple[int, int]:
        n = await self.loop.run_in_executor(None, lambda: backend.insert_batch(conn, batch))
        return n, 1


async def run_query_worker_clickhouse_async(
    query_queue: asyncio.Queue,
    client_queue: asyncio.Queue,
    loop: asyncio.AbstractEventLoop,
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
            sleep_sec = insert_time + query_delay_sec - time.time()
            if sleep_sec > 0:
                await asyncio.sleep(sleep_sec)
        client = await client_queue.get()
        try:
            total_latency_sec = 0.0
            failed = 0
            for _ in range(queries_per_record):
                if query_rate_limiter is not None:
                    await query_rate_limiter.acquire()
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
