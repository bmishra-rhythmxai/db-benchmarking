"""ClickHouse insert worker and query worker (async via asynch driver)."""
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
    def __init__(self, client_queue: asyncio.Queue, connections: list) -> None:
        self.client_queue = client_queue
        self.connections = connections


class ClickHouseWorker:
    """ClickHouse worker context: async setup/teardown and async insert workers (asynch driver)."""

    def __init__(self) -> None:
        self._resources_async = None

    async def setup(self, num_workers: int, target_rps: int, init_schema: bool = True) -> ClickHouseWorker:
        if self._resources_async is not None:
            raise RuntimeError("ClickHouseWorker.setup() already called")
        host = os.environ.get("CLICKHOUSE_HOST") or DEFAULT_HOST
        port = int(os.environ.get("CLICKHOUSE_PORT") or DEFAULT_PORT)
        pool_size = num_workers * 2
        connections = await backend.create_pool(host, port, pool_size)
        await backend.prewarm_pool(connections)
        if init_schema:
            await backend.init_schema(connections[0])
        logger.info("Starting insertions (target %d rows/sec) ...", target_rps)
        client_queue: asyncio.Queue[Any] = asyncio.Queue()
        for c in connections:
            await client_queue.put(c)
        self._resources_async = _ClickHouseResourcesAsync(client_queue, connections)
        return self

    async def teardown(self) -> None:
        if self._resources_async is not None:
            await backend.close_pool(self._resources_async.connections)
            self._resources_async = None

    def make_worker(
        self,
        insertion_queue: asyncio.Queue,
        query_queue: asyncio.Queue,
        inserted_lock: asyncio.Lock,
        inserted_shared: list[float],
        batch_size: int,
        queries_per_record: int = 1,
        pgbouncer_enabled: bool = False,
    ) -> ClickHouseAsyncWorker:
        return ClickHouseAsyncWorker(
            insertion_queue,
            query_queue,
            self._resources_async.client_queue,
            inserted_lock,
            inserted_shared,
            batch_size,
            queries_per_record,
        )

    async def get_max_patient_counter(self) -> int:
        conn = await self._resources_async.client_queue.get()
        try:
            return await backend.get_max_patient_counter(conn)
        finally:
            await self._resources_async.client_queue.put(conn)


class ClickHouseAsyncWorker(BaseAsyncInsertWorker):
    """Async ClickHouse worker using asynch driver (native async)."""

    def __init__(
        self,
        insertion_queue: asyncio.Queue,
        query_queue: asyncio.Queue,
        client_queue: asyncio.Queue,
        inserted_lock: asyncio.Lock,
        inserted_shared: list[float],
        batch_size: int,
        queries_per_record: int = 1,
    ) -> None:
        self.client_queue = client_queue
        super().__init__(insertion_queue, query_queue, inserted_lock, inserted_shared, batch_size, queries_per_record)

    async def get_connection(self) -> Any:
        return await self.client_queue.get()

    async def release_connection(self, conn: Any) -> None:
        await self.client_queue.put(conn)

    async def insert_batch(
        self, conn: Any, batch: list[tuple[str, str, str]], query_hint: str = ""
    ) -> tuple[int, int]:
        _ = query_hint  # unused for ClickHouse
        n = await backend.insert_batch(conn, batch)
        return n, 1


async def run_query_worker_clickhouse(
    query_queue: asyncio.Queue,
    client_queue: asyncio.Queue,
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
                rows = await backend.query_by_primary_key(client, mrn)
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
