"""ClickHouse insert worker and query worker: setup, teardown, and worker instances."""
from __future__ import annotations

import logging
import os
import queue
import threading
import time
from typing import Any

from ..base_worker import BaseInsertWorker
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
) -> None:
    """Query worker loop: get MRN from query_queue, run queries_per_record lookups, log if != 1 row. Stops on QUERY_SENTINEL."""
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
            for _ in range(queries_per_record):
                t0 = time.perf_counter()
                rows = backend.query_by_primary_key(conn, mrn)
                total_latency_sec += time.perf_counter() - t0
                if len(rows) != 1:
                    logger.error(
                        "Query by primary key returned %d rows for MEDICAL_RECORD_NUMBER=%s (expected 1)",
                        len(rows), mrn,
                    )
            with queries_lock:
                queries_shared[0] += queries_per_record
                queries_shared[1] += total_latency_sec
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
        batch_wait_sec: float | None = None,
        queries_per_record: int = 1,
        _resources: _ClickHouseResources | None = None,
    ) -> None:
        self._resources = _resources
        self.client_queue = client_queue
        if insertion_queue is not None and query_queue is not None and client_queue is not None and inserted_lock is not None and inserted_shared is not None and batch_size is not None and batch_wait_sec is not None:
            super().__init__(insertion_queue, query_queue, inserted_lock, inserted_shared, batch_size, batch_wait_sec, queries_per_record)

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
        batch_wait_sec: float,
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
            batch_wait_sec,
            queries_per_record,
        )

    def make_query_worker(
        self,
        query_queue: queue.Queue,
        queries_lock: threading.Lock,
        queries_shared: list[float],
        queries_per_record: int,
        query_delay_sec: float = 0.0,
    ):
        """Return a callable to run as query worker thread target (consumes query_queue, runs lookups)."""
        def run() -> None:
            _run_query_worker_clickhouse(
                query_queue, self._resources.client_queue, queries_lock, queries_shared, queries_per_record, query_delay_sec
            )
        return run

    def get_max_patient_counter(self) -> int:
        """Return the maximum patient ordinal in the database, or -1 if empty."""
        client = self._resources.client_queue.get()
        try:
            return backend.get_max_patient_counter(client)
        finally:
            self._resources.client_queue.put(client)
