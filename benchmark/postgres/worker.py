"""PostgreSQL insert worker and query worker: setup, teardown, and worker instances."""
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

DEFAULT_HOST = "postgres"
DEFAULT_PORT = 5432


def _run_query_worker_postgres(
    query_queue: queue.Queue,
    pool: Any,
    queries_lock: threading.Lock,
    queries_shared: list[int],
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
        conn = pool.getconn()
        try:
            for _ in range(queries_per_record):
                rows = backend.query_by_primary_key(conn, mrn)
                if len(rows) != 1:
                    logger.error(
                        "Query by primary key returned %d rows for MEDICAL_RECORD_NUMBER=%s (expected 1)",
                        len(rows), mrn,
                    )
            with queries_lock:
                queries_shared[0] += queries_per_record
        finally:
            pool.putconn(conn)
        query_queue.task_done()


class PostgresWorker(BaseInsertWorker):
    """PostgreSQL insert worker using a connection pool. Pushes MRNs to query_queue for parallel query workers."""

    def __init__(
        self,
        insertion_queue: queue.Queue,
        query_queue: queue.Queue,
        pool: Any,
        inserted_lock: threading.Lock,
        inserted_shared: list[int],
    ) -> None:
        super().__init__(insertion_queue, query_queue, inserted_lock, inserted_shared)
        self.pool = pool

    def get_connection(self) -> Any:
        return self.pool.getconn()

    def release_connection(self, conn: Any) -> None:
        self.pool.putconn(conn)

    def insert_batch(self, conn: Any, batch: list[tuple[str, str, str]]) -> int:
        return backend.insert_batch(conn, batch)

    @classmethod
    def setup(cls, num_workers: int, target_rps: int) -> Any:
        """Resolve host/port, create pool, prewarm, init schema. Returns pool (pass to make_worker and teardown)."""
        host = os.environ.get("POSTGRES_HOST") or DEFAULT_HOST
        port = DEFAULT_PORT
        pool_size = num_workers * 2  # insert workers + query workers
        logger.info(
            "Creating PostgreSQL connection pool at %s:%d (%d connections for %d insert + %d query workers) ...",
            host, port, pool_size, num_workers, num_workers,
        )
        pool = backend.create_pool(host, port, pool_size)
        backend.prewarm_pool(pool, pool_size)
        conn = pool.getconn()
        try:
            backend.init_schema(conn)
        finally:
            pool.putconn(conn)
        logger.info("Starting insertions (target %d rows/sec) ...", target_rps)
        return pool

    @staticmethod
    def teardown(pool: Any) -> None:
        """Close all connections in the pool."""
        pool.closeall()

    @staticmethod
    def make_worker(
        insertion_queue: queue.Queue,
        query_queue: queue.Queue,
        pool: Any,
        inserted_lock: threading.Lock,
        inserted_shared: list[int],
    ) -> PostgresWorker:
        """Return a PostgresWorker instance (use .run as thread target)."""
        return PostgresWorker(insertion_queue, query_queue, pool, inserted_lock, inserted_shared)

    @staticmethod
    def make_query_worker(
        query_queue: queue.Queue,
        pool: Any,
        queries_lock: threading.Lock,
        queries_shared: list[int],
        queries_per_record: int,
        query_delay_sec: float = 0.0,
    ):
        """Return a callable to run as query worker thread target (consumes query_queue, runs lookups)."""
        def run() -> None:
            _run_query_worker_postgres(
                query_queue, pool, queries_lock, queries_shared, queries_per_record, query_delay_sec
            )
        return run
