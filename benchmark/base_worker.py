"""Base insert worker: consume records from queue, batch (1s or batch_size), insert, push MRNs to query queue."""
from __future__ import annotations

import json
import logging
import queue
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, TypeVar

from .config import INSERTION_SENTINEL

logger = logging.getLogger(__name__)

Record = tuple[str, str, str, bool]
Batch = list[Record]
Conn = TypeVar("Conn")


def _mrns_from_batch(batch: Batch) -> list[str]:
    """Extract MEDICAL_RECORD_NUMBER from each record in the batch. Skips invalid/empty; logs and continues."""
    mrns: list[str] = []
    for record in batch:
        json_str = record[2]
        try:
            mrn = json.loads(json_str).get("MEDICAL_RECORD_NUMBER")
        except (json.JSONDecodeError, KeyError, TypeError):
            logger.error("Query queue: could not get MEDICAL_RECORD_NUMBER from record, skipping")
            continue
        if mrn:
            mrns.append(mrn)
        else:
            logger.error("Query queue: MEDICAL_RECORD_NUMBER is empty, skipping")
    return mrns


class BaseInsertWorker(ABC):
    """Base class for database insert workers. Consumes records, batches (1s or batch_size), inserts, pushes MRNs to query_queue."""

    def __init__(
        self,
        insertion_queue: queue.Queue[Record | None],
        query_queue: queue.Queue[str | Any],
        inserted_lock: threading.Lock,
        inserted_shared: list[float],
        batch_size: int,
        batch_wait_sec: float,
        queries_per_record: int = 1,
    ) -> None:
        self.insertion_queue = insertion_queue
        self.query_queue = query_queue
        self.inserted_lock = inserted_lock
        self.inserted_shared = inserted_shared
        self.batch_size = batch_size
        self.batch_wait_sec = batch_wait_sec
        self.queries_per_record = queries_per_record

    @abstractmethod
    def get_connection(self) -> Any:
        """Acquire a connection/client for this thread. Must be paired with release_connection."""
        ...

    @abstractmethod
    def release_connection(self, conn: Any) -> None:
        """Return the connection/client to the pool."""
        ...

    @abstractmethod
    def insert_batch(self, conn: Any, batch: list[tuple[str, str, str]]) -> int:
        """Insert the batch using the given connection. Return number of rows inserted."""
        ...

    def _flush(self, batch: Batch) -> None:
        """Insert batch, update inserted count (total, originals, duplicates), insert latency, push MRNs to query_queue."""
        if not batch:
            return
        conn = self.get_connection()
        try:
            batch_for_db: list[tuple[str, str, str]] = [(r[0], r[1], r[2]) for r in batch]
            t0 = time.perf_counter()
            n = self.insert_batch(conn, batch_for_db)
            insert_latency_sec = time.perf_counter() - t0
            n_originals = sum(1 for r in batch if r[3])
            n_duplicates = len(batch) - n_originals
            with self.inserted_lock:
                self.inserted_shared[0] += n
                self.inserted_shared[1] += n_originals
                self.inserted_shared[2] += n_duplicates
                self.inserted_shared[3] += insert_latency_sec
            if self.queries_per_record > 0:
                insert_time = time.time()
                for mrn in _mrns_from_batch(batch):
                    self.query_queue.put((mrn, insert_time))
        finally:
            self.release_connection(conn)

    # Short poll interval so we don't block for batch_wait_sec when queue is empty (which would
    # throttle the producer and cap throughput at queue_size / batch_wait_sec).
    _GET_POLL_SEC = 0.1  # 100ms; workers wake often to drain queue and keep producer unblocked

    def run(self) -> None:
        """Run the worker loop: get records, batch (flush on batch_size or batch_wait_sec), insert, push MRNs. Stops on INSERTION_SENTINEL."""
        accumulated: Batch = []
        batch_start = time.perf_counter()
        while True:
            batch_elapsed = time.perf_counter() - batch_start
            # Never block longer than _GET_POLL_SEC so queue drain isn't starved by batch_wait_sec.
            timeout_sec = min(self._GET_POLL_SEC, max(0.001, self.batch_wait_sec - batch_elapsed))
            try:
                record = self.insertion_queue.get(timeout=timeout_sec)
            except queue.Empty:
                now = time.perf_counter()
                if accumulated and (now - batch_start) >= self.batch_wait_sec:
                    self._flush(accumulated)
                    accumulated = []
                    batch_start = now
                continue
            if record is INSERTION_SENTINEL:
                self.insertion_queue.task_done()
                if accumulated:
                    self._flush(accumulated)
                return
            self.insertion_queue.task_done()
            accumulated.append(record)
            if len(accumulated) >= self.batch_size:
                self._flush(accumulated)
                accumulated = []
                batch_start = time.perf_counter()
