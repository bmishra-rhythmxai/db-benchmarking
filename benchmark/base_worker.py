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

Record = tuple[str, str, str]
Batch = list[Record]
Conn = TypeVar("Conn")


def _mrns_from_batch(batch: Batch) -> list[str]:
    """Extract MEDICAL_RECORD_NUMBER from each record in the batch. Skips invalid/empty; logs and continues."""
    mrns: list[str] = []
    for _pid, _msg_type, json_str in batch:
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
        inserted_shared: list[int],
        batch_size: int,
        batch_wait_sec: float,
    ) -> None:
        self.insertion_queue = insertion_queue
        self.query_queue = query_queue
        self.inserted_lock = inserted_lock
        self.inserted_shared = inserted_shared
        self.batch_size = batch_size
        self.batch_wait_sec = batch_wait_sec

    @abstractmethod
    def get_connection(self) -> Any:
        """Acquire a connection/client for this thread. Must be paired with release_connection."""
        ...

    @abstractmethod
    def release_connection(self, conn: Any) -> None:
        """Return the connection/client to the pool."""
        ...

    @abstractmethod
    def insert_batch(self, conn: Any, batch: Batch) -> int:
        """Insert the batch using the given connection. Return number of rows inserted."""
        ...

    def _flush(self, batch: Batch) -> None:
        """Insert batch, update inserted count, push MRNs to query_queue."""
        if not batch:
            return
        conn = self.get_connection()
        try:
            n = self.insert_batch(conn, batch)
            with self.inserted_lock:
                self.inserted_shared[0] += n
            insert_time = time.time()
            for mrn in _mrns_from_batch(batch):
                self.query_queue.put((mrn, insert_time))
        finally:
            self.release_connection(conn)

    def run(self) -> None:
        """Run the worker loop: get records, batch (flush on batch_size or batch_wait_sec), insert, push MRNs. Stops on INSERTION_SENTINEL."""
        accumulated: Batch = []
        batch_start = time.perf_counter()
        while True:
            timeout_sec = max(0.001, self.batch_wait_sec - (time.perf_counter() - batch_start))
            try:
                record = self.insertion_queue.get(timeout=timeout_sec)
            except queue.Empty:
                if accumulated:
                    self._flush(accumulated)
                    accumulated = []
                batch_start = time.perf_counter()
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
