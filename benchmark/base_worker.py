"""Base insert worker: consume batches from queue, insert, push MRNs to query queue (query workers run in parallel)."""
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

Batch = list[tuple[str, str, str]]
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
    """Base class for database insert workers. Subclasses implement connection and insert_batch. Pushes MRNs to query_queue."""

    def __init__(
        self,
        insertion_queue: queue.Queue[Batch | None],
        query_queue: queue.Queue[str | Any],
        inserted_lock: threading.Lock,
        inserted_shared: list[int],
    ) -> None:
        self.insertion_queue = insertion_queue
        self.query_queue = query_queue
        self.inserted_lock = inserted_lock
        self.inserted_shared = inserted_shared

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

    def run(self) -> None:
        """Run the worker loop: get batch, insert, push each MRN to query_queue. Stops on INSERTION_SENTINEL."""
        while True:
            batch = self.insertion_queue.get()
            if batch is INSERTION_SENTINEL:
                self.insertion_queue.task_done()
                return
            try:
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
            finally:
                self.insertion_queue.task_done()
