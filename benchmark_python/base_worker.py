"""Base insert worker: async; consume full batches, insert originals then duplicates."""
from __future__ import annotations

import asyncio
import json
import logging
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


def _split_originals_duplicates(batch: Batch) -> tuple[Batch, Batch]:
    """Split batch into originals and duplicates (match Go InsertPair)."""
    originals: Batch = []
    duplicates: Batch = []
    for r in batch:
        if r[3]:  # is_original
            originals.append(r)
        else:
            duplicates.append(r)
    return originals, duplicates


class BaseAsyncInsertWorker(ABC):
    """Async base: consume full batches from asyncio.Queue; insert originals then duplicates."""

    def __init__(
        self,
        insertion_queue: asyncio.Queue[Batch | None],
        query_queue: asyncio.Queue[Any],
        inserted_lock: asyncio.Lock,
        inserted_shared: list[float],
        batch_size: int,
        queries_per_record: int = 1,
    ) -> None:
        self.insertion_queue = insertion_queue
        self.query_queue = query_queue
        self.inserted_lock = inserted_lock
        self.inserted_shared = inserted_shared
        self.batch_size = batch_size
        self.queries_per_record = queries_per_record

    @abstractmethod
    async def get_connection(self) -> Any:
        ...

    @abstractmethod
    async def release_connection(self, conn: Any) -> None:
        ...

    @abstractmethod
    async def insert_batch(self, conn: Any, batch: list[tuple[str, str, str]]) -> tuple[int, int]:
        """Insert batch. Returns (rows_inserted, statement_count)."""
        ...

    async def _flush(self, batch: Batch) -> None:
        if not batch:
            return
        originals, duplicates = _split_originals_duplicates(batch)
        async with self.inserted_lock:
            self.inserted_shared[5] += 1
        conn = await self.get_connection()
        try:
            total_rows = 0
            total_latency_sec = 0.0
            n_statements = 0
            if originals:
                rows_db = [(r[0], r[1], r[2]) for r in originals]
                t0 = time.perf_counter()
                n, stmts = await self.insert_batch(conn, rows_db)
                total_rows += n
                total_latency_sec += time.perf_counter() - t0
                n_statements += stmts
            if duplicates:
                rows_db = [(r[0], r[1], r[2]) for r in duplicates]
                t0 = time.perf_counter()
                n, stmts = await self.insert_batch(conn, rows_db)
                total_rows += n
                total_latency_sec += time.perf_counter() - t0
                n_statements += stmts
            n_originals = len(originals)
            n_duplicates = len(duplicates)
            async with self.inserted_lock:
                self.inserted_shared[0] += total_rows
                self.inserted_shared[1] += n_originals
                self.inserted_shared[2] += n_duplicates
                self.inserted_shared[3] += total_latency_sec
                self.inserted_shared[4] += n_statements
            if self.queries_per_record > 0:
                insert_time = time.time()
                for mrn in _mrns_from_batch(batch):
                    await self.query_queue.put((mrn, insert_time))
        finally:
            async with self.inserted_lock:
                self.inserted_shared[5] -= 1
                if self.inserted_shared[5] < 0:
                    self.inserted_shared[5] = 0
            await self.release_connection(conn)

    async def run(self) -> None:
        while True:
            item = await self.insertion_queue.get()
            if item is INSERTION_SENTINEL:
                return
            async with self.inserted_lock:
                self.inserted_shared[6] += 1
            await self._flush(item)
