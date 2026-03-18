"""Producer: enqueues batches at target rate (async)."""
from __future__ import annotations

import asyncio
import json
import random
import threading
import time
from typing import Any

from .config import INSERTION_SENTINEL
from .patient_generator import generate_one_patient, DUPLICATE_RATIO

Record = tuple[str, str, str, bool]


class SyncCounter:
    """Thread-safe counter for use from async (single process). Has .get_lock() and .value like multiprocessing.Value."""

    def __init__(self, initial: int = 0) -> None:
        self._lock = threading.Lock()
        self._value = initial

    def get_lock(self) -> threading.Lock:
        return self._lock

    @property
    def value(self) -> int:
        return self._value

    @value.setter
    def value(self, v: int) -> None:
        self._value = v

# Message type for rows built from generate_one_patient (patient record as message body)
PATIENT_MESSAGE_TYPE = "PATIENT"


def build_one_batch(
    batch_size: int,
    patient_start_base: int,
    batch_index: int,
    duplicate_ratio: float = DUPLICATE_RATIO,
) -> list[Record]:
    """Build one batch of records. Patient ordinals are derived from batch_index (deterministic, no next_id contention).
    Originals at patient_start_base + batch_index*batch_size + i; duplicates random in [patient_start_base, base). Batch 0 has no duplicate range.
    """
    batch: list[Record] = []
    base = patient_start_base + batch_index * batch_size
    dup_end = base  # exclusive upper bound for duplicate ordinals
    for i in range(batch_size):
        if random.random() < duplicate_ratio and dup_end > patient_start_base:
            n = dup_end - patient_start_base
            ordinal = patient_start_base + (random.randint(0, n - 1) if n > 1 else 0)
            is_original = False
        else:
            ordinal = base + i
            is_original = True
        p = generate_one_patient(ordinal, is_original)
        record: Record = (
            p["PATIENT_ID"],
            PATIENT_MESSAGE_TYPE,
            json.dumps(p, default=str),
            p["is_original"],
        )
        batch.append(record)
    return batch


def build_query_hint(idx: int, batch: list[Record], pgbouncer_enabled: bool) -> str:
    """Build the single query hint string to prepend to the INSERT (two separate comments: pgbouncer.database, pgbouncer.patient_ids). Only originals are included in patient_ids."""
    if not pgbouncer_enabled:
        return ""
    db = "postgres1" if (idx % 2) == 0 else "postgres2"
    safe_db = db.replace("'", "''")
    prefix = f"/* pgbouncer.database = '{safe_db}' */ "
    originals_only = [r[0] for r in batch if r[3]]  # r[3] is is_original
    if originals_only:
        patient_ids = ",".join(originals_only)
        safe_ids = patient_ids.replace("'", "''")
        prefix += f"/* pgbouncer.patient_ids = '{safe_ids}' */ "
    return prefix


async def run_batch_producer(
    insertion_queue: asyncio.Queue[tuple[str, list[Record]] | None],  # (query_hint, batch)
    batch_size: int,
    patient_start_base: int,
    next_batch_index: SyncCounter,
    recv_trigger: asyncio.Queue[None],
    send_trigger: asyncio.Queue[None],
    stop_event: asyncio.Event,
    duplicate_ratio: float = DUPLICATE_RATIO,
    insert_rate_limiter: Any = None,
    pgbouncer_enabled: bool = False,
) -> None:
    """Async round-robin producer: rate-limit then put (target_db, batch). Batch is built from batch index (deterministic patient ordinals)."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(recv_trigger.get(), timeout=0.1)
        except asyncio.TimeoutError:
            continue
        if stop_event.is_set():
            break
        if insert_rate_limiter is not None:
            await insert_rate_limiter.acquire(batch_size)
        with next_batch_index.get_lock():
            idx = next_batch_index.value
            next_batch_index.value += 1
        batch = build_one_batch(batch_size, patient_start_base, idx, duplicate_ratio)
        query_hint = build_query_hint(idx, batch, pgbouncer_enabled)
        await insertion_queue.put((query_hint, batch))
        send_trigger.put_nowait(None)


async def run_producer(
    insertion_queue: asyncio.Queue[tuple[str, list[Record]] | None],
    num_workers: int,
    target_rps: int,
    batch_size: int,
    patient_start_base: int,
    next_batch_index: SyncCounter,
    sentinels: int,
    max_records: int | None,
    duplicate_ratio: float = DUPLICATE_RATIO,
    pgbouncer_enabled: bool = False,
) -> None:
    """Async producer: enqueue (target_db, batch) at target_rps until max_records reached. Batch built from batch index (deterministic)."""
    interval_per_batch = batch_size / target_rps if target_rps > 0 and batch_size > 0 else 0.0
    start = time.perf_counter()
    next_put_at = start
    count = 0
    while True:
        if max_records is not None and count >= max_records:
            break
        remaining = max_records - count if max_records is not None else batch_size
        this_batch_size = min(batch_size, remaining) if max_records is not None else batch_size
        if this_batch_size <= 0:
            break
        now = time.perf_counter()
        if now < next_put_at:
            await asyncio.sleep(min(0.01, next_put_at - now))
            continue
        with next_batch_index.get_lock():
            idx = next_batch_index.value
            next_batch_index.value += 1
        batch = build_one_batch(this_batch_size, patient_start_base, idx, duplicate_ratio)
        query_hint = build_query_hint(idx, batch, pgbouncer_enabled)
        await insertion_queue.put((query_hint, batch))
        count += len(batch)
        next_put_at = next_put_at + interval_per_batch
        if next_put_at < time.perf_counter():
            next_put_at = time.perf_counter() + interval_per_batch
    for _ in range(sentinels):
        await insertion_queue.put(INSERTION_SENTINEL)
