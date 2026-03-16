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
    next_id: Any,
    duplicate_ratio: float = DUPLICATE_RATIO,
) -> list[Record]:
    """Build one batch of records (same logic as run_producer). Call in order for each producer so build order is deterministic."""
    batch: list[Record] = []
    for _ in range(batch_size):
        if random.random() < duplicate_ratio:
            with next_id.get_lock():
                existing_max = next_id.value - 1
            if existing_max < patient_start_base:
                with next_id.get_lock():
                    ordinal = next_id.value
                    next_id.value += 1
                is_original = True
            else:
                ordinal = patient_start_base + random.randint(
                    0, existing_max - patient_start_base
                )
                is_original = False
        else:
            with next_id.get_lock():
                ordinal = next_id.value
                next_id.value += 1
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


async def run_batch_producer(
    insertion_queue: asyncio.Queue[list[Record] | None],
    initial_batch: list[Record],
    batch_size: int,
    patient_start_base: int,
    next_id: Any,
    recv_trigger: asyncio.Queue[None],
    send_trigger: asyncio.Queue[None],
    stop_event: asyncio.Event,
    duplicate_ratio: float = DUPLICATE_RATIO,
    insert_rate_limiter: Any = None,
) -> None:
    """Async round-robin producer: rate-limit then put batch (match Go Router). Repeats until stop_event is set."""
    batch = initial_batch
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(recv_trigger.get(), timeout=0.1)
        except asyncio.TimeoutError:
            continue
        if stop_event.is_set():
            break
        if insert_rate_limiter is not None:
            await insert_rate_limiter.acquire(len(batch))
        await insertion_queue.put(batch)
        send_trigger.put_nowait(None)
        batch = build_one_batch(batch_size, patient_start_base, next_id, duplicate_ratio)


async def run_producer(
    insertion_queue: asyncio.Queue[list[Record] | None],
    num_workers: int,
    target_rps: int,
    batch_size: int,
    patient_start_base: int,
    next_id: Any,
    sentinels: int,
    max_records: int | None,
    duplicate_ratio: float = DUPLICATE_RATIO,
) -> None:
    """Async producer: enqueue full batches at target_rps until max_records reached, then put sentinels. next_id is SyncCounter."""
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
        batch = build_one_batch(this_batch_size, patient_start_base, next_id, duplicate_ratio)
        await insertion_queue.put(batch)
        count += len(batch)
        next_put_at = next_put_at + interval_per_batch
        if next_put_at < time.perf_counter():
            next_put_at = time.perf_counter() + interval_per_batch
    for _ in range(sentinels):
        await insertion_queue.put(INSERTION_SENTINEL)
