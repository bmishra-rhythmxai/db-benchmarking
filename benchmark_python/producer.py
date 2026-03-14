"""Producer: enqueues batches at target rate (sync or async)."""
from __future__ import annotations

import asyncio
import json
import queue
import random
import threading
import time
from typing import Any, Union

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
    """Build one batch of records (same logic as run_producer). Call in order for each producer before starting threads so build order is deterministic."""
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


def run_batch_producer(
    insertion_queue: queue.Queue[list[Record] | None],
    initial_batch: list[Record],
    batch_size: int,
    patient_start_base: int,
    next_id: Any,
    recv_trigger: queue.Queue[None],
    send_trigger: queue.Queue[None],
    stop_event: Any,
    duplicate_ratio: float = DUPLICATE_RATIO,
) -> None:
    """Round-robin producer: wait for signal, put pre-built batch (one item) onto queue, signal next producer, then build next batch.
    Repeats until stop_event is set. Use build_one_batch in order before starting threads to pre-build initial batches."""
    batch = initial_batch
    while not stop_event.is_set():
        try:
            recv_trigger.get(timeout=0.1)
        except queue.Empty:
            continue
        if stop_event.is_set():
            break
        insertion_queue.put(batch)
        send_trigger.put(None)
        batch = build_one_batch(batch_size, patient_start_base, next_id, duplicate_ratio)


def run_producer(
    duration_sec: float,
    insertion_queue: queue.Queue[list[Record] | None],
    num_workers: int,
    target_rps: int,
    batch_size: int,
    patient_start_base: int,
    next_id: Any,
    sentinels: int | None = None,
    max_records: int | None = None,
    duplicate_ratio: float = DUPLICATE_RATIO,
) -> None:
    """Enqueue full batches at target_rps until duration_sec or max_records (if set), then enqueue sentinels.
    next_id is a shared multiprocessing.Value('q') holding the next patient ordinal (atomic counter).
    patient_start_base is the first ordinal in the range; duplicates are chosen in [patient_start_base, current_max].
    sentinels: number of INSERTION_SENTINEL to put at end; None = num_workers.
    max_records: if set, stop after this many records (rate-limited by target_rps); duration_sec is ignored when set."""
    if sentinels is None:
        sentinels = num_workers
    # Rate-limit by batch: wait so that batch_size rows take batch_size/target_rps seconds.
    interval_per_batch = batch_size / target_rps if target_rps > 0 and batch_size > 0 else 0.0
    start = time.perf_counter()
    next_put_at = start
    count = 0
    while True:
        if max_records is not None and count >= max_records:
            break
        if max_records is None and time.perf_counter() - start >= duration_sec:
            break
        now = time.perf_counter()
        if now >= next_put_at:
            remaining = max_records - count if max_records is not None else batch_size
            this_batch_size = min(batch_size, remaining) if max_records is not None else batch_size
            if this_batch_size <= 0:
                break
            batch = build_one_batch(this_batch_size, patient_start_base, next_id, duplicate_ratio)
            insertion_queue.put(batch)
            count += len(batch)
            next_put_at = next_put_at + interval_per_batch
            if next_put_at < now:
                next_put_at = now + interval_per_batch
        else:
            time.sleep(min(0.001, next_put_at - now))
    for _ in range(sentinels):
        insertion_queue.put(INSERTION_SENTINEL)


# --- Async producers (asyncio.Queue, single process) ---


async def async_run_batch_producer(
    insertion_queue: asyncio.Queue[list[Record] | None],
    initial_batch: list[Record],
    batch_size: int,
    patient_start_base: int,
    next_id: Any,
    recv_trigger: asyncio.Queue[None],
    send_trigger: asyncio.Queue[None],
    stop_event: asyncio.Event,
    duplicate_ratio: float = DUPLICATE_RATIO,
) -> None:
    """Async round-robin producer: wait for signal, put batch, signal next, build next. Repeats until stop_event is set."""
    batch = initial_batch
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(recv_trigger.get(), timeout=0.1)
        except asyncio.TimeoutError:
            continue
        if stop_event.is_set():
            break
        await insertion_queue.put(batch)
        send_trigger.put_nowait(None)
        batch = build_one_batch(batch_size, patient_start_base, next_id, duplicate_ratio)


async def async_run_producer(
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
