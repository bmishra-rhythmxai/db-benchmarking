"""Producer: enqueues single records at target rate, or batch-by-batch on signal (round-robin)."""
from __future__ import annotations

import json
import queue
import random
import time
from multiprocessing.synchronize import Synchronized
from typing import Any, Union

from .config import INSERTION_SENTINEL
from .patient_generator import generate_one_patient, DUPLICATE_RATIO

Record = tuple[str, str, str, bool]

# Message type for rows built from generate_one_patient (patient record as message body)
PATIENT_MESSAGE_TYPE = "PATIENT"


def build_one_batch(
    batch_size: int,
    patient_start_base: int,
    next_id: Synchronized,
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
    insertion_queue: queue.Queue[Record | None],
    initial_batch: list[Record],
    batch_size: int,
    patient_start_base: int,
    next_id: Union[Synchronized, Any],
    recv_trigger: queue.Queue[None],
    send_trigger: queue.Queue[None],
    stop_event: Any,
    duplicate_ratio: float = DUPLICATE_RATIO,
) -> None:
    """Round-robin producer: wait for signal, put pre-built batch onto queue (only insertions between recv and send), signal next producer, then build next batch.
    Repeats until stop_event is set. Use build_one_batch in order before starting threads to pre-build initial batches."""
    batch = initial_batch
    while not stop_event.is_set():
        try:
            recv_trigger.get(timeout=0.1)
        except queue.Empty:
            continue
        if stop_event.is_set():
            break
        # Only insertions to the queue between recv and send (mimic Go).
        for record in batch:
            insertion_queue.put(record)
        send_trigger.put(None)
        # Build next batch for next iteration (outside the recv/send window).
        batch = build_one_batch(batch_size, patient_start_base, next_id, duplicate_ratio)


def run_producer(
    duration_sec: float,
    insertion_queue: queue.Queue[Record | None],
    num_workers: int,
    target_rps: int,
    patient_start_base: int,
    next_id: Synchronized,
    sentinels: int | None = None,
    max_records: int | None = None,
) -> None:
    """Enqueue single records at target_rps until duration_sec or max_records (if set), then enqueue sentinels.
    next_id is a shared multiprocessing.Value('q') holding the next patient ordinal (atomic counter).
    patient_start_base is the first ordinal in the range; duplicates are chosen in [patient_start_base, current_max].
    sentinels: number of INSERTION_SENTINEL to put at end; None = num_workers (single producer), 0 = none (multi-producer).
    max_records: if set, stop after this many records (rate-limited by target_rps); duration_sec is ignored when set."""
    if sentinels is None:
        sentinels = num_workers
    interval = 1.0 / target_rps if target_rps > 0 else 0.0
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
            if random.random() < DUPLICATE_RATIO:
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
            insertion_queue.put(record)
            count += 1
            next_put_at = next_put_at + interval
            if next_put_at < now:
                next_put_at = now + interval
        else:
            time.sleep(min(0.001, next_put_at - now))
    for _ in range(sentinels):
        insertion_queue.put(INSERTION_SENTINEL)
