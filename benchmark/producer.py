"""Producer: enqueues batches at target rate, then sends shutdown sentinels."""
from __future__ import annotations

import json
import queue
import time

from .config import INSERTION_SENTINEL
from .patient_generator import generate_bulk_patients

Batch = list[tuple[str, str, str]]

# Message type for rows built from generate_bulk_patients (patient record as message body)
PATIENT_MESSAGE_TYPE = "PATIENT"


def _next_batch(
    patient_records: list[dict],
    batch_size: int,
    patient_count: int,
    patient_start: int,
) -> tuple[Batch, int]:
    """Consume up to batch_size records from patient_records; refill by calling generate_bulk_patients when empty.
    Returns (rows, next patient_start)."""
    rows: list[tuple[str, str, str]] = []
    n_unique = max(1, int(patient_count * 0.75))  # match generate_bulk_patients default duplicate_ratio
    while len(rows) < batch_size:
        if not patient_records:
            patient_records.extend(
                generate_bulk_patients(total=patient_count, start=patient_start)
            )
            patient_start += n_unique
        n = min(batch_size - len(rows), len(patient_records))
        for p in patient_records[:n]:
            rows.append((p["PATIENT_ID"], PATIENT_MESSAGE_TYPE, json.dumps(p, default=str)))
        del patient_records[:n]
    return rows, patient_start


def run_producer(
    duration_sec: float,
    batch_size: int,
    patient_count: int,
    insertion_queue: queue.Queue[Batch | None],
    num_workers: int,
    target_rps: int,
) -> None:
    """Enqueue batches at target_rps until duration_sec, then enqueue num_workers sentinels.
    Calls generate_bulk_patients repeatedly when the local patient list runs out."""
    interval = batch_size / target_rps
    start = time.perf_counter()
    next_batch_at = start
    patient_records: list[dict] = []
    patient_start = 0
    while time.perf_counter() - start < duration_sec:
        now = time.perf_counter()
        if now >= next_batch_at:
            rows, patient_start = _next_batch(
                patient_records, batch_size, patient_count, patient_start
            )
            insertion_queue.put(rows)
            next_batch_at = next_batch_at + interval
            if next_batch_at < now:
                next_batch_at = now + interval
        else:
            time.sleep(min(0.001, next_batch_at - now))
    for _ in range(num_workers):
        insertion_queue.put(INSERTION_SENTINEL)
