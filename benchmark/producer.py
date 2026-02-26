"""Producer: enqueues single records at target rate, then sends shutdown sentinels."""
from __future__ import annotations

import json
import queue
import time

from .config import INSERTION_SENTINEL
from .patient_generator import generate_bulk_patients

Record = tuple[str, str, str, bool]

# Message type for rows built from generate_bulk_patients (patient record as message body)
PATIENT_MESSAGE_TYPE = "PATIENT"


def _next_record(
    patient_records: list[dict],
    patient_count: int,
    patient_start: int,
) -> tuple[Record, int]:
    """Yield one record from patient_records; refill by calling generate_bulk_patients when empty.
    Returns (record, next patient_start)."""
    n_unique = max(1, int(patient_count * 0.75))  # match generate_bulk_patients default duplicate_ratio
    if not patient_records:
        patient_records.extend(
            generate_bulk_patients(total=patient_count, start=patient_start)
        )
        patient_start += n_unique
    p = patient_records.pop(0)
    is_original = p.pop("is_original", True)
    record: Record = (p["PATIENT_ID"], PATIENT_MESSAGE_TYPE, json.dumps(p, default=str), is_original)
    return record, patient_start


def run_producer(
    duration_sec: float,
    patient_count: int,
    insertion_queue: queue.Queue[Record | None],
    num_workers: int,
    target_rps: int,
    patient_start: int = 0,
    sentinels: int | None = None,
) -> None:
    """Enqueue single records at target_rps until duration_sec, then enqueue sentinels.
    patient_start is the starting counter for MRN/patient IDs (e.g. from get_max_patient_counter + 1).
    sentinels: number of INSERTION_SENTINEL to put at end; None = num_workers (single producer), 0 = none (multi-producer)."""
    if sentinels is None:
        sentinels = num_workers
    interval = 1.0 / target_rps if target_rps > 0 else 0.0
    start = time.perf_counter()
    next_put_at = start
    patient_records: list[dict] = []
    while time.perf_counter() - start < duration_sec:
        now = time.perf_counter()
        if now >= next_put_at:
            record, patient_start = _next_record(
                patient_records, patient_count, patient_start
            )
            insertion_queue.put(record)
            next_put_at = next_put_at + interval
            if next_put_at < now:
                next_put_at = now + interval
        else:
            time.sleep(min(0.001, next_put_at - now))
    for _ in range(sentinels):
        insertion_queue.put(INSERTION_SENTINEL)
