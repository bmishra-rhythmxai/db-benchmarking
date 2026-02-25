"""Progress logger: periodically logs insert and query counts while run is active."""
from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)


def run_progress_logger(
    inserted_lock: threading.Lock,
    inserted_shared: list[float],
    stop_event: threading.Event,
    queries_lock: threading.Lock,
    queries_shared: list[float],
    interval_sec: float = 5.0,
) -> None:
    """Log inserted and query counts every interval_sec until stop_event is set."""
    while not stop_event.is_set():
        stop_event.wait(interval_sec)
        if stop_event.is_set():
            break
        with inserted_lock:
            total = inserted_shared[0]
            originals = inserted_shared[1]
            duplicates = inserted_shared[2]
            total_insert_latency_sec = inserted_shared[3]
        avg_insert_ms = (total_insert_latency_sec / total * 1000.0) if total > 0 else 0.0
        logger.info(
            "Insert progress: %d rows inserted (%d original, %d duplicate) (avg latency %.2f ms)",
            total, originals, duplicates, avg_insert_ms,
        )
        with queries_lock:
            q = int(queries_shared[0])
            total_latency_sec = queries_shared[1]
        avg_latency_ms = (total_latency_sec / q * 1000.0) if q > 0 else 0.0
        logger.info(
            "Query progress: %d queries executed (avg latency %.2f ms)",
            q, avg_latency_ms,
        )
