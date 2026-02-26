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
    prev_inserted = [0.0, 0.0, 0.0, 0.0]  # total, originals, duplicates, total_latency_sec
    prev_queries = 0.0
    prev_query_latency_sec = 0.0
    while not stop_event.is_set():
        stop_event.wait(interval_sec)
        if stop_event.is_set():
            break
        with inserted_lock:
            total = inserted_shared[0]
            originals = inserted_shared[1]
            duplicates = inserted_shared[2]
            total_insert_latency_sec = inserted_shared[3]
        # This interval (since last log)
        interval_total = int(total - prev_inserted[0])
        interval_originals = int(originals - prev_inserted[1])
        interval_duplicates = int(duplicates - prev_inserted[2])
        interval_latency_sec = total_insert_latency_sec - prev_inserted[3]
        prev_inserted[:] = [total, originals, duplicates, total_insert_latency_sec]
        interval_avg_insert_ms = (interval_latency_sec / interval_total * 1000.0) if interval_total > 0 else 0.0
        cumulative_avg_insert_ms = (total_insert_latency_sec / total * 1000.0) if total > 0 else 0.0
        logger.info(
            "Insert progress (this interval): %d total, %d original, %d duplicate, avg latency %.2f ms",
            interval_total, interval_originals, interval_duplicates, interval_avg_insert_ms,
        )
        logger.info(
            "Insert progress (cumulative): %d total, %d original, %d duplicate, avg latency %.2f ms",
            int(total), int(originals), int(duplicates), cumulative_avg_insert_ms,
        )
        with queries_lock:
            q = int(queries_shared[0])
            total_latency_sec = queries_shared[1]
        interval_q = q - int(prev_queries)
        interval_query_latency_sec = total_latency_sec - prev_query_latency_sec
        prev_queries = q
        prev_query_latency_sec = total_latency_sec
        avg_latency_ms = (total_latency_sec / q * 1000.0) if q > 0 else 0.0
        interval_avg_ms = (interval_query_latency_sec / interval_q * 1000.0) if interval_q > 0 else 0.0
        logger.info(
            "Query progress (this interval): %d queries, avg latency %.2f ms",
            interval_q, interval_avg_ms,
        )
        logger.info(
            "Query progress (cumulative): %d queries, avg latency %.2f ms",
            q, avg_latency_ms,
        )
