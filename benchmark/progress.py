"""Progress logger: periodically logs insert and query counts while run is active."""
from __future__ import annotations

import logging
import queue
import threading
from typing import Any

logger = logging.getLogger(__name__)

WHITE = "\033[37m"
YELLOW = "\033[33m"
RESET = "\033[0m"

# Tuple: (total, originals, duplicates, total_insert_latency_sec, query_count, query_latency_sec)
ProgressStats = tuple[float, float, float, float, float, float]


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
        with queries_lock:
            q = int(queries_shared[0])
            total_latency_sec = queries_shared[1]
        # This interval (since last log)
        interval_total = int(total - prev_inserted[0])
        interval_originals = int(originals - prev_inserted[1])
        interval_duplicates = int(duplicates - prev_inserted[2])
        interval_latency_sec = total_insert_latency_sec - prev_inserted[3]
        prev_inserted[:] = [total, originals, duplicates, total_insert_latency_sec]
        interval_avg_insert_ms = (interval_latency_sec / interval_total * 1000.0) if interval_total > 0 else 0.0
        cumulative_avg_insert_ms = (total_insert_latency_sec / total * 1000.0) if total > 0 else 0.0
        interval_q = q - int(prev_queries)
        interval_query_latency_sec = total_latency_sec - prev_query_latency_sec
        prev_queries = q
        prev_query_latency_sec = total_latency_sec
        avg_latency_ms = (total_latency_sec / q * 1000.0) if q > 0 else 0.0
        interval_avg_ms = (interval_query_latency_sec / interval_q * 1000.0) if interval_q > 0 else 0.0
        # This interval first
        logger.info("---")
        logger.info(
            "%sInsert progress (this interval): %d total, %d original, %d duplicate, avg latency %.2f ms%s",
            WHITE, interval_total, interval_originals, interval_duplicates, interval_avg_insert_ms, RESET,
        )
        logger.info(
            "%sQuery progress (this interval): %d queries, avg latency %.2f ms%s",
            WHITE, interval_q, interval_avg_ms, RESET,
        )
        logger.info("---")
        # Then cumulative
        logger.info(
            "%sInsert progress (cumulative): %d total, %d original, %d duplicate, avg latency %.2f ms%s",
            YELLOW, int(total), int(originals), int(duplicates), cumulative_avg_insert_ms, RESET,
        )
        logger.info(
            "%sQuery progress (cumulative): %d queries, avg latency %.2f ms%s",
            YELLOW, q, avg_latency_ms, RESET,
        )


def _log_aggregated(
    interval_total: int,
    interval_originals: int,
    interval_duplicates: int,
    interval_avg_insert_ms: float,
    interval_q: int,
    interval_avg_query_ms: float,
    total: float,
    originals: float,
    duplicates: float,
    cumulative_avg_insert_ms: float,
    q: float,
    avg_query_latency_ms: float,
) -> None:
    """Print one aggregated progress block (interval + cumulative)."""
    logger.info("---")
    logger.info(
        "%sInsert progress (this interval): %d total, %d original, %d duplicate, avg latency %.2f ms%s",
        WHITE, interval_total, interval_originals, interval_duplicates, interval_avg_insert_ms, RESET,
    )
    logger.info(
        "%sQuery progress (this interval): %d queries, avg latency %.2f ms%s",
        WHITE, interval_q, interval_avg_query_ms, RESET,
    )
    logger.info("---")
    logger.info(
        "%sInsert progress (cumulative): %d total, %d original, %d duplicate, avg latency %.2f ms%s",
        YELLOW, int(total), int(originals), int(duplicates), cumulative_avg_insert_ms, RESET,
    )
    logger.info(
        "%sQuery progress (cumulative): %d queries, avg latency %.2f ms%s",
        YELLOW, int(q), avg_query_latency_ms, RESET,
    )


def run_aggregated_progress_logger(
    progress_queue: Any,
    num_processes: int,
    stop_event: threading.Event,
    interval_sec: float = 5.0,
) -> None:
    """Run in the parent process: drain progress_queue from children, aggregate across processes, log combined progress."""
    last_stats: dict[int, ProgressStats] = {i: (0.0, 0.0, 0.0, 0.0, 0.0, 0.0) for i in range(num_processes)}
    prev_total = 0.0
    prev_originals = 0.0
    prev_duplicates = 0.0
    prev_insert_latency = 0.0
    prev_queries = 0.0
    prev_query_latency = 0.0
    while not stop_event.is_set():
        stop_event.wait(interval_sec)
        if stop_event.is_set():
            break
        # Drain all available updates from children
        try:
            while True:
                msg = progress_queue.get_nowait()
                process_index, stats = msg
                last_stats[process_index] = stats
        except queue.Empty:
            pass
        total = sum(s[0] for s in last_stats.values())
        originals = sum(s[1] for s in last_stats.values())
        duplicates = sum(s[2] for s in last_stats.values())
        total_insert_latency_sec = sum(s[3] for s in last_stats.values())
        query_count = sum(s[4] for s in last_stats.values())
        total_query_latency_sec = sum(s[5] for s in last_stats.values())
        interval_total = int(total - prev_total)
        interval_originals = int(originals - prev_originals)
        interval_duplicates = int(duplicates - prev_duplicates)
        interval_insert_latency_sec = total_insert_latency_sec - prev_insert_latency
        interval_q = int(query_count - prev_queries)
        interval_query_latency_sec = total_query_latency_sec - prev_query_latency
        prev_total, prev_originals, prev_duplicates = total, originals, duplicates
        prev_insert_latency = total_insert_latency_sec
        prev_queries = query_count
        prev_query_latency = total_query_latency_sec
        interval_avg_insert_ms = (interval_insert_latency_sec / interval_total * 1000.0) if interval_total > 0 else 0.0
        cumulative_avg_insert_ms = (total_insert_latency_sec / total * 1000.0) if total > 0 else 0.0
        interval_avg_query_ms = (interval_query_latency_sec / interval_q * 1000.0) if interval_q > 0 else 0.0
        avg_query_latency_ms = (total_query_latency_sec / query_count * 1000.0) if query_count > 0 else 0.0
        _log_aggregated(
            interval_total, interval_originals, interval_duplicates, interval_avg_insert_ms,
            interval_q, interval_avg_query_ms,
            total, originals, duplicates, cumulative_avg_insert_ms,
            query_count, avg_query_latency_ms,
        )


# First update after this many seconds so parent progress shows non-zero soon
_PROGRESS_REPORTER_FIRST_INTERVAL_SEC = 1.0


def run_progress_reporter(
    process_index: int,
    progress_queue: Any,
    inserted_lock: threading.Lock,
    inserted_shared: list[float],
    queries_lock: threading.Lock,
    queries_shared: list[float],
    progress_stop: threading.Event,
    interval_sec: float = 5.0,
) -> None:
    """Run in a child process: push (process_index, stats) to progress_queue. First update after 1s, then every interval_sec."""
    first_interval = min(_PROGRESS_REPORTER_FIRST_INTERVAL_SEC, interval_sec)
    if progress_stop.wait(first_interval):
        return
    with inserted_lock:
        ins = (inserted_shared[0], inserted_shared[1], inserted_shared[2], inserted_shared[3])
    with queries_lock:
        q = (queries_shared[0], queries_shared[1])
    progress_queue.put((process_index, (*ins, q[0], q[1])))
    while not progress_stop.is_set():
        if progress_stop.wait(interval_sec):
            break
        with inserted_lock:
            ins = (inserted_shared[0], inserted_shared[1], inserted_shared[2], inserted_shared[3])
        with queries_lock:
            q = (queries_shared[0], queries_shared[1])
        progress_queue.put((process_index, (*ins, q[0], q[1])))
