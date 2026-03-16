"""Progress logger: periodically logs insert and query counts while run is active (async)."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

RESET = "\033[0m"
DIM = "\033[2m"
CYAN = "\033[36m"
YELLOW = "\033[33m"
GREEN = "\033[32m"
WHITE = "\033[37m"

COL_W = 12
PREFIX = "           "  # 11 chars to align with "  Insert   " / "  Query    "


def _pad_left(s: str, w: int) -> str:
    """Right-align s in column of width w (pad with spaces on the left)."""
    if len(s) >= w:
        return s
    return s.rjust(w)


def _fmt_insert_header() -> str:
    """Insert row: status (incoming, completed) | interval (int_) | cumulative (cum_)."""
    return (
        f"{YELLOW}  Insert   "
        f"{_pad_left('incoming', COL_W)}{_pad_left('completed', COL_W)} "
        f"{_pad_left('int_tot', COL_W)}{_pad_left('int_orig', COL_W)}"
        f"{_pad_left('int_dup', COL_W)}{_pad_left('int_avg_ms', COL_W)} "
        f"{_pad_left('cum_tot', COL_W)}{_pad_left('cum_orig', COL_W)}"
        f"{_pad_left('cum_dup', COL_W)}{_pad_left('cum_avg_ms', COL_W)}"
        f"{RESET}"
    )


def _fmt_insert_data(
    incoming: int,
    completed: int,
    interval_total: int,
    interval_originals: int,
    interval_duplicates: int,
    interval_avg_insert_ms: float,
    total: int,
    originals: int,
    duplicates: int,
    cumulative_avg_insert_ms: float,
) -> str:
    """One data row for Insert: incoming, completed, then interval and cumulative (same as Go)."""
    return (
        f"{PREFIX}"
        f"{CYAN}{incoming:>{COL_W}d}{RESET}{CYAN}{completed:>{COL_W}d}{RESET} "
        f"{CYAN}{interval_total:>{COL_W}d}{RESET}{CYAN}{interval_originals:>{COL_W}d}{RESET}"
        f"{CYAN}{interval_duplicates:>{COL_W}d}{RESET}{CYAN}{interval_avg_insert_ms:>{COL_W}.2f}{RESET} "
        f"{CYAN}{total:>{COL_W}d}{RESET}{CYAN}{originals:>{COL_W}d}{RESET}"
        f"{CYAN}{duplicates:>{COL_W}d}{RESET}{CYAN}{cumulative_avg_insert_ms:>{COL_W}.2f}{RESET}"
    )


def _fmt_query_header() -> str:
    """Query row: interval (int_) | cumulative (cum_)."""
    return (
        f"{YELLOW}  Query    "
        f"{_pad_left('int_queries', COL_W)}{_pad_left('int_failed', COL_W)}{_pad_left('int_avg_ms', COL_W)} "
        f"{_pad_left('cum_queries', COL_W)}{_pad_left('cum_failed', COL_W)}{_pad_left('cum_avg_ms', COL_W)}"
        f"{RESET}"
    )


def _fmt_query_data(
    interval_q: int,
    interval_failed: int,
    interval_avg_ms: float,
    q: int,
    failed: int,
    avg_latency_ms: float,
) -> str:
    """One data row for Query: 6 right-aligned values, cyan."""
    return (
        f"{PREFIX}"
        f"{CYAN}{interval_q:>{COL_W}d}{RESET}{CYAN}{interval_failed:>{COL_W}d}{RESET}"
        f"{CYAN}{interval_avg_ms:>{COL_W}.2f}{RESET} "
        f"{CYAN}{q:>{COL_W}.0f}{RESET}{CYAN}{failed:>{COL_W}.0f}{RESET}"
        f"{CYAN}{avg_latency_ms:>{COL_W}.2f}{RESET}"
    )

async def run_progress_logger(
    inserted_lock: asyncio.Lock,
    inserted_shared: list[float],
    stop_event: asyncio.Event,
    queries_lock: asyncio.Lock,
    queries_shared: list[float],
    interval_sec: float = 5.0,
) -> None:
    """Log insert/query counts every interval_sec until stop_event is set (same columns as Go)."""
    prev_inserted = [0.0, 0.0, 0.0, 0.0, 0.0]
    prev_insert_started = 0.0
    prev_queries = 0.0
    prev_query_latency_sec = 0.0
    prev_failed = 0.0
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
        except asyncio.TimeoutError:
            pass
        if stop_event.is_set():
            break
        async with inserted_lock:
            total = inserted_shared[0]
            originals = inserted_shared[1]
            duplicates = inserted_shared[2]
            total_insert_latency_sec = inserted_shared[3]
            insert_statements = inserted_shared[4]
            cur_insert_started = inserted_shared[6]
        async with queries_lock:
            q = int(queries_shared[0])
            total_latency_sec = queries_shared[1]
            failed = queries_shared[2]
        interval_insert_started = int(cur_insert_started - prev_insert_started)
        prev_insert_started = cur_insert_started
        interval_total = int(total - prev_inserted[0])
        interval_originals = int(originals - prev_inserted[1])
        interval_duplicates = int(duplicates - prev_inserted[2])
        interval_latency_sec = total_insert_latency_sec - prev_inserted[3]
        interval_statements = int(insert_statements - prev_inserted[4])
        prev_inserted[:] = [total, originals, duplicates, total_insert_latency_sec, insert_statements]
        interval_avg_insert_ms = (interval_latency_sec / interval_total * 1000.0) if interval_total > 0 else 0.0
        cumulative_avg_insert_ms = (total_insert_latency_sec / total * 1000.0) if total > 0 else 0.0
        interval_q = q - int(prev_queries)
        interval_query_latency_sec = total_latency_sec - prev_query_latency_sec
        interval_failed = int(failed - prev_failed)
        prev_queries = q
        prev_query_latency_sec = total_latency_sec
        prev_failed = failed
        avg_latency_ms = (total_latency_sec / q * 1000.0) if q > 0 else 0.0
        interval_avg_ms = (interval_query_latency_sec / interval_q * 1000.0) if interval_q > 0 else 0.0
        logger.info("%s---%s", DIM, RESET)
        logger.info("%s", _fmt_insert_header())
        logger.info(
            "%s",
            _fmt_insert_data(
                interval_insert_started,
                interval_statements,
                interval_total,
                interval_originals,
                interval_duplicates,
                interval_avg_insert_ms,
                int(total),
                int(originals),
                int(duplicates),
                cumulative_avg_insert_ms,
            ),
        )
        logger.info("%s", _fmt_query_header())
        logger.info(
            "%s",
            _fmt_query_data(interval_q, interval_failed, interval_avg_ms, q, int(failed), avg_latency_ms),
        )
