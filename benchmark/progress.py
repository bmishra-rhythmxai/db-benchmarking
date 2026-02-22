"""Progress logger: periodically logs insert and query counts while run is active."""
from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)


def run_progress_logger(
    inserted_lock: threading.Lock,
    inserted_shared: list[int],
    stop_event: threading.Event,
    queries_lock: threading.Lock,
    queries_shared: list[int],
    interval_sec: float = 5.0,
) -> None:
    """Log inserted and query counts every interval_sec until stop_event is set."""
    while not stop_event.is_set():
        stop_event.wait(interval_sec)
        if stop_event.is_set():
            break
        with inserted_lock:
            n = inserted_shared[0]
        logger.info("Insert progress: %d rows inserted", n)
        with queries_lock:
            q = queries_shared[0]
        logger.info("Query progress: %d queries executed", q)
