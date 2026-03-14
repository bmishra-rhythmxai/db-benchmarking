"""PgBouncer pipeline mode: use psycopg3 connection.pipeline() to send SET + INSERT in one round-trip."""
from __future__ import annotations

from typing import Any

from ..config import PASSWORD, USER

# Reuse column list and row mapping from async backend
from .backend_async import _HL7_COLUMNS, _row_from_producer_tuple

try:
    import psycopg
    from psycopg_pool import ConnectionPool
except ImportError:
    psycopg = None  # type: ignore[assignment]
    ConnectionPool = None  # type: ignore[assignment, misc]


def create_psycopg_pool(host: str, port: int, size: int, database: str) -> Any:
    """Create a sync psycopg3 connection pool (for pipeline mode when pgbouncer enabled)."""
    if ConnectionPool is None or psycopg is None:
        raise RuntimeError("psycopg and psycopg_pool are required for pipeline mode; install psycopg[binary,pool]")
    conninfo = f"host={host} port={port} user={USER} password={PASSWORD} dbname={database}"
    return ConnectionPool(conninfo=conninfo, min_size=size, max_size=size)


def insert_batch_pipeline_sync(
    conn: Any,
    rows: list[tuple[str, str, str]],
    database: str,
) -> int:
    """Run SET pgbouncer.database + INSERT in one round-trip using connection.pipeline(). Returns rows inserted."""
    if not rows or psycopg is None:
        return 0
    mapped = [_row_from_producer_tuple(r) for r in rows]
    cols = ", ".join(_HL7_COLUMNS)
    n = len(_HL7_COLUMNS)
    update_cols = [c for c in _HL7_COLUMNS if c != "medical_record_number"]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    # psycopg uses %s placeholders
    args: list[Any] = []
    placeholders_list = []
    for r in range(len(mapped)):
        base = r * n
        placeholders_list.append("(" + ", ".join("%s" for _ in range(n)) + ")")
        args.extend(mapped[r])
    values_sql = ", ".join(placeholders_list)
    insert_sql = (
        f"INSERT INTO hl7_messages ({cols}) VALUES {values_sql} "
        f"ON CONFLICT (medical_record_number) DO UPDATE SET {set_clause}"
    )
    with conn.pipeline():
        conn.execute("SET pgbouncer.database = %s", [database])
        conn.execute(insert_sql, args)
    return len(rows)


def insert_batch_pipeline_with_pool(
    pool: Any,
    rows: list[tuple[str, str, str]],
    database: str,
) -> int:
    """Get a connection from the pool, run SET + INSERT in pipeline, return rows inserted."""
    if not rows:
        return 0
    with pool.connection() as conn:
        return insert_batch_pipeline_sync(conn, rows, database)
