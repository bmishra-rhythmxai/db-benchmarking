"""PostgreSQL backend (asyncpg): pool, schema init, batch insert, query."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import asyncpg

from ..config import DB_NAME, PASSWORD, USER

logger = logging.getLogger(__name__)

_HL7_COLUMNS = (
    "fhir_id", "rx_patient_id", "source", "cdc", "created_at", "created_by",
    "updated_at", "updated_by", "load_date", "checksum", "patient_id",
    "medical_record_number", "name_prefix", "last_name", "first_name", "name_suffix",
    "date_of_birth", "gender_administrative", "fhir_gender_administrative",
    "gender_identity", "fhir_gender_identity", "marital_status", "fhir_marital_status",
    "race_display", "fhir_race_display", "ethnicity_display", "fhir_ethnicity_display",
    "sex_at_birth", "is_pregnant",
)

HASH_PARTITION_MODULUS = 8


def _row_from_producer_tuple(t: tuple[str, str, str]) -> tuple:
    _pid, _msg_type, json_str = t
    d = json.loads(json_str)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return (
        d.get("FHIR_ID"),
        d.get("RX_PATIENT_ID"),
        d.get("SOURCE"),
        d.get("CDC"),
        d.get("CREATED_AT") or now,
        d.get("CREATED_BY"),
        d.get("UPDATED_AT") or now,
        d.get("UPDATED_BY"),
        d.get("LOAD_DATE"),
        d.get("CHECKSUM"),
        d.get("PATIENT_ID"),
        d.get("MEDICAL_RECORD_NUMBER"),
        d.get("NAME_PREFIX"),
        d.get("LAST_NAME"),
        d.get("FIRST_NAME"),
        d.get("NAME_SUFFIX"),
        d.get("DATE_OF_BIRTH"),
        d.get("GENDER_ADMINISTRATIVE"),
        d.get("FHIR_GENDER_ADMINISTRATIVE"),
        d.get("GENDER_IDENTITY"),
        d.get("FHIR_GENDER_IDENTITY"),
        d.get("MARITAL_STATUS"),
        d.get("FHIR_MARITAL_STATUS"),
        d.get("RACE_DISPLAY"),
        d.get("FHIR_RACE_DISPLAY"),
        d.get("ETHNICITY_DISPLAY"),
        d.get("FHIR_ETHNICITY_DISPLAY"),
        d.get("SEX_AT_BIRTH"),
        d.get("IS_PREGNANT"),
    )


async def create_pool(
    host: str,
    port: int,
    size: int,
    database: str = DB_NAME,
    *,
    statement_cache_size: int | None = None,
) -> asyncpg.Pool:
    kwargs: dict[str, Any] = {
        "host": host,
        "port": port,
        "user": USER,
        "password": PASSWORD,
        "database": database,
        "min_size": size,
        "max_size": size,
        "command_timeout": 60,
    }
    if statement_cache_size is not None:
        kwargs["statement_cache_size"] = statement_cache_size
    pool = await asyncpg.create_pool(**kwargs)
    return pool


async def prewarm_pool(pool: asyncpg.Pool, size: int, skip_session_set: bool = False) -> None:
    """Prewarm pool by acquiring/releasing connections. skip_session_set=True for PgBouncer (avoids SET that can desync asyncpg protocol)."""
    if not skip_session_set:
        async with pool.acquire() as conn:
            await conn.execute("SET synchronous_commit = off")
    conns = []
    for _ in range(size - 1):
        conns.append(await pool.acquire())
    for c in conns:
        await pool.release(c)
    logger.info("Prewarmed PostgreSQL pool (%d connections)", size)


async def init_schema(conn: asyncpg.Connection) -> None:
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS hl7_messages (
            fhir_id TEXT,
            rx_patient_id TEXT,
            source TEXT,
            cdc TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            created_by TEXT,
            updated_at TIMESTAMPTZ NOT NULL,
            updated_by TEXT,
            load_date TEXT,
            checksum TEXT,
            patient_id TEXT,
            medical_record_number TEXT NOT NULL,
            name_prefix TEXT,
            last_name TEXT,
            first_name TEXT,
            name_suffix TEXT,
            date_of_birth TEXT,
            gender_administrative TEXT,
            fhir_gender_administrative TEXT,
            gender_identity TEXT,
            fhir_gender_identity TEXT,
            marital_status TEXT,
            fhir_marital_status TEXT,
            race_display TEXT,
            fhir_race_display TEXT,
            ethnicity_display TEXT,
            fhir_ethnicity_display TEXT,
            sex_at_birth TEXT,
            is_pregnant TEXT,
            PRIMARY KEY (medical_record_number)
        ) PARTITION BY HASH (medical_record_number);
    """)
    for i in range(HASH_PARTITION_MODULUS):
        await conn.execute(
            f"CREATE TABLE IF NOT EXISTS hl7_messages_{i} PARTITION OF hl7_messages "
            f"FOR VALUES WITH (MODULUS {HASH_PARTITION_MODULUS}, REMAINDER {i})"
        )
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_hl7_patient_id ON hl7_messages(patient_id);")
    try:
        await conn.execute("ALTER TABLE hl7_messages ALTER COLUMN source SET COMPRESSION lz4;")
    except asyncpg.PostgresError:
        pass
    row = await conn.fetchrow("SELECT 1 FROM pg_extension WHERE extname = 'citus'")
    if row:
        row2 = await conn.fetchrow("SELECT 1 FROM citus_tables WHERE tablename = 'hl7_messages'")
        if not row2:
            await conn.execute("SELECT create_distributed_table('hl7_messages', 'medical_record_number')")
            logger.info("Citus: distributed hl7_messages by medical_record_number")
    logger.info("Table hl7_messages created with hash partitioning (modulus %d)", HASH_PARTITION_MODULUS)


def _build_insert_sql_and_args(
    rows: list[tuple[str, str, str]],
    *,
    placeholder_start: int = 1,
) -> tuple[str, list[Any]]:
    """Build parameterized INSERT SQL and flat args for hl7_messages. Returns (sql, args).
    placeholder_start: first placeholder number (default 1; use 2 when prepending SET pgbouncer.database = $1)."""
    if not rows:
        return "", []
    mapped = [_row_from_producer_tuple(r) for r in rows]
    cols = ", ".join(_HL7_COLUMNS)
    n = len(_HL7_COLUMNS)
    update_cols = [c for c in _HL7_COLUMNS if c != "medical_record_number"]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    args: list[Any] = []
    for row in mapped:
        args.extend(row)
    placeholders_list = []
    for r in range(len(mapped)):
        base = r * n + placeholder_start
        placeholders_list.append("(" + ", ".join(f"${base + j}" for j in range(n)) + ")")
    values_sql = ", ".join(placeholders_list)
    sql = (
        f"INSERT INTO hl7_messages ({cols}) VALUES {values_sql} "
        f"ON CONFLICT (medical_record_number) DO UPDATE SET {set_clause}"
    )
    return sql, args


async def insert_batch(conn: asyncpg.Connection, rows: list[tuple[str, str, str]]) -> int:
    """Insert rows with a single connection. Returns number of rows inserted."""
    if not rows:
        return 0
    sql, args = _build_insert_sql_and_args(rows)
    await conn.execute(sql, *args)
    return len(rows)


# PgBouncer: flip-flop between postgres1 and postgres2 per batch; SET (simple) prepended to parameterized INSERT.
PGBOUNCER_DB1 = "postgres1"
PGBOUNCER_DB2 = "postgres2"


async def insert_batch_with_pgbouncer_set(
    conn: asyncpg.Connection,
    rows: list[tuple[str, str, str]],
    database: str,
) -> int:
    """Execute SET pgbouncer.database = '...'; INSERT ... in one round-trip. PgBouncer strips SET, forwards INSERT. Returns rows inserted."""
    if not rows:
        return 0
    if database not in (PGBOUNCER_DB1, PGBOUNCER_DB2):
        raise ValueError(f"pgbouncer database must be {PGBOUNCER_DB1!r} or {PGBOUNCER_DB2!r}, got {database!r}")
    safe_db = database.replace("'", "''")
    insert_sql, insert_args = _build_insert_sql_and_args(rows, placeholder_start=1)
    combined_sql = f"SET pgbouncer.database = '{safe_db}'; " + insert_sql
    await conn.execute(combined_sql, *insert_args)
    return len(rows)


async def query_by_primary_key(conn: asyncpg.Connection, medical_record_number: str) -> list:
    rows = await conn.fetch(
        "SELECT * FROM hl7_messages WHERE medical_record_number = $1",
        medical_record_number,
    )
    return [dict(r) for r in rows]


async def get_max_patient_counter(conn: asyncpg.Connection) -> int:
    row = await conn.fetchrow(
        "SELECT COALESCE(MAX(CAST(SUBSTRING(patient_id FROM 10) AS BIGINT)), -1) FROM hl7_messages WHERE patient_id IS NOT NULL AND patient_id ~ '^patient-[0-9]+$'"
    )
    return int(row[0]) if row and row[0] is not None else -1


async def init_schema_standalone(host: str, port: int, database: str = DB_NAME) -> None:
    conn = await asyncpg.connect(
        host=host,
        port=port,
        user=USER,
        password=PASSWORD,
        database=database,
    )
    try:
        await init_schema(conn)
    finally:
        await conn.close()


async def get_max_patient_counter_standalone(host: str, port: int, database: str = DB_NAME) -> int:
    try:
        conn = await asyncpg.connect(
            host=host,
            port=port,
            user=USER,
            password=PASSWORD,
            database=database,
        )
        try:
            return await get_max_patient_counter(conn)
        finally:
            await conn.close()
    except asyncpg.PostgresError:
        return -1
