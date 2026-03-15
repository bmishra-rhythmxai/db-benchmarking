"""ClickHouse backend (asynch): pool, schema init, batch insert, query."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from asynch import Connection

from ..config import (
    CLICKHOUSE_CLUSTER,
    CLICKHOUSE_STORAGE_POLICY,
    DB_NAME,
    PASSWORD,
    USER,
)

logger = logging.getLogger(__name__)

_HL7_COLUMNS = (
    "FHIR_ID", "RX_PATIENT_ID", "SOURCE", "CDC", "CREATED_AT", "CREATED_BY",
    "UPDATED_AT", "UPDATED_BY", "LOAD_DATE", "CHECKSUM", "PATIENT_ID",
    "MEDICAL_RECORD_NUMBER", "NAME_PREFIX", "LAST_NAME", "FIRST_NAME", "NAME_SUFFIX",
    "DATE_OF_BIRTH", "GENDER_ADMINISTRATIVE", "FHIR_GENDER_ADMINISTRATIVE",
    "GENDER_IDENTITY", "FHIR_GENDER_IDENTITY", "MARITAL_STATUS", "FHIR_MARITAL_STATUS",
    "RACE_DISPLAY", "FHIR_RACE_DISPLAY", "ETHNICITY_DISPLAY", "FHIR_ETHNICITY_DISPLAY",
    "SEX_AT_BIRTH", "IS_PREGNANT",
)


async def create_pool(host: str, port: int, size: int) -> list[Connection]:
    """Create a list of asynch connections (use as a pool via queue)."""
    connections: list[Connection] = []
    for _ in range(size):
        conn = Connection(
            host=host,
            port=port,
            user=USER,
            password=PASSWORD,
            database=DB_NAME,
        )
        await conn.connect()
        connections.append(conn)
    return connections


async def prewarm_pool(connections: list[Connection]) -> None:
    """Prewarm by executing a no-op on each connection."""
    for conn in connections:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
    logger.info("Prewarmed ClickHouse pool (%d connections)", len(connections))


async def close_pool(connections: list[Connection]) -> None:
    """Close all connections in the pool."""
    for conn in connections:
        await conn.close()


async def init_schema(conn: Connection) -> None:
    cluster = CLICKHOUSE_CLUSTER
    async with conn.cursor() as cursor:
        await cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} ON CLUSTER '{cluster}'")
        policy = CLICKHOUSE_STORAGE_POLICY
        await cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {DB_NAME}.hl7_messages_local ON CLUSTER '{cluster}' (
                FHIR_ID Nullable(String),
                RX_PATIENT_ID Nullable(String),
                SOURCE Nullable(String),
                CDC Nullable(String),
                CREATED_AT DateTime64(3),
                CREATED_BY Nullable(String),
                UPDATED_AT DateTime64(3),
                UPDATED_BY Nullable(String),
                LOAD_DATE Nullable(String),
                CHECKSUM Nullable(String),
                PATIENT_ID Nullable(String),
                MEDICAL_RECORD_NUMBER String,
                NAME_PREFIX Nullable(String),
                LAST_NAME Nullable(String),
                FIRST_NAME Nullable(String),
                NAME_SUFFIX Nullable(String),
                DATE_OF_BIRTH Nullable(String),
                GENDER_ADMINISTRATIVE Nullable(String),
                FHIR_GENDER_ADMINISTRATIVE Nullable(String),
                GENDER_IDENTITY Nullable(String),
                FHIR_GENDER_IDENTITY Nullable(String),
                MARITAL_STATUS Nullable(String),
                FHIR_MARITAL_STATUS Nullable(String),
                RACE_DISPLAY Nullable(String),
                FHIR_RACE_DISPLAY Nullable(String),
                ETHNICITY_DISPLAY Nullable(String),
                FHIR_ETHNICITY_DISPLAY Nullable(String),
                SEX_AT_BIRTH Nullable(String),
                IS_PREGNANT Nullable(String)
            )
            ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{shard}}/hl7_messages_local', '{{replica}}', UPDATED_AT)
            ORDER BY MEDICAL_RECORD_NUMBER
            SETTINGS storage_policy = '{policy}'
        """)
        await cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {DB_NAME}.hl7_messages ON CLUSTER '{cluster}' (
                FHIR_ID Nullable(String),
                RX_PATIENT_ID Nullable(String),
                SOURCE Nullable(String),
                CDC Nullable(String),
                CREATED_AT DateTime64(3),
                CREATED_BY Nullable(String),
                UPDATED_AT DateTime64(3),
                UPDATED_BY Nullable(String),
                LOAD_DATE Nullable(String),
                CHECKSUM Nullable(String),
                PATIENT_ID Nullable(String),
                MEDICAL_RECORD_NUMBER String,
                NAME_PREFIX Nullable(String),
                LAST_NAME Nullable(String),
                FIRST_NAME Nullable(String),
                NAME_SUFFIX Nullable(String),
                DATE_OF_BIRTH Nullable(String),
                GENDER_ADMINISTRATIVE Nullable(String),
                FHIR_GENDER_ADMINISTRATIVE Nullable(String),
                GENDER_IDENTITY Nullable(String),
                FHIR_GENDER_IDENTITY Nullable(String),
                MARITAL_STATUS Nullable(String),
                FHIR_MARITAL_STATUS Nullable(String),
                RACE_DISPLAY Nullable(String),
                FHIR_RACE_DISPLAY Nullable(String),
                ETHNICITY_DISPLAY Nullable(String),
                FHIR_ETHNICITY_DISPLAY Nullable(String),
                SEX_AT_BIRTH Nullable(String),
                IS_PREGNANT Nullable(String)
            )
            ENGINE = Distributed('{cluster}', '{DB_NAME}', hl7_messages_local, sipHash64(MEDICAL_RECORD_NUMBER))
        """)
    logger.info("Cluster tables hl7_messages created (ClickHouse)")


def _row_from_producer_tuple(t: tuple[str, str, str]) -> tuple:
    """Convert (patient_id, message_type, json_message) to a row tuple matching hl7_messages schema."""
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


async def insert_batch(conn: Connection, rows: list[tuple[str, str, str]]) -> int:
    """Insert batch of (patient_id, message_type, json_message) by mapping to hl7_messages columns."""
    if not rows:
        return 0
    mapped = [_row_from_producer_tuple(r) for r in rows]
    cols = ", ".join(_HL7_COLUMNS)
    async with conn.cursor() as cursor:
        cursor.set_settings({
            "insert_quorum": 2,
            "insert_quorum_parallel": 1,
            "distributed_foreground_insert": 1,
            "async_insert": 0,
        })
        await cursor.execute(
            f"INSERT INTO {DB_NAME}.hl7_messages ({cols}) VALUES",
            mapped,
        )
    return len(rows)


async def query_by_primary_key(conn: Connection, medical_record_number: str) -> list:
    """Query hl7_messages by primary key MEDICAL_RECORD_NUMBER."""
    async with conn.cursor() as cursor:
        cursor.set_settings({"select_sequential_consistency": 1, "prefer_localhost_replica": 0})
        await cursor.execute(
            f"SELECT * FROM {DB_NAME}.hl7_messages FINAL WHERE MEDICAL_RECORD_NUMBER = %(mrn)s",
            {"mrn": medical_record_number},
        )
        result = await cursor.fetchall()
    return list(result) if result else []


async def get_max_patient_counter(conn: Connection) -> int:
    """Return the maximum patient ordinal in hl7_messages, or -1 if empty."""
    async with conn.cursor() as cursor:
        cursor.set_settings({"select_sequential_consistency": 1, "prefer_localhost_replica": 0})
        await cursor.execute(
            f"SELECT COALESCE(MAX(toInt64OrZero(substring(PATIENT_ID, 10))), -1) FROM {DB_NAME}.hl7_messages WHERE PATIENT_ID != ''"
        )
        result = await cursor.fetchall()
    if not result or result[0][0] is None:
        return -1
    return int(result[0][0])


async def init_schema_standalone(host: str, port: int) -> None:
    """Create a single connection, run init_schema, close. Used by parent process."""
    conn = Connection(
        host=host,
        port=port,
        user=USER,
        password=PASSWORD,
        database=DB_NAME,
    )
    await conn.connect()
    try:
        await init_schema(conn)
    finally:
        await conn.close()


async def get_max_patient_counter_standalone(host: str, port: int) -> int:
    """Create a single connection, read max patient counter, close. Returns -1 if table missing or empty."""
    try:
        conn = Connection(
            host=host,
            port=port,
            user=USER,
            password=PASSWORD,
            database=DB_NAME,
        )
        await conn.connect()
        try:
            return await get_max_patient_counter(conn)
        finally:
            await conn.close()
    except Exception:
        return -1
