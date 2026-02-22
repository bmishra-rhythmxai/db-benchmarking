"""PostgreSQL connection pool, schema init, and batch insert for hl7_messages."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from ..config import DB_NAME, PASSWORD, USER

logger = logging.getLogger(__name__)


def create_pool(host: str, port: int, size: int):
    import psycopg2.pool
    return psycopg2.pool.ThreadedConnectionPool(
        minconn=size,
        maxconn=size,
        host=host,
        port=port,
        user=USER,
        password=PASSWORD,
        dbname=DB_NAME,
    )


def prewarm_pool(pool, size: int) -> None:
    """Prewarm the pool by acquiring and releasing each connection."""
    conns = []
    for _ in range(size):
        conns.append(pool.getconn())
    for c in conns:
        pool.putconn(c)
    logger.info("Prewarmed PostgreSQL connection pool (%d connections)", size)


# Column order matches ClickHouse hl7_messages; used to build rows from producer (patient_id, type, json).
_HL7_COLUMNS = (
    "fhir_id", "rx_patient_id", "source", "cdc", "created_at", "created_by",
    "updated_at", "updated_by", "load_date", "checksum", "patient_id",
    "medical_record_number", "name_prefix", "last_name", "first_name", "name_suffix",
    "date_of_birth", "gender_administrative", "fhir_gender_administrative",
    "gender_identity", "fhir_gender_identity", "marital_status", "fhir_marital_status",
    "race_display", "fhir_race_display", "ethnicity_display", "fhir_ethnicity_display",
    "sex_at_birth", "is_pregnant",
)


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


def init_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS hl7_messages;
            CREATE TABLE hl7_messages (
                id BIGSERIAL PRIMARY KEY,
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
                is_pregnant TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_hl7_patient_id ON hl7_messages(patient_id);
            CREATE INDEX IF NOT EXISTS idx_hl7_medical_record_number ON hl7_messages(medical_record_number);
        """)
    conn.commit()
    logger.info("Table hl7_messages created (PostgreSQL)")


def insert_batch(conn, rows: list[tuple[str, str, str]]) -> int:
    """Insert batch of (patient_id, message_type, json_message) by mapping to hl7_messages columns."""
    if not rows:
        return 0
    mapped = [_row_from_producer_tuple(r) for r in rows]
    cols = ", ".join(_HL7_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_HL7_COLUMNS))
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO hl7_messages ({cols}) VALUES ({placeholders})",
            mapped,
        )
    conn.commit()
    return len(rows)


def query_by_primary_key(conn, medical_record_number: str) -> list:
    """Query hl7_messages by primary key medical_record_number. Returns list of rows (0 or more)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM hl7_messages WHERE medical_record_number = %s",
            (medical_record_number,),
        )
        return cur.fetchall()
