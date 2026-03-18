"""
db_test.py — straight scenarios for robust PgBouncer testing.
Covers SELECT, INSERT, TRUNCATE with and without query hint /* pgbouncer.database = dbname */.

Usage: set DATABASE_URL (or PGBOUNCER_URL) then run:
  python db_test.py
  # or with unittest:
  python -m pytest db_test.py -v
  python -m unittest db_test -v

Requires: psycopg (sync). Install with: pip install 'psycopg[binary]'
"""

import os
import unittest

try:
    import psycopg
except ImportError:
    psycopg = None


TEST_TABLE = "pgbouncer_db_test_table"

# Matches scripts/psql_prepare_execute_insert.sql: same columns and ON CONFLICT.
# Uses 29 params (medical_record_number passed as param; SQL file uses current_setting('app.mrn', true)).
HL7_INSERT_SQL = """
INSERT INTO hl7_messages (
  fhir_id, rx_patient_id, source, cdc, created_at, created_by,
  updated_at, updated_by, load_date, checksum, patient_id,
  medical_record_number,
  name_prefix, last_name, first_name, name_suffix,
  date_of_birth, gender_administrative, fhir_gender_administrative,
  gender_identity, fhir_gender_identity, marital_status, fhir_marital_status,
  race_display, fhir_race_display, ethnicity_display, fhir_ethnicity_display,
  sex_at_birth, is_pregnant
) VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
  %s,
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (medical_record_number) DO UPDATE SET
  fhir_id = EXCLUDED.fhir_id,
  rx_patient_id = EXCLUDED.rx_patient_id,
  source = EXCLUDED.source,
  cdc = EXCLUDED.cdc,
  created_at = EXCLUDED.created_at,
  created_by = EXCLUDED.created_by,
  updated_at = EXCLUDED.updated_at,
  updated_by = EXCLUDED.updated_by,
  load_date = EXCLUDED.load_date,
  checksum = EXCLUDED.checksum,
  patient_id = EXCLUDED.patient_id,
  name_prefix = EXCLUDED.name_prefix,
  last_name = EXCLUDED.last_name,
  first_name = EXCLUDED.first_name,
  name_suffix = EXCLUDED.name_suffix,
  date_of_birth = EXCLUDED.date_of_birth,
  gender_administrative = EXCLUDED.gender_administrative,
  fhir_gender_administrative = EXCLUDED.fhir_gender_administrative,
  gender_identity = EXCLUDED.gender_identity,
  fhir_gender_identity = EXCLUDED.fhir_gender_identity,
  marital_status = EXCLUDED.marital_status,
  fhir_marital_status = EXCLUDED.fhir_marital_status,
  race_display = EXCLUDED.race_display,
  fhir_race_display = EXCLUDED.fhir_race_display,
  ethnicity_display = EXCLUDED.ethnicity_display,
  fhir_ethnicity_display = EXCLUDED.fhir_ethnicity_display,
  sex_at_birth = EXCLUDED.sex_at_birth,
  is_pregnant = EXCLUDED.is_pregnant
"""


def get_conninfo():
    if s := os.environ.get("DATABASE_URL"):
        return s
    if s := os.environ.get("PGBOUNCER_URL"):
        return s
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "6432")
    db = os.environ.get("PGDATABASE", "postgres")
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD", "")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def pgbouncer_database_hint_prefix(db_name: str) -> str:
    safe = db_name.replace("'", "''")
    return f"/* pgbouncer.database = '{safe}' */ "


def query_with_hint(db_name: str, sql: str) -> str:
    return pgbouncer_database_hint_prefix(db_name) + sql


@unittest.skipIf(psycopg is None, "psycopg not installed")
class PgBouncerDbTests(unittest.TestCase):
    """All combinations: SELECT / INSERT / TRUNCATE with and without query hint /* pgbouncer.database = dbname */.
    One connection is created for the class and reused for all tests."""

    @classmethod
    def setUpClass(cls):
        cls.conn = psycopg.connect(get_conninfo())
        cls.conn.autocommit = False

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "conn") and cls.conn is not None and not cls.conn.closed:
            cls.conn.close()

    def setUp(self):
        self.conn = self.__class__.conn

    def tearDown(self):
        if not self.conn.closed:
            self.conn.rollback()

    def _current_db(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            return cur.fetchone()[0]

    def _create_test_table(self):
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
                    id   SERIAL PRIMARY KEY,
                    val  TEXT
                )
                """
            )
        self.conn.commit()

    def _create_hl7_table(self):
        with self.conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS hl7_messages")
            cur.execute("""
                CREATE TABLE hl7_messages (
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
                ) PARTITION BY HASH (medical_record_number)
            """)
            for i in range(8):
                cur.execute(
                    f"CREATE TABLE hl7_messages_{i} PARTITION OF hl7_messages FOR VALUES WITH (MODULUS 8, REMAINDER {i})"
                )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_hl7_patient_id ON hl7_messages(patient_id)")
        self.conn.commit()

    @staticmethod
    def _hl7_insert_args(mrn: str):
        """Same 29 args as scripts/psql_prepare_execute_insert.sql EXECUTE (MRN as param)."""
        return (
            "patient-0000000001", "rx-patient-0000000001", "fake-hl7-source-payload", "CDC-EXAMPLE",
            "2025-03-17 12:00:00+00", "system", "2025-03-17 12:00:00+00", "system",
            "2025-03-17", "a1b2c3d4e5", "patient-0000000001",
            mrn,
            "Ms", "Doe", "Jane", None, "1990-05-15",
            "female", "female", "Female", "female", "Single", "S",
            "White", "2106-3", "Not Hispanic or Latino", "2186-5", "female", "false",
        )

    # ---- Without query hint ----

    def test_select_simple_without_set(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
        self.assertEqual(row[0], 1)

    def test_select_from_table_without_set(self):
        self._create_test_table()
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TEST_TABLE}")
            cur.fetchone()

    def test_insert_without_set(self):
        self._create_hl7_table()
        with self.conn.cursor() as cur:
            cur.execute(HL7_INSERT_SQL, self._hl7_insert_args("MRN-0000000001"))
        self.conn.commit()

    def test_truncate_without_set(self):
        self._create_test_table()
        with self.conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {TEST_TABLE}")
        self.conn.commit()

    def test_select_insert_truncate_without_set(self):
        self._create_test_table()
        self._create_hl7_table()
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1")
            self.assertEqual(cur.fetchone()[0], 1)
            cur.execute(HL7_INSERT_SQL, self._hl7_insert_args("MRN-0000000001"))
            cur.execute(f"TRUNCATE TABLE {TEST_TABLE}")
        self.conn.commit()

    def test_citus_check_without_set(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_extension WHERE extname = %s", ("citus",))
            cur.fetchone()  # no row is ok (Citus not installed)

    def test_multi_statement_without_set(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1; SELECT 2")
            while cur.nextset():
                pass

    # ---- With query hint /* pgbouncer.database = dbname */ ----

    def test_select_simple_with_set(self):
        db_name = self._current_db()
        with self.conn.cursor() as cur:
            cur.execute(query_with_hint(db_name, "SELECT 1"))
            self.assertEqual(cur.fetchone()[0], 1)

    def test_select_from_table_with_set(self):
        self._create_test_table()
        db_name = self._current_db()
        with self.conn.cursor() as cur:
            cur.execute(query_with_hint(db_name, f"SELECT COUNT(*) FROM {TEST_TABLE}"))
            cur.fetchone()

    def test_insert_with_set(self):
        self._create_hl7_table()
        db_name = self._current_db()
        with self.conn.cursor() as cur:
            cur.execute(query_with_hint(db_name, HL7_INSERT_SQL), self._hl7_insert_args("MRN-0000000001"))
        self.conn.commit()

    def test_truncate_with_set(self):
        self._create_test_table()
        db_name = self._current_db()
        with self.conn.cursor() as cur:
            cur.execute(query_with_hint(db_name, f"TRUNCATE TABLE {TEST_TABLE}"))
        self.conn.commit()

    def test_select_insert_truncate_with_set(self):
        self._create_test_table()
        self._create_hl7_table()
        db_name = self._current_db()
        with self.conn.cursor() as cur:
            cur.execute(query_with_hint(db_name, "SELECT 1"))
            self.assertEqual(cur.fetchone()[0], 1)
            cur.execute(query_with_hint(db_name, HL7_INSERT_SQL), self._hl7_insert_args("MRN-0000000001"))
            cur.execute(query_with_hint(db_name, f"TRUNCATE TABLE {TEST_TABLE}"))
        self.conn.commit()

    def test_citus_check_with_set(self):
        db_name = self._current_db()
        with self.conn.cursor() as cur:
            cur.execute(query_with_hint(db_name, "SELECT 1 FROM pg_extension WHERE extname = %s"), ("citus",))
            cur.fetchone()  # no row is ok

    def test_multi_statement_with_set(self):
        db_name = self._current_db()
        with self.conn.cursor() as cur:
            cur.execute(query_with_hint(db_name, "SELECT 1"))
            self.assertEqual(cur.fetchone()[0], 1)
            cur.execute(query_with_hint(db_name, "SELECT 2"))
            self.assertEqual(cur.fetchone()[0], 2)

    # ---- Combined: hint + query in one (single query string) ----

    def test_combined_set_and_select_with_set(self):
        db_name = self._current_db()
        q = query_with_hint(db_name, "SELECT 1")
        with self.conn.cursor() as cur:
            cur.execute(q)
            row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 1)

    def test_combined_set_and_insert_with_set(self):
        self._create_hl7_table()
        db_name = self._current_db()
        with self.conn.cursor() as cur:
            cur.execute(query_with_hint(db_name, HL7_INSERT_SQL), self._hl7_insert_args("MRN-0000000001"))
        self.conn.commit()

    def test_combined_set_and_truncate_with_set(self):
        self._create_test_table()
        db_name = self._current_db()
        q = query_with_hint(db_name, f"TRUNCATE TABLE {TEST_TABLE}")
        with self.conn.cursor() as cur:
            cur.execute(q)
        self.conn.commit()

    def test_combined_set_and_citus_check_with_set(self):
        db_name = self._current_db()
        q = query_with_hint(db_name, "SELECT 1 FROM pg_extension WHERE extname = 'citus'")
        with self.conn.cursor() as cur:
            cur.execute(q)
            cur.fetchone()  # no row is ok


if __name__ == "__main__":
    unittest.main()
