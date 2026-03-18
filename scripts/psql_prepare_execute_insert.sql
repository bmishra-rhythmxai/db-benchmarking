-- PREPARE / EXECUTE insert for hl7_messages with ID parameterization via session variable
-- Matches benchmark-go and benchmark_python INSERT (upsert by medical_record_number).
-- Use session variable app.mrn for medical_record_number so you set once and EXECUTE many times.
-- (PostgreSQL custom session values use set_config/current_setting; SET only works for known GUCs.)
-- Use query hint so PgBouncer routes this session to postgres2.

-- 1) Set the ID (medical_record_number) once per session or per batch:
/* pgbouncer.database = postgres2 */ SELECT set_config('app.mrn', 'MRN-0000000001', true);

-- 2) Prepare the insert (28 params: all columns except medical_record_number, which comes from app.mrn):
PREPARE ins_hl7 (
  text, text, text, text, timestamptz, text, timestamptz, text, text, text, text,
  text, text, text, text, text, text, text, text, text, text, text, text, text,
  text, text, text, text
) AS
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
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
  current_setting('app.mrn', true),
  $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28
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
  is_pregnant = EXCLUDED.is_pregnant;

-- 3) Execute with 28 arguments (same MRN from app.mrn). Fake data matches benchmark patient generator style:
EXECUTE ins_hl7(
  'patient-0000000001',           -- fhir_id
  'rx-patient-0000000001',        -- rx_patient_id
  'fake-hl7-source-payload',      -- source
  'CDC-EXAMPLE',                  -- cdc
  '2025-03-17 12:00:00+00',       -- created_at
  'system',                       -- created_by
  '2025-03-17 12:00:00+00',       -- updated_at
  'system',                       -- updated_by
  '2025-03-17',                   -- load_date
  'a1b2c3d4e5',                   -- checksum
  'patient-0000000001',           -- patient_id
  'Ms',                           -- name_prefix
  'Doe',                          -- last_name
  'Jane',                         -- first_name
  NULL,                           -- name_suffix
  '1990-05-15',                   -- date_of_birth
  'female',                       -- gender_administrative
  'female',                       -- fhir_gender_administrative
  'Female',                       -- gender_identity
  'female',                       -- fhir_gender_identity
  'Single',                       -- marital_status
  'S',                            -- fhir_marital_status
  'White',                        -- race_display
  '2106-3',                       -- fhir_race_display
  'Not Hispanic or Latino',       -- ethnicity_display
  '2186-5',                       -- fhir_ethnicity_display
  'female',                       -- sex_at_birth
  'false'                         -- is_pregnant (28th arg)
);

-- Reuse same ID for another row (e.g. duplicate or different payload): set again and EXECUTE:
-- SELECT set_config('app.mrn', 'MRN-0000000001', true);
-- EXECUTE ins_hl7(...);

-- Optional: also parameterize patient_id from a session variable (e.g. same as MRN or derived):
-- SELECT set_config('app.patient_id', 'patient-1', true);
-- Then in PREPARE use current_setting('app.patient_id', true) for patient_id and 23 params.

DEALLOCATE ins_hl7;
