"""
Dynamic patient generation (from clickhouse-poc).
Generates patient records with configurable count and duplicate ratio in the required schema.
"""
from __future__ import annotations

import random
import string
from typing import Any

# Pre-generate 100 payloads at startup to avoid slow per-call generation
_PAYLOAD_POOL_SIZE = 100
_PAYLOAD_SIZE = 2 * 1024 * 1024  # 2 MiB
_PAYLOAD_POOL: list[str] = [
    "".join(random.choices(string.ascii_letters + string.digits, k=_PAYLOAD_SIZE))
    for _ in range(_PAYLOAD_POOL_SIZE)
]


DUPLICATE_RATIO = 0.25


def generate_one_patient(ordinal: int, is_original: bool) -> dict[str, Any]:
    """Generate a single patient record for the given ordinal."""
    first_names = (
        "John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"
    )
    last_names = (
        "Smith", "Doe", "Brown", "Johnson", "Williams", "Jones", "Garcia", "Miller", "Davis", "Wilson"
    )
    genders = ("male", "female", "other")
    base_source = random.choice(_PAYLOAD_POOL)
    ord_str = f"{ordinal:010d}"
    mrn = f"MRN-{ord_str}"
    pid = f"patient-{ord_str}"
    return {
        "is_original": is_original,
        "FHIR_ID": pid,
        "RX_PATIENT_ID": f"rx-{pid}",
        "SOURCE": base_source,
        "PATIENT_ID": pid,
        "MEDICAL_RECORD_NUMBER": mrn,
        "NAME_PREFIX": "Mr" if ordinal % 2 == 0 else "Ms",
        "LAST_NAME": last_names[ordinal % len(last_names)],
        "FIRST_NAME": first_names[ordinal % len(first_names)],
        "NAME_SUFFIX": "Jr" if ordinal % 4 == 0 else None,
        "DATE_OF_BIRTH": f"{1980 + (ordinal % 40)}-{(ordinal % 12) + 1:02d}-{(ordinal % 28) + 1:02d}",
        "GENDER_ADMINISTRATIVE": genders[ordinal % 3],
        "FHIR_GENDER_ADMINISTRATIVE": genders[ordinal % 3],
        "GENDER_IDENTITY": genders[ordinal % 3].capitalize(),
        "FHIR_GENDER_IDENTITY": genders[ordinal % 3],
        "MARITAL_STATUS": "Married" if ordinal % 2 == 0 else "Single",
        "FHIR_MARITAL_STATUS": "M" if ordinal % 2 == 0 else "S",
        "RACE_DISPLAY": "White" if ordinal % 3 == 0 else "Black or African American",
        "FHIR_RACE_DISPLAY": "2106-3" if ordinal % 3 == 0 else "2054-5",
        "ETHNICITY_DISPLAY": "Not Hispanic or Latino",
        "FHIR_ETHNICITY_DISPLAY": "2186-5",
        "SEX_AT_BIRTH": "male" if ordinal % 2 == 0 else "female",
        "IS_PREGNANT": "false",
    }


def generate_bulk_patients(
    start: int = 0,
    total: int = 1000,
    duplicate_ratio: float = 0.25,
) -> list[dict[str, Any]]:
    """Generate `total` patient records with duplicates in the required schema.
    `start` is the starting counter for MRN/patient IDs so each call can produce a different range."""
    n_unique = max(1, int(total * (1 - duplicate_ratio)))
    patients: list[dict[str, Any]] = [
        generate_one_patient(start + i, True) for i in range(n_unique)
    ]
    base_source = random.choice(_PAYLOAD_POOL)
    n_duplicates = total - n_unique
    for j in range(n_duplicates):
        patients.append(patients[j % n_unique].copy())
        patients[-1]["SOURCE"] = base_source
        patients[-1]["is_original"] = False
    return patients
