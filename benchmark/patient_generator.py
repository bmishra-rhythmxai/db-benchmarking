"""
Dynamic patient generation (from clickhouse-poc).
Generates patient records with configurable count and duplicate ratio in the required schema.
"""
from __future__ import annotations

import random
import string
from typing import Any


def generate_bulk_patients(
    start: int = 0,
    total: int = 1000,
    duplicate_ratio: float = 0.25,
) -> list[dict[str, Any]]:
    """Generate `total` patient records with duplicates in the required schema.
    `start` is the starting counter for MRN/patient IDs so each call can produce a different range."""
    n_unique = max(1, int(total * (1 - duplicate_ratio)))
    first_names = (
        "John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"
    )
    last_names = (
        "Smith", "Doe", "Brown", "Johnson", "Williams", "Jones", "Garcia", "Miller", "Davis", "Wilson"
    )
    genders = ("male", "female", "other")
    patients: list[dict[str, Any]] = []
    base_source = "".join(
        random.choices(string.ascii_letters + string.digits, k=2 * 1024 * 1024)
    )

    for i in range(n_unique):
        fn = first_names[i % len(first_names)]
        ln = last_names[i % len(last_names)]
        ordinal = f"{start + i:010d}"
        mrn = f"MRN-{ordinal}"
        pid = f"patient-{ordinal}"
        patients.append({
            "is_original": True,
            "FHIR_ID": pid,
            "RX_PATIENT_ID": f"rx-{pid}",
            "SOURCE": base_source,
            "PATIENT_ID": pid,
            "MEDICAL_RECORD_NUMBER": mrn,
            "NAME_PREFIX": "Mr" if i % 2 == 0 else "Ms",
            "LAST_NAME": ln,
            "FIRST_NAME": fn,
            "NAME_SUFFIX": "Jr" if i % 4 == 0 else None,
            "DATE_OF_BIRTH": f"{1980 + (i % 40)}-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
            "GENDER_ADMINISTRATIVE": genders[i % 3],
            "FHIR_GENDER_ADMINISTRATIVE": genders[i % 3],
            "GENDER_IDENTITY": genders[i % 3].capitalize(),
            "FHIR_GENDER_IDENTITY": genders[i % 3],
            "MARITAL_STATUS": "Married" if i % 2 == 0 else "Single",
            "FHIR_MARITAL_STATUS": "M" if i % 2 == 0 else "S",
            "RACE_DISPLAY": "White" if i % 3 == 0 else "Black or African American",
            "FHIR_RACE_DISPLAY": "2106-3" if i % 3 == 0 else "2054-5",
            "ETHNICITY_DISPLAY": "Not Hispanic or Latino",
            "FHIR_ETHNICITY_DISPLAY": "2186-5",
            "SEX_AT_BIRTH": "male" if i % 2 == 0 else "female",
            "IS_PREGNANT": "false",
        })

    n_duplicates = total - n_unique
    for j in range(n_duplicates):
        patients.append(patients[j % n_unique].copy())
        patients[-1]["SOURCE"] = base_source
        patients[-1]["is_original"] = False

    return patients
