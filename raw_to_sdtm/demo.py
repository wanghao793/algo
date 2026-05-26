"""
Demo: three mapping scenarios for map_variable().

Run:
  ANTHROPIC_API_KEY=sk-... python -m raw_to_sdtm.demo
"""
from __future__ import annotations

import sys

from raw_to_sdtm import map_variable
from raw_to_sdtm.state import MappingContext

# ── Demo data ─────────────────────────────────────────────────────────────────

_RAW_DM = [
    {"SUBJECT": "STUDY001-001", "SEX_CD": 1, "BRTHDT": "15JAN1985", "RFSTDT": "2023-06-01"},
    {"SUBJECT": "STUDY001-002", "SEX_CD": 2, "BRTHDT": "22MAR1972", "RFSTDT": "2023-07-15"},
    {"SUBJECT": "STUDY001-003", "SEX_CD": 1, "BRTHDT": "08JUL1990", "RFSTDT": "2023-05-20"},
    {"SUBJECT": "STUDY001-004", "SEX_CD": 3, "BRTHDT": "30NOV1968", "RFSTDT": "2023-08-10"},
    {"SUBJECT": "STUDY001-005", "SEX_CD": 2, "BRTHDT": "14APR2001", "RFSTDT": "2023-09-01"},
]

# ── Case 1: Direct mapping ────────────────────────────────────────────────────
CASE_DIRECT = MappingContext(
    raw_column="SUBJECT",
    raw_data=_RAW_DM,
    sdtm_variable="USUBJID",
    sdtm_domain="DM",
    sdtm_spec={
        "variable": "USUBJID",
        "type": "Char",
        "label": "Unique Subject Identifier",
        "required": True,
        "length": 200,
    },
)

# ── Case 2: Logical transformation — numeric sex code → SDTM codelist ────────
CASE_LOGICAL = MappingContext(
    raw_column="SEX_CD",
    raw_data=_RAW_DM,
    sdtm_variable="SEX",
    sdtm_domain="DM",
    sdtm_spec={
        "variable": "SEX",
        "type": "Char",
        "label": "Sex",
        "codelist": ["M", "F", "U", "UNDIFFERENTIATED"],
        "required": True,
        "length": 20,
    },
)

# ── Case 3: Complex derivation — AGE (years) from two date columns ────────────
CASE_COMPLEX = MappingContext(
    raw_column="BRTHDT",
    raw_data=_RAW_DM,
    sdtm_variable="AGE",
    sdtm_domain="DM",
    sdtm_spec={
        "variable": "AGE",
        "type": "Num",
        "label": "Age",
        "required": True,
        "description": (
            "Age in completed years at reference start date (RFSTDT). "
            "BRTHDT is in DD-MON-YYYY format; RFSTDT is ISO 8601 YYYY-MM-DD."
        ),
    },
)

# ── Runner ────────────────────────────────────────────────────────────────────

_DEMOS: dict[str, tuple[str, MappingContext]] = {
    "1": ("Direct mapping  — SUBJECT → DM.USUBJID", CASE_DIRECT),
    "2": ("Logical mapping — SEX_CD (1/2/3) → DM.SEX (M/F/U)", CASE_LOGICAL),
    "3": ("Complex derivation — BRTHDT + RFSTDT → DM.AGE (years)", CASE_COMPLEX),
}


def main() -> None:
    print("\nRAW → SDTM  |  Single Variable Mapping Agent")
    print("=" * 48)
    for k, (label, _) in _DEMOS.items():
        print(f"  {k}. {label}")
    print()

    choice = input("Select demo (1-3): ").strip()
    if choice not in _DEMOS:
        print("Invalid choice.")
        sys.exit(1)

    label, ctx = _DEMOS[choice]
    print(f"\nRunning: {label}\n")

    result = map_variable(ctx)

    if result["success"]:
        print(f"\nMapped values for {ctx.sdtm_domain}.{ctx.sdtm_variable}:")
        print(f"  {'RAW':>30s}    SDTM")
        print(f"  {'-'*30}    ----")
        for row, val in zip(ctx.raw_data, result["values"]):
            raw_val = row.get(ctx.raw_column, "")
            print(f"  {str(raw_val):>30s} →  {val!r}")
    else:
        print("\nMapping did not complete successfully.")
        print("Last issues:", result.get("last_issues"))


if __name__ == "__main__":
    main()
