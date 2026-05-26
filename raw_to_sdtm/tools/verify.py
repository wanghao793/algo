from __future__ import annotations

import re

import numpy as np
import pandas as pd

# Matches ISO 8601 partial dates: YYYY, YYYY-MM, YYYY-MM-DD, with optional time/tz
_ISO8601 = re.compile(
    r"^\d{4}(-\d{2}(-\d{2}"
    r"(T\d{2}:\d{2}(:\d{2}(\.\d+)?)?"
    r"(Z|[+-]\d{2}:\d{2})?)?)?)?$"
)


def verify_sdtm(values: list, sdtm_spec: dict) -> dict:
    """
    Check transformed values against the SDTM specification.

    Checks performed (all applicable given the spec):
      1. Numeric type conformance
      2. Required-field completeness
      3. Codelist membership
      4. Character max-length
      5. ISO 8601 format for *DTC variables
      6. Non-negative range for AGE-like Num variables
    """
    s = pd.Series(values)
    issues: list[str] = []
    warnings: list[str] = []

    var_name = sdtm_spec.get("variable", "")
    var_type = sdtm_spec.get("type", "Char").upper()
    codelist = sdtm_spec.get("codelist")
    required = sdtm_spec.get("required", False)
    max_len = sdtm_spec.get("length")

    # 1 ── Numeric type
    if var_type == "NUM":
        bad = [
            v for v in s.dropna()
            if not isinstance(v, (int, float, np.integer, np.floating))
        ]
        if bad:
            issues.append(f"TYPE: expected Num, got non-numeric: {bad[:3]}")

    # 2 ── Required completeness
    null_rate = float(s.isna().mean())
    if required and null_rate > 0:
        issues.append(
            f"REQUIRED: {null_rate:.1%} nulls ({int(s.isna().sum())} rows)"
        )
    elif null_rate > 0.20:
        warnings.append(f"COMPLETENESS: {null_rate:.1%} null rate")

    # 3 ── Codelist membership
    if codelist:
        allowed = {str(c) for c in codelist} | set(codelist)
        invalid = [v for v in s.dropna() if str(v) not in allowed and v not in allowed]
        if invalid:
            uniq = list({str(v) for v in invalid})[:5]
            issues.append(
                f"CODELIST: {len(invalid)} values not in allowed set: {uniq}"
            )

    # 4 ── Character length
    if max_len and var_type in ("CHAR", "CHARACTER"):
        too_long = s.dropna().astype(str).str.len() > max_len
        if too_long.any():
            issues.append(
                f"LENGTH: {int(too_long.sum())} values exceed max {max_len} chars"
            )

    # 5 ── ISO 8601 for *DTC variables
    if var_name.upper().endswith("DTC"):
        bad_dates = [
            v for v in s.dropna().astype(str)
            if not _ISO8601.match(str(v))
        ]
        if bad_dates:
            issues.append(
                f"ISO8601: {len(bad_dates)} values not in ISO 8601 format: {bad_dates[:3]}"
            )

    # 6 ── Non-negative age
    if "AGE" in var_name.upper() and var_type == "NUM":
        neg = [
            v for v in s.dropna()
            if isinstance(v, (int, float, np.integer, np.floating)) and v < 0
        ]
        if neg:
            issues.append(f"RANGE: {len(neg)} negative age values")

    return {
        "pass": len(issues) == 0,
        "issues": issues,
        "warnings": warnings,
        "stats": {
            "n_rows": len(s),
            "n_nulls": int(s.isna().sum()),
            "null_pct": f"{null_rate:.1%}",
            "sample": s.dropna().head(5).tolist(),
        },
    }
