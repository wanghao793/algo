"""
SDTM / FDA TCG rule catalog.

Each rule is a plain function:
    rule_fn(values: pd.Series, spec: dict, ctx: dict) -> list[Finding]

ctx keys: domain (str), variable (str)

Rule-ID prefixes
  SDTM-xxx  SDTM Implementation Guide (CDISC SDTMIG 3.3+)
  TCG-xxx   FDA Study Data Technical Conformance Guide
  DM/AE/LB/VS/CM-xxx  Domain-specific (SDTMIG + TAUG)
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Literal

import numpy as np
import pandas as pd

Severity = Literal["CRITICAL", "MAJOR", "MINOR"]


@dataclass
class Finding:
    rule_id: str
    severity: Severity
    message: str
    affected_count: int = 0


RuleFn = Callable[[pd.Series, dict, dict], list[Finding]]

# ─────────────────────────────────────────────────────────────────────────────
# SDTM IG conformance rules
# ─────────────────────────────────────────────────────────────────────────────

def rule_sdtm_001_variable_name(values, spec, ctx) -> list[Finding]:
    """Variable name: ≤8 chars, uppercase, alphanumeric, starts with letter."""
    var = spec.get("variable", "")
    out: list[Finding] = []
    if len(var) > 8:
        out.append(Finding("SDTM-001", "CRITICAL",
            f"Variable name '{var}' exceeds 8 characters ({len(var)})"))
    if var and var != var.upper():
        out.append(Finding("SDTM-001", "CRITICAL",
            f"Variable name '{var}' must be uppercase"))
    if var and not re.match(r'^[A-Z][A-Z0-9]*$', var):
        out.append(Finding("SDTM-001", "CRITICAL",
            f"Variable name '{var}' contains invalid characters"))
    return out


def rule_sdtm_002_type(values, spec, ctx) -> list[Finding]:
    """Numeric type (Num) variables must contain only numeric values."""
    if spec.get("type", "Char").upper() != "NUM":
        return []
    bad = [v for v in values.dropna()
           if not isinstance(v, (int, float, np.integer, np.floating))]
    if bad:
        return [Finding("SDTM-002", "CRITICAL",
            f"Num variable has {len(bad)} non-numeric value(s): {bad[:3]}",
            len(bad))]
    return []


def rule_sdtm_003_required(values, spec, ctx) -> list[Finding]:
    """Required variables must have zero null values."""
    if not spec.get("required"):
        return []
    nulls = int(values.isna().sum())
    if nulls:
        return [Finding("SDTM-003", "CRITICAL",
            f"Required variable has {nulls} missing value(s)", nulls)]
    return []


def rule_sdtm_004_codelist(values, spec, ctx) -> list[Finding]:
    """Values must be members of the CDISC CT codelist when one is specified."""
    codelist = spec.get("codelist")
    if not codelist:
        return []
    allowed = {str(c) for c in codelist} | set(codelist)
    invalid = [v for v in values.dropna()
               if str(v) not in allowed and v not in allowed]
    if invalid:
        uniq = list({str(v) for v in invalid})[:5]
        return [Finding("SDTM-004", "CRITICAL",
            f"Codelist violation: {len(invalid)} value(s) not in CDISC CT: {uniq}",
            len(invalid))]
    return []


_ISO8601 = re.compile(
    r"^\d{4}(-\d{2}(-\d{2}"
    r"(T\d{2}:\d{2}(:\d{2}(\.\d+)?)?"
    r"(Z|[+-]\d{2}:\d{2})?)?)?)?$"
)


def rule_sdtm_005_dtc_format(values, spec, ctx) -> list[Finding]:
    """Date/time variables (*DTC) must follow ISO 8601."""
    if not spec.get("variable", "").upper().endswith("DTC"):
        return []
    bad = [v for v in values.dropna().astype(str) if not _ISO8601.match(v)]
    if bad:
        return [Finding("SDTM-005", "CRITICAL",
            f"ISO 8601 violation: {len(bad)} value(s), e.g. {bad[:2]}",
            len(bad))]
    return []


def rule_sdtm_006_length(values, spec, ctx) -> list[Finding]:
    """Char values must not exceed the defined max length."""
    max_len = spec.get("length")
    if not max_len or spec.get("type", "Char").upper() not in ("CHAR", "CHARACTER"):
        return []
    too_long = values.dropna().astype(str).str.len() > max_len
    n = int(too_long.sum())
    if n:
        return [Finding("SDTM-006", "MAJOR",
            f"Length violation: {n} value(s) exceed max {max_len} chars", n)]
    return []


def rule_sdtm_007_flag_values(values, spec, ctx) -> list[Finding]:
    """Flag variables (*FL, *IND) must contain only Y, N, or null."""
    var = spec.get("variable", "").upper()
    if not (var.endswith("FL") or var.endswith("IND")):
        return []
    invalid = [v for v in values.dropna() if str(v).upper() not in {"Y", "N"}]
    if invalid:
        uniq = list({str(v) for v in invalid})[:5]
        return [Finding("SDTM-007", "MAJOR",
            f"Flag variable {var} has non-Y/N value(s): {uniq}", len(invalid))]
    return []


def rule_sdtm_008_testcd_length(values, spec, ctx) -> list[Finding]:
    """*TESTCD values must be ≤8 chars (Findings domains, SDTMIG 4.1.4.1)."""
    if not spec.get("variable", "").upper().endswith("TESTCD"):
        return []
    too_long = [v for v in values.dropna().astype(str) if len(v) > 8]
    if too_long:
        return [Finding("SDTM-008", "CRITICAL",
            f"TESTCD values exceed 8 chars: {too_long[:3]}", len(too_long))]
    return []


def rule_sdtm_009_dy_integer(values, spec, ctx) -> list[Finding]:
    """Study day (*DY) must be a non-zero integer (SDTMIG section 4.4.7)."""
    if not spec.get("variable", "").upper().endswith("DY"):
        return []
    findings: list[Finding] = []
    non_int = [v for v in values.dropna()
               if not (isinstance(v, (int, np.integer)) or
                       (isinstance(v, float) and v == int(v)))]
    if non_int:
        findings.append(Finding("SDTM-009", "MAJOR",
            f"Study day variable has non-integer value(s): {non_int[:3]}",
            len(non_int)))
    zeros = [v for v in values.dropna() if v == 0]
    if zeros:
        findings.append(Finding("SDTM-009", "MAJOR",
            "Study day variable contains 0 "
            "(SDTM day numbering skips 0: …-1, 1, 2…)"))
    return findings


def rule_sdtm_010_epoch(values, spec, ctx) -> list[Finding]:
    """EPOCH values must come from the CDISC Epoch codelist."""
    if spec.get("variable", "").upper() != "EPOCH":
        return []
    EPOCH_CT = {
        "SCREENING", "RUN-IN", "TREATMENT", "FOLLOW-UP",
        "WASH-OUT", "OPEN LABEL EXTENSION", "DOUBLE BLIND TREATMENT",
    }
    invalid = [v for v in values.dropna()
               if str(v).upper() not in EPOCH_CT]
    if invalid:
        uniq = list({str(v) for v in invalid})[:5]
        return [Finding("SDTM-010", "MAJOR",
            f"EPOCH has {len(invalid)} value(s) outside CDISC CT: {uniq}",
            len(invalid))]
    return []


# ─────────────────────────────────────────────────────────────────────────────
# FDA Study Data Technical Conformance Guide (SDTCG) rules
# ─────────────────────────────────────────────────────────────────────────────

_MISSING_PLACEHOLDERS = {
    "MISSING", "N/A", "NA", "UNK", "UNKNOWN",
    ".", "NULL", "NONE", "NOT DONE", "ND", "NE",
}


def rule_tcg_001_missing_encoding(values, spec, ctx) -> list[Finding]:
    """Missing values must not be encoded as placeholder strings (TCG §4.1)."""
    if spec.get("type", "Char").upper() == "NUM":
        return []
    bad = [v for v in values.dropna().astype(str)
           if v.strip().upper() in _MISSING_PLACEHOLDERS]
    if bad:
        return [Finding("TCG-001", "CRITICAL",
            f"Missing-value placeholder(s) found — use null/empty string: "
            f"{list({v.strip() for v in bad})[:5]}",
            len(bad))]
    return []


def rule_tcg_002_label_length(values, spec, ctx) -> list[Finding]:
    """Variable label must be ≤40 chars (TCG §4.1.2.1)."""
    label = spec.get("label", "")
    if len(label) > 40:
        return [Finding("TCG-002", "MAJOR",
            f"Variable label ({len(label)} chars) exceeds 40-char limit: "
            f"'{label[:35]}…'")]
    return []


def rule_tcg_003_whitespace(values, spec, ctx) -> list[Finding]:
    """Char values must not have leading or trailing whitespace (TCG §4.1.3)."""
    if spec.get("type", "Char").upper() not in ("CHAR", "CHARACTER"):
        return []
    padded = [v for v in values.dropna().astype(str) if v != v.strip()]
    if padded:
        return [Finding("TCG-003", "MINOR",
            f"{len(padded)} value(s) have leading/trailing whitespace",
            len(padded))]
    return []


def rule_tcg_004_visitnum(values, spec, ctx) -> list[Finding]:
    """VISITNUM must be numeric (TCG §4.1.4.10)."""
    if spec.get("variable", "").upper() != "VISITNUM":
        return []
    bad = [v for v in values.dropna()
           if not isinstance(v, (int, float, np.integer, np.floating))]
    if bad:
        return [Finding("TCG-004", "CRITICAL",
            f"VISITNUM has {len(bad)} non-numeric value(s): {bad[:3]}",
            len(bad))]
    return []


def rule_tcg_005_age(values, spec, ctx) -> list[Finding]:
    """AGE must be a plausible non-negative value ≤120 (TCG §4.2 DM guidance)."""
    if "AGE" not in spec.get("variable", "").upper():
        return []
    findings: list[Finding] = []
    neg = [v for v in values.dropna()
           if isinstance(v, (int, float, np.integer, np.floating)) and v < 0]
    implaus = [v for v in values.dropna()
               if isinstance(v, (int, float, np.integer, np.floating)) and v > 120]
    if neg:
        findings.append(Finding("TCG-005", "CRITICAL",
            f"{len(neg)} negative AGE value(s)", len(neg)))
    if implaus:
        findings.append(Finding("TCG-005", "MAJOR",
            f"{len(implaus)} implausible AGE value(s) >120: {implaus[:3]}",
            len(implaus)))
    return findings


def rule_tcg_006_sex(values, spec, ctx) -> list[Finding]:
    """SEX must follow CDISC/FDA NCI C66731 codelist (TCG §4.2 DM)."""
    if spec.get("variable", "").upper() != "SEX":
        return []
    FDA_SEX = {"M", "F", "U", "UNDIFFERENTIATED"}
    invalid = [v for v in values.dropna() if str(v).upper() not in FDA_SEX]
    if invalid:
        uniq = list({str(v) for v in invalid})[:5]
        return [Finding("TCG-006", "CRITICAL",
            f"SEX value(s) outside NCI C66731 {FDA_SEX}: {uniq}",
            len(invalid))]
    return []


def rule_tcg_007_race(values, spec, ctx) -> list[Finding]:
    """RACE must follow NIH/FDA C74457 codelist (TCG §4.2 DM)."""
    if spec.get("variable", "").upper() != "RACE":
        return []
    RACE_CT = {
        "AMERICAN INDIAN OR ALASKA NATIVE", "ASIAN",
        "BLACK OR AFRICAN AMERICAN",
        "NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER",
        "WHITE", "MULTIPLE", "NOT REPORTED", "UNKNOWN",
    }
    invalid = [v for v in values.dropna() if str(v).upper() not in RACE_CT]
    if invalid:
        uniq = list({str(v) for v in invalid})[:5]
        return [Finding("TCG-007", "MAJOR",
            f"RACE has {len(invalid)} value(s) outside NIH C74457: {uniq}",
            len(invalid))]
    return []


def rule_tcg_008_ethnic(values, spec, ctx) -> list[Finding]:
    """ETHNIC must follow NCI C66790 codelist (TCG §4.2 DM)."""
    if spec.get("variable", "").upper() != "ETHNIC":
        return []
    ETHNIC_CT = {
        "HISPANIC OR LATINO", "NOT HISPANIC OR LATINO",
        "NOT REPORTED", "UNKNOWN",
    }
    invalid = [v for v in values.dropna() if str(v).upper() not in ETHNIC_CT]
    if invalid:
        uniq = list({str(v) for v in invalid})[:5]
        return [Finding("TCG-008", "MAJOR",
            f"ETHNIC has {len(invalid)} value(s) outside NCI C66790: {uniq}",
            len(invalid))]
    return []


def rule_tcg_009_usubjid_format(values, spec, ctx) -> list[Finding]:
    """USUBJID should follow STUDYID-SITEID-SUBJID pattern (TCG §4.2 DM)."""
    if spec.get("variable", "").upper() != "USUBJID":
        return []
    # Must contain at least one hyphen and be non-empty
    bad = [v for v in values.dropna().astype(str) if "-" not in v]
    if bad:
        return [Finding("TCG-009", "MINOR",
            f"USUBJID value(s) may not follow recommended STUDYID-SITEID-SUBJID "
            f"pattern (no hyphen): {bad[:3]}",
            len(bad))]
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Domain-specific rules (SDTMIG + TAUG)
# ─────────────────────────────────────────────────────────────────────────────

def rule_ae_001_aeser(values, spec, ctx) -> list[Finding]:
    """AE: AESER must be Y or N for every record (SDTMIG AE 6.2)."""
    if ctx.get("domain", "").upper() != "AE" or \
       spec.get("variable", "").upper() != "AESER":
        return []
    findings: list[Finding] = []
    nulls = int(values.isna().sum())
    if nulls:
        findings.append(Finding("AE-001", "CRITICAL",
            f"AESER has {nulls} null value(s) — must be Y or N for all records",
            nulls))
    invalid = [v for v in values.dropna()
               if str(v).upper() not in {"Y", "N"}]
    if invalid:
        findings.append(Finding("AE-001", "CRITICAL",
            f"AESER has non-Y/N value(s): "
            f"{list({str(v) for v in invalid})[:5]}",
            len(invalid)))
    return findings


def rule_ae_002_aesev(values, spec, ctx) -> list[Finding]:
    """AE: AESEV must follow CDISC severity codelist (SDTMIG AE 6.2)."""
    if ctx.get("domain", "").upper() != "AE" or \
       spec.get("variable", "").upper() != "AESEV":
        return []
    SEV_CT = {"MILD", "MODERATE", "SEVERE"}
    invalid = [v for v in values.dropna()
               if str(v).upper() not in SEV_CT]
    if invalid:
        uniq = list({str(v) for v in invalid})[:5]
        return [Finding("AE-002", "MAJOR",
            f"AESEV has value(s) outside {SEV_CT}: {uniq}", len(invalid))]
    return []


def rule_lb_001_lbtestcd(values, spec, ctx) -> list[Finding]:
    """LB: LBTESTCD must be ≤8 chars (SDTMIG LB 6.3.1)."""
    if ctx.get("domain", "").upper() != "LB" or \
       spec.get("variable", "").upper() != "LBTESTCD":
        return []
    too_long = [v for v in values.dropna().astype(str) if len(v) > 8]
    if too_long:
        return [Finding("LB-001", "CRITICAL",
            f"LBTESTCD exceeds 8 chars: {too_long[:3]}", len(too_long))]
    return []


def rule_lb_002_lbtest(values, spec, ctx) -> list[Finding]:
    """LB: LBTEST must be ≤40 chars (SDTMIG LB 6.3.1)."""
    if ctx.get("domain", "").upper() != "LB" or \
       spec.get("variable", "").upper() != "LBTEST":
        return []
    too_long = [v for v in values.dropna().astype(str) if len(v) > 40]
    if too_long:
        return [Finding("LB-002", "MAJOR",
            f"LBTEST exceeds 40 chars: {too_long[:3]}", len(too_long))]
    return []


def rule_vs_001_vstestcd(values, spec, ctx) -> list[Finding]:
    """VS: VSTESTCD must be ≤8 chars (SDTMIG VS 6.3.5)."""
    if ctx.get("domain", "").upper() != "VS" or \
       spec.get("variable", "").upper() != "VSTESTCD":
        return []
    too_long = [v for v in values.dropna().astype(str) if len(v) > 8]
    if too_long:
        return [Finding("VS-001", "CRITICAL",
            f"VSTESTCD exceeds 8 chars: {too_long[:3]}", len(too_long))]
    return []


def rule_cm_001_cmtrt(values, spec, ctx) -> list[Finding]:
    """CM: CMTRT must not be null (SDTMIG CM 6.1.1 — topic variable)."""
    if ctx.get("domain", "").upper() != "CM" or \
       spec.get("variable", "").upper() != "CMTRT":
        return []
    nulls = int(values.isna().sum())
    if nulls:
        return [Finding("CM-001", "CRITICAL",
            f"CMTRT (topic variable) has {nulls} null value(s)", nulls)]
    return []


def rule_ex_001_extrt(values, spec, ctx) -> list[Finding]:
    """EX: EXTRT must not be null (SDTMIG EX 6.1.2 — topic variable)."""
    if ctx.get("domain", "").upper() != "EX" or \
       spec.get("variable", "").upper() != "EXTRT":
        return []
    nulls = int(values.isna().sum())
    if nulls:
        return [Finding("EX-001", "CRITICAL",
            f"EXTRT (topic variable) has {nulls} null value(s)", nulls)]
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Catalogs — add custom rules here
# ─────────────────────────────────────────────────────────────────────────────

SDTM_RULES: list[RuleFn] = [
    rule_sdtm_001_variable_name,
    rule_sdtm_002_type,
    rule_sdtm_003_required,
    rule_sdtm_004_codelist,
    rule_sdtm_005_dtc_format,
    rule_sdtm_006_length,
    rule_sdtm_007_flag_values,
    rule_sdtm_008_testcd_length,
    rule_sdtm_009_dy_integer,
    rule_sdtm_010_epoch,
]

FDA_TCG_RULES: list[RuleFn] = [
    rule_tcg_001_missing_encoding,
    rule_tcg_002_label_length,
    rule_tcg_003_whitespace,
    rule_tcg_004_visitnum,
    rule_tcg_005_age,
    rule_tcg_006_sex,
    rule_tcg_007_race,
    rule_tcg_008_ethnic,
    rule_tcg_009_usubjid_format,
]

DOMAIN_RULES: list[RuleFn] = [
    rule_ae_001_aeser,
    rule_ae_002_aesev,
    rule_lb_001_lbtestcd,
    rule_lb_002_lbtest,
    rule_vs_001_vstestcd,
    rule_cm_001_cmtrt,
    rule_ex_001_extrt,
]

ALL_RULES: list[RuleFn] = SDTM_RULES + FDA_TCG_RULES + DOMAIN_RULES
