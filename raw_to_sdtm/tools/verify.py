from __future__ import annotations

import pandas as pd

from raw_to_sdtm.tools.rules import ALL_RULES, Finding


def verify_sdtm(values: list, sdtm_spec: dict, domain: str = "") -> dict:
    """
    Run the full SDTM / FDA-TCG rule catalog against transformed values.

    Parameters
    ----------
    values    : transformed column as a plain list
    sdtm_spec : spec dict (variable, type, label, codelist, required, length…)
    domain    : SDTM domain name (DM, AE, LB, VS…) — enables domain rules

    Returns
    -------
    {
      "pass"    : bool   — True only when zero CRITICAL findings
      "issues"  : list   — CRITICAL findings   → agent must fix
      "warnings": list   — MAJOR findings      → agent should note
      "info"    : list   — MINOR findings      → informational
      "findings": {"critical": n, "major": n, "minor": n}
      "stats"   : {"n_rows", "n_nulls", "null_pct", "sample"}
    }
    """
    s = pd.Series(values)
    ctx = {
        "domain": domain.upper(),
        "variable": sdtm_spec.get("variable", "").upper(),
    }

    all_findings: list[Finding] = []
    for rule_fn in ALL_RULES:
        try:
            all_findings.extend(rule_fn(s, sdtm_spec, ctx))
        except Exception as exc:
            all_findings.append(
                Finding("INTERNAL", "MINOR",
                        f"Rule {rule_fn.__name__} raised: {exc}")
            )

    critical = [f for f in all_findings if f.severity == "CRITICAL"]
    major    = [f for f in all_findings if f.severity == "MAJOR"]
    minor    = [f for f in all_findings if f.severity == "MINOR"]

    null_rate = float(s.isna().mean())

    return {
        "pass": len(critical) == 0,
        "issues":   [f"{f.rule_id} [CRITICAL]: {f.message}" for f in critical],
        "warnings": [f"{f.rule_id} [MAJOR]: {f.message}"    for f in major],
        "info":     [f"{f.rule_id} [MINOR]: {f.message}"    for f in minor],
        "findings": {
            "critical": len(critical),
            "major":    len(major),
            "minor":    len(minor),
        },
        "stats": {
            "n_rows":   len(s),
            "n_nulls":  int(s.isna().sum()),
            "null_pct": f"{null_rate:.1%}",
            "sample":   s.dropna().head(5).tolist(),
        },
    }
