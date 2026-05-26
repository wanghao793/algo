from __future__ import annotations


def ask_human(ctx_info: dict) -> str:
    """
    Block the process, print full failure context, and wait for a human-provided
    natural-language mapping rule.  Returns the rule string.
    """
    sep = "=" * 64
    print(f"\n{sep}")
    print("  ⚠️  HUMAN INTERVENTION REQUIRED")
    print(sep)
    print(f"  Mapping  : {ctx_info['raw_column']} → "
          f"{ctx_info['sdtm_domain']}.{ctx_info['sdtm_variable']}")
    print(f"  Failures : {ctx_info['verify_failures']} verify failures")

    if ctx_info.get("last_issues"):
        print("\n  Last verification issues:")
        for issue in ctx_info["last_issues"]:
            print(f"    ✗ {issue}")

    if ctx_info.get("attempts"):
        print("\n  Approaches already tried:")
        for i, att in enumerate(ctx_info["attempts"], 1):
            issues_str = "; ".join(att.get("issues", [])) or "no detail"
            print(f"    [{i}] type={att.get('mapping_type', '?')} — {issues_str}")

    print(f"\n  Raw sample : {ctx_info.get('raw_sample', [])[:8]}")
    print(f"  SDTM spec  : {ctx_info.get('sdtm_spec', {})}")

    print("\n  Enter the correct mapping rule in plain English. Examples:")
    print("    '1 → MALE, 2 → FEMALE, 3 → UNKNOWN'")
    print("    'Age in years = floor((RFSTDTC – BRTHDTC) / 365.25)'")
    print("    'Reformat DD-MON-YYYY to ISO 8601 YYYY-MM-DD'")
    print("    'Concatenate STUDYID and SUBJID with a hyphen'")
    print("-" * 64)

    while True:
        rule = input("  Your rule: ").strip()
        if rule:
            break
        print("  (please enter a non-empty rule)")

    print(f"\n  Rule accepted — restarting agent with your rule...\n")
    return rule
