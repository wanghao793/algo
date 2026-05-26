"""
ReAct agent for a single RAW→SDTM variable mapping.

Each call to run_agent_session() is one session: the agent classifies,
generates code, verifies, and retries within the session.  The outer
map_variable() loop (in __init__.py) handles cross-session failure
counting and human intervention at the 3-failure threshold.
"""
from __future__ import annotations

import json

import anthropic

from raw_to_sdtm.state import MappingContext
from raw_to_sdtm.tools import run_transform, verify_sdtm

# ── Tool schemas ─────────────────────────────────────────────────────────────

_TOOL_DEFS: list[dict] = [
    {
        "name": "classify_mapping",
        "description": (
            "Classify the mapping type before writing any code. Call this first."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "mapping_type": {
                    "type": "string",
                    "enum": ["direct", "logical", "complex"],
                    "description": (
                        "direct  = rename / cast / 1-to-1 lookup, no conditional logic; "
                        "logical = conditional logic, formula, unit conversion, date reformatting; "
                        "complex = multi-source derivation or algorithm "
                        "(e.g. VISITNUM from free text, AGE from two date columns)"
                    ),
                },
                "rationale": {
                    "type": "string",
                    "description": "One-sentence justification for this classification.",
                },
            },
            "required": ["mapping_type", "rationale"],
        },
    },
    {
        "name": "execute_transform",
        "description": (
            "Run Python code to transform the raw column into SDTM values.\n"
            "Available in scope: df (pandas DataFrame of all raw columns), pd, np, re.\n"
            "Your code MUST assign the final output to a variable named 'result' "
            "(a list or Series of the same length as df).\n"
            "Do NOT add import statements — libraries are already loaded."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "Self-contained Python code that assigns to 'result'.",
                },
                "description": {
                    "type": "string",
                    "description": "One-line summary of what this transformation does.",
                },
            },
            "required": ["code", "description"],
        },
    },
    {
        "name": "verify_result",
        "description": (
            "Verify transformed values against the SDTM specification. "
            "Call this after every execute_transform. "
            "Pass the 'values' list from the execute_transform result."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "values": {
                    "type": "array",
                    "description": "The 'values' list returned by execute_transform.",
                },
            },
            "required": ["values"],
        },
    },
]

# ── System prompt ─────────────────────────────────────────────────────────────

_SYSTEM = """\
You are an expert clinical data programmer specialising in RAW→SDTM transformations.
Your task: transform a single raw column into its SDTM target variable.

STRICT THREE-STEP WORKFLOW — follow on every attempt:

STEP 1 — classify_mapping
  Decide which of the three cases applies:
  • direct  : simple rename, cast, or 1-to-1 lookup — no conditional logic
  • logical : conditional logic, formula, unit conversion, or date reformatting
  • complex : multi-source derivation or algorithm
              (e.g. VISITNUM from visit text, AGE computed from two date fields)

STEP 2 — execute_transform
  Write Python code.  Rules:
  - Access all raw columns via `df` (a pandas DataFrame).
  - Assign your final output to `result` (list or Series, same length as df).
  - Handle nulls/edge cases explicitly.
  - Do NOT add imports; pd, np, re are already in scope.

STEP 3 — verify_result
  Pass the `values` from execute_transform.
  - If PASS  → state the final mapping clearly and stop.
  - If FAIL  → read the issues, adjust your approach, and retry from STEP 2.
  - If human rules are present → follow them precisely; they override your judgment.

Principles:
- Try the simplest correct approach first.
- When a verify fails, explain what went wrong and what you will do differently.
- If you genuinely cannot produce a passing mapping, say so explicitly — do not loop.
"""


# ── Prompt builder ────────────────────────────────────────────────────────────

def _build_prompt(ctx: MappingContext) -> str:
    parts = [
        f"TASK: map  {ctx.raw_column}  →  {ctx.sdtm_domain}.{ctx.sdtm_variable}",
        "",
        "RAW COLUMN INFO:",
        json.dumps(ctx.raw_col_info, indent=2, default=str),
        "",
        "SDTM TARGET SPEC:",
        json.dumps(ctx.sdtm_spec, indent=2),
    ]

    if ctx.human_rules:
        parts += ["", "HUMAN MAPPING RULES (follow exactly — these override your reasoning):"]
        for i, rule in enumerate(ctx.human_rules, 1):
            parts.append(f"  Rule {i}: {rule}")

    if ctx.attempt_history:
        parts += [
            "",
            f"PREVIOUS FAILED ATTEMPTS ({ctx.verify_failures} total verify failures so far):",
        ]
        for i, att in enumerate(ctx.attempt_history, 1):
            issues_str = "; ".join(att.get("issues", [])) or "unknown"
            parts.append(
                f"  [{i}] mapping_type={att['mapping_type']}  →  issues: {issues_str}"
            )
        parts.append("  → Use a different approach this time.")

    return "\n".join(parts)


# ── Agent session ─────────────────────────────────────────────────────────────

MAX_TOOL_CALLS = 14  # safety ceiling for one session


def run_agent_session(ctx: MappingContext) -> dict:
    """
    Run one ReAct session for ctx.
    Returns:
      {"success": True,  "values": [...], "summary": "..."}
      {"success": False, "last_issues": [...], "summary": "..."}
    """
    client = anthropic.Anthropic()
    messages: list[dict] = [{"role": "user", "content": _build_prompt(ctx)}]
    current_values: list | None = None
    last_issues: list[str] = []

    for iteration in range(MAX_TOOL_CALLS):
        response = client.messages.create(
            model="claude-opus-4-7",
            max_tokens=4096,
            system=_SYSTEM,
            tools=_TOOL_DEFS,  # type: ignore[arg-type]
            messages=messages,  # type: ignore[arg-type]
        )
        messages.append({"role": "assistant", "content": response.content})

        # ── Agent finished talking ──────────────────────────────────────────
        if response.stop_reason == "end_turn":
            last_text = next(
                (b.text for b in response.content if hasattr(b, "text")), ""
            )
            succeeded = current_values is not None and not last_issues
            return {
                "success": succeeded,
                "values": current_values,
                "last_issues": last_issues,
                "summary": last_text,
            }

        if response.stop_reason != "tool_use":
            return {
                "success": False,
                "last_issues": [f"Unexpected stop_reason: {response.stop_reason}"],
                "summary": "",
            }

        # ── Process tool calls ──────────────────────────────────────────────
        tool_results: list[dict] = []

        for block in response.content:
            if block.type != "tool_use":
                continue

            name = block.name
            inp = block.input

            if name == "classify_mapping":
                ctx.mapping_type = inp["mapping_type"]
                result_payload: dict = {
                    "ok": True,
                    "mapping_type": ctx.mapping_type,
                    "note": f"Classified as '{ctx.mapping_type}'. Proceed to execute_transform.",
                }

            elif name == "execute_transform":
                result_payload = run_transform(inp["code"], ctx.raw_data)
                if result_payload["success"]:
                    current_values = result_payload["values"]
                    last_issues = []  # reset until next verify

            elif name == "verify_result":
                result_payload = verify_sdtm(inp["values"], ctx.sdtm_spec, ctx.sdtm_domain)
                if not result_payload["pass"]:
                    last_issues = result_payload["issues"]
                    ctx.verify_failures += 1
                    ctx.attempt_history.append(
                        {
                            "mapping_type": ctx.mapping_type,
                            "issues": result_payload["issues"],
                        }
                    )
                    result_payload["verify_failure_count"] = ctx.verify_failures
                else:
                    last_issues = []

            else:
                result_payload = {"error": f"Unknown tool: {name}"}

            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result_payload, default=str),
                }
            )

        messages.append({"role": "user", "content": tool_results})

    return {
        "success": False,
        "last_issues": last_issues or ["Max tool-call iterations reached"],
        "summary": "Agent hit the iteration ceiling without a passing result.",
    }
