"""
raw_to_sdtm
===========
Single-variable RAW→SDTM mapping agent.

Entry point: map_variable(ctx)
  - Runs a Claude ReAct agent session.
  - After MAX_VERIFY_FAILURES consecutive failures, blocks for human input,
    injects the natural-language rule, and restarts the agent.
"""
from __future__ import annotations

from raw_to_sdtm.agent import run_agent_session
from raw_to_sdtm.human import ask_human
from raw_to_sdtm.state import MappingContext

MAX_VERIFY_FAILURES = 3


def map_variable(ctx: MappingContext) -> dict:
    """
    Outer orchestration loop.

    Flow
    ────
    1. Run one agent session (classify → transform → verify, with internal retries).
    2. If verify passes  → return result.
    3. If verify_failures ≥ MAX_VERIFY_FAILURES:
         a. Block and request a human rule via stdin.
         b. Inject the rule into ctx.human_rules.
         c. Reset the failure counter and attempt history.
         d. Restart from step 1.
    4. If session exhausted without hitting the threshold → restart session
       (agent will see the accumulated failure history).
    """
    while True:
        result = run_agent_session(ctx)

        if result["success"]:
            print(
                f"\n✅ Mapping complete: "
                f"{ctx.raw_column} → {ctx.sdtm_domain}.{ctx.sdtm_variable}"
            )
            if result.get("summary"):
                print(f"   {result['summary']}")
            return result

        # ── Check whether human intervention is needed ──────────────────────
        if ctx.verify_failures >= MAX_VERIFY_FAILURES:
            rule = ask_human(
                {
                    "raw_column": ctx.raw_column,
                    "sdtm_variable": ctx.sdtm_variable,
                    "sdtm_domain": ctx.sdtm_domain,
                    "verify_failures": ctx.verify_failures,
                    "last_issues": result.get("last_issues", []),
                    "attempts": ctx.attempt_history,
                    "raw_sample": ctx.raw_col_info["sample"],
                    "sdtm_spec": ctx.sdtm_spec,
                }
            )
            ctx.human_rules.append(rule)
            ctx.verify_failures = 0
            ctx.attempt_history = []
        # Loop back — agent will pick up the updated ctx (new rules / history).
