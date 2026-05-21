#!/usr/bin/env python3
"""Entry point — interactive REPL for the multi-agent harness."""

import os
from dotenv import load_dotenv

load_dotenv()

if not os.getenv("ANTHROPIC_API_KEY"):
    raise EnvironmentError("Set ANTHROPIC_API_KEY in your environment or .env file.")

from agent import run_agent  # noqa: E402  (import after env check)


def main() -> None:
    print("=== Multi-Agent Harness Engineer (type 'exit' to quit) ===\n")
    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break
        if not user_input:
            continue
        if user_input.lower() in {"exit", "quit"}:
            print("Bye.")
            break
        try:
            response = run_agent(user_input)
            print(f"\nAgent: {response}\n")
        except Exception as exc:
            print(f"[error] {exc}\n")


if __name__ == "__main__":
    main()
