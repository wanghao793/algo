import json
from langchain_core.tools import tool

_task_store: list[dict] = []


@tool
def decompose_task(goal: str, subtasks: list[str]) -> str:
    """Break a high-level goal into ordered subtasks and register them.

    Args:
        goal: The high-level objective to accomplish.
        subtasks: Ordered list of subtask descriptions.
    """
    entries = [
        {"id": i + 1, "goal": goal, "subtask": s, "status": "pending"}
        for i, s in enumerate(subtasks)
    ]
    _task_store.extend(entries)
    return json.dumps({"registered": len(entries), "tasks": entries}, indent=2)


@tool
def list_tasks() -> str:
    """Return all registered tasks and their current status."""
    return json.dumps(_task_store, indent=2) if _task_store else "No tasks registered."


@tool
def update_task_status(task_id: int, status: str) -> str:
    """Update the status of a task by its ID.

    Args:
        task_id: The numeric ID of the task.
        status: New status — one of: pending, in_progress, done, failed.
    """
    valid = {"pending", "in_progress", "done", "failed"}
    if status not in valid:
        return f"Invalid status '{status}'. Choose from: {valid}"
    for task in _task_store:
        if task["id"] == task_id:
            task["status"] = status
            return f"Task {task_id} updated to '{status}'."
    return f"Task {task_id} not found."
