import json
from datetime import datetime
from langchain_core.tools import tool

_deployment_store: list[dict] = []
_next_id = 1


@tool
def create_deployment(service: str, version: str, environment: str) -> str:
    """Register and simulate a deployment for a service.

    Args:
        service: Name of the service to deploy.
        version: Version or image tag to deploy (e.g. "v1.2.3").
        environment: Target environment — one of: dev, staging, production.
    """
    global _next_id
    allowed_envs = {"dev", "staging", "production"}
    if environment not in allowed_envs:
        return f"Unknown environment '{environment}'. Use: {allowed_envs}"

    record = {
        "id": _next_id,
        "service": service,
        "version": version,
        "environment": environment,
        "status": "deployed",
        "deployed_at": datetime.utcnow().isoformat() + "Z",
    }
    _deployment_store.append(record)
    _next_id += 1
    return json.dumps(record, indent=2)


@tool
def list_deployments() -> str:
    """List all recorded deployments across all services and environments."""
    return json.dumps(_deployment_store, indent=2) if _deployment_store else "No deployments found."


@tool
def get_deployment_status(deployment_id: int) -> str:
    """Get detailed status for a specific deployment by its ID.

    Args:
        deployment_id: The numeric ID returned when the deployment was created.
    """
    for d in _deployment_store:
        if d["id"] == deployment_id:
            return json.dumps(d, indent=2)
    return f"Deployment {deployment_id} not found."


@tool
def rollback_deployment(deployment_id: int, reason: str) -> str:
    """Roll back a deployment by marking it as rolled back.

    Args:
        deployment_id: The numeric ID of the deployment to roll back.
        reason: Human-readable reason for the rollback.
    """
    for d in _deployment_store:
        if d["id"] == deployment_id:
            d["status"] = "rolled_back"
            d["rollback_reason"] = reason
            d["rolled_back_at"] = datetime.utcnow().isoformat() + "Z"
            return json.dumps(d, indent=2)
    return f"Deployment {deployment_id} not found."
