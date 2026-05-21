from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage
from langgraph.prebuilt import create_react_agent

from agent.tools import (
    decompose_task,
    list_tasks,
    update_task_status,
    create_deployment,
    list_deployments,
    rollback_deployment,
    get_deployment_status,
)
from agent.state import AgentState

_SYSTEM = """You are a Harness Engineer — a senior software delivery specialist.

Your responsibilities:
1. TASK DECOMPOSITION: When given a feature or project goal, break it into clear,
   ordered subtasks and register them using decompose_task. Keep subtasks atomic
   and actionable.

2. DEPLOYMENT MANAGEMENT: Plan and execute deployments using the deployment tools.
   Always start with dev → staging → production unless the user specifies otherwise.
   Before deploying to production, summarise what was done in lower environments.
   Use rollback_deployment if anything looks wrong.

Think step-by-step. Use tools methodically and report clearly."""

_TOOLS = [
    decompose_task,
    list_tasks,
    update_task_status,
    create_deployment,
    list_deployments,
    rollback_deployment,
    get_deployment_status,
]

_llm = ChatAnthropic(model="claude-sonnet-4-6", temperature=0)

_agent = create_react_agent(
    model=_llm,
    tools=_TOOLS,
    prompt=SystemMessage(content=_SYSTEM),
)


def harness_engineer_node(state: AgentState) -> AgentState:
    result = _agent.invoke({"messages": state["messages"]})
    return {"messages": result["messages"]}
