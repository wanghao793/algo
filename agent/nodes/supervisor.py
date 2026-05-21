from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate

from agent.state import AgentState

_SYSTEM = """You are a Supervisor orchestrating a team of specialist agents.
Available agents:
  - harness_engineer: handles task decomposition and deployment management
  - FINISH: use when the user's request is fully resolved

Given the conversation so far, decide which agent should act next.
Reply with ONLY the agent name or FINISH — nothing else."""

_llm = ChatAnthropic(model="claude-sonnet-4-6", temperature=0)


def supervisor_node(state: AgentState) -> AgentState:
    prompt = ChatPromptTemplate.from_messages(
        [
            SystemMessage(content=_SYSTEM),
            ("placeholder", "{messages}"),
        ]
    )
    chain = prompt | _llm
    result = chain.invoke({"messages": state["messages"]})
    next_agent = result.content.strip()
    return {"next": next_agent}


def router(state: AgentState) -> str:
    return state["next"]
