from langgraph.graph import END, START, StateGraph

from agent.nodes import harness_engineer_node, router, supervisor_node
from agent.state import AgentState


def build_graph() -> StateGraph:
    graph = StateGraph(AgentState)

    # Nodes
    graph.add_node("supervisor", supervisor_node)
    graph.add_node("harness_engineer", harness_engineer_node)

    # Entry point
    graph.add_edge(START, "supervisor")

    # Supervisor routes to an agent or finishes
    graph.add_conditional_edges(
        "supervisor",
        router,
        {
            "harness_engineer": "harness_engineer",
            "FINISH": END,
        },
    )

    # After each agent, return to supervisor for re-evaluation
    graph.add_edge("harness_engineer", "supervisor")

    return graph.compile()


def run_agent(user_input: str) -> str:
    """Run the multi-agent graph and return the last AI message."""
    from langchain_core.messages import HumanMessage

    app = build_graph()
    final_state = app.invoke(
        {"messages": [HumanMessage(content=user_input)]},
        config={"recursion_limit": 20},
    )
    return final_state["messages"][-1].content
