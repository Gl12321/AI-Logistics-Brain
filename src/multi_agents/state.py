import operator
from typing import Annotated, TypedDict, Any, Dict

class AgentState(TypedDict):
    user_query: str
    conversation_summary: str
    agent_outcomes: Annotated[list[str], operator.add]
    plan: dict
    iteration_count: Annotated[int, operator.add]
    completed_steps: int
    next_actor: str
    status: str
    final_answer: str