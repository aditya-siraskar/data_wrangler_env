from typing import List, Optional, Any, Dict
from pydantic import Field
from openenv.core.env_server import Action, Observation, State

class DataWranglerAction(Action):
    """The agent can either execute a SQL command or submit the task."""
    action_type: str = Field(..., description="Either 'execute_sql' or 'submit_task'")
    sql_query: Optional[str] = Field(None, description="The SQL query to run if action_type is 'execute_sql'")

class DataWranglerObservation(Observation):
    """What the agent sees after taking an action."""
    # Note: 'done' and 'reward' are automatically inherited from Observation base class
    feedback_message: str = Field(..., description="Status of the last action (e.g., 'Query succeeded', 'Syntax error')")
    schema_info: Dict[str, str] = Field(default_factory=dict, description="Dictionary of table names and their CREATE TABLE schemas")
    query_results: Optional[List[Dict[str, Any]]] = Field(None, description="Rows returned from a SELECT query (max 50 rows)")
    rows_affected: int = Field(0, description="Number of rows modified by an UPDATE/INSERT/DELETE")

class DataWranglerState(State):
    """Underlying state of the episode."""
    # Note: 'episode_id' and 'step_count' are inherited
    task_difficulty: str = "easy"
    task_description: str = ""
    # We will use this to track partial progress for reward shaping
    milestones_achieved: Dict[str, bool] = Field(default_factory=dict)