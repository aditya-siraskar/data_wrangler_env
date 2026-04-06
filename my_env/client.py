from openenv.core.env_client import EnvClient
from openenv.core.client_types import StepResult
from .models import DataWranglerAction, DataWranglerObservation, DataWranglerState

class DataWranglerEnv(EnvClient[DataWranglerAction, DataWranglerObservation, DataWranglerState]):
    """Client for interacting with the Data Wrangler environment."""
    
    def _step_payload(self, action: DataWranglerAction) -> dict:
        """Translates the Action object into a JSON payload for the WebSocket."""
        return {
            "action_type": action.action_type,
            "sql_query": action.sql_query
        }

    def _parse_result(self, payload: dict) -> StepResult:
        """Translates the JSON payload from the server back into an Observation."""
        obs_data = payload.get("observation", {})
        return StepResult(
            observation=DataWranglerObservation(
                done=payload.get("done", False),
                reward=payload.get("reward", 0.0),
                feedback_message=obs_data.get("feedback_message", ""),
                schema_info=obs_data.get("schema_info", {}),
                query_results=obs_data.get("query_results"),
                rows_affected=obs_data.get("rows_affected", 0)
            ),
            reward=payload.get("reward", 0.0),
            done=payload.get("done", False),
        )

    def _parse_state(self, payload: dict) -> DataWranglerState:
        """Translates the JSON state payload back into a State object."""
        return DataWranglerState(
            episode_id=payload.get("episode_id"),
            step_count=payload.get("step_count", 0),
            task_difficulty=payload.get("task_difficulty", "easy"),
            task_description=payload.get("task_description", ""),
            milestones_achieved=payload.get("milestones_achieved", {})
        )