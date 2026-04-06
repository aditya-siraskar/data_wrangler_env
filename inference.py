import asyncio
import os
import json
import textwrap
from typing import List, Optional

from openai import OpenAI

from my_env import DataWranglerEnv, DataWranglerAction

# Environment Variables configured as per Hackathon Specs
API_KEY = os.getenv("HF_TOKEN") or os.getenv("API_KEY")
API_BASE_URL = os.getenv("API_BASE_URL") or "https://api.openai.com/v1"
MODEL_NAME = os.getenv("MODEL_NAME") or "gpt-4o-mini" # Or qwen/llama if using HF router
TASK_DIFFICULTY = os.getenv("MY_ENV_TASK", "easy") # easy, medium, or hard
BENCHMARK = "data_wrangler_env"

MAX_STEPS = 10
TEMPERATURE = 0.1
SUCCESS_SCORE_THRESHOLD = 1.0 

SYSTEM_PROMPT = textwrap.dedent(
    """
    You are an expert Data Engineer. You are interacting with a SQLite database environment.
    Your goal is to solve the given data cleaning task.
    
    You must respond with a JSON object. Do not include markdown formatting (like ```json).
    The JSON object must have exactly these keys:
    {
        "action_type": "execute_sql" OR "submit_task",
        "sql_query": "YOUR SQL HERE (only if action_type is execute_sql, otherwise null)",
        "thought": "Briefly explain what you are trying to do"
    }
    
    Hints:
    1. Always SELECT and view the data first to understand the problem.
    2. Then run your UPDATE/DELETE queries to fix the data.
    3. Finally, use action_type="submit_task" to finish the episode and get graded.
    """
).strip()

def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)

def log_step(step: int, action: str, reward: float, done: bool, error: Optional[str]) -> None:
    error_val = error if error else "null"
    done_val = str(done).lower()
    # Ensure action string doesn't have line breaks that ruin the single-line log format
    safe_action = action.replace("\n", " ") 
    print(f"[STEP] step={step} action={safe_action} reward={reward:.2f} done={done_val} error={error_val}", flush=True)

def log_end(success: bool, steps: int, score: float, rewards: List[float]) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(f"[END] success={str(success).lower()} steps={steps} score={score:.3f} rewards={rewards_str}", flush=True)

def build_user_prompt(step: int, obs_dict: dict, history: List[str]) -> str:
    history_block = "\n".join(history[-3:]) if history else "None"
    return textwrap.dedent(
        f"""
        Step: {step}
        Last Observation: {json.dumps(obs_dict, indent=2)}
        
        Previous actions:
        {history_block}
        
        Provide your next action in JSON format.
        """
    ).strip()

def get_model_action(client: OpenAI, step: int, obs_dict: dict, history: List[str]) -> DataWranglerAction:
    user_prompt = build_user_prompt(step, obs_dict, history)
    
    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=TEMPERATURE,
            response_format={ "type": "json_object" } # Force JSON output if supported
        )
        text = (completion.choices[0].message.content or "").strip()
        data = json.loads(text)
        
        return DataWranglerAction(
            action_type=data.get("action_type", "submit_task"),
            sql_query=data.get("sql_query")
        )
    except Exception as exc:
        print(f"[DEBUG] Model request failed or failed to parse JSON: {exc}", flush=True)
        # Fallback to submit to prevent infinite crash loops
        return DataWranglerAction(action_type="submit_task", sql_query=None)

async def main() -> None:
    # Requires API_KEY to be set in your terminal
    if not API_KEY:
         print("ERROR: API_KEY or HF_TOKEN environment variable is missing.")
         return

    client = OpenAI(base_url=API_BASE_URL, api_key=API_KEY)

    # Connect to the local server running in Terminal 1
    env = DataWranglerEnv(base_url="http://localhost:8000")

    history: List[str] = []
    rewards: List[float] = []
    steps_taken = 0
    score = 0.0
    success = False

    log_start(task=TASK_DIFFICULTY, env=BENCHMARK, model=MODEL_NAME)

    try:
        # We must pass kwargs to the reset function to define the task difficulty
        result = await env.reset(task=TASK_DIFFICULTY) 
        
        for step in range(1, MAX_STEPS + 1):
            if result.done:
                break

            # Convert observation to dict for the prompt
            obs_dict = {
                "feedback": result.observation.feedback_message,
                "schema": result.observation.schema_info,
                "query_results": result.observation.query_results,
                "rows_affected": result.observation.rows_affected
            }

            action = get_model_action(client, step, obs_dict, history)
            
            # Send action to environment
            result = await env.step(action)
            
            reward = result.reward or 0.0
            done = result.done
            error = None # We handle errors in the feedback_message

            rewards.append(reward)
            steps_taken = step
            
            # Create a string representation for logging
            action_str = f"{action.action_type}({action.sql_query})" if action.sql_query else action.action_type
            log_step(step=step, action=action_str, reward=reward, done=done, error=error)
            history.append(f"Action: {action_str} -> Feedback: {result.observation.feedback_message}")

            if done:
                # The final reward from a "submit_task" action is the grade (0.0 to 1.0)
                score = reward
                break

        success = score >= SUCCESS_SCORE_THRESHOLD

    finally:
        try:
            await env.close()
        except Exception as e:
            print(f"[DEBUG] env.close() error: {e}", flush=True)
        log_end(success=success, steps=steps_taken, score=score, rewards=rewards)

if __name__ == "__main__":
    asyncio.run(main())