import sqlite3
import uuid
from typing import Dict, Any, Tuple
from openenv.core.env_server import Environment
from ..models import DataWranglerAction, DataWranglerObservation, DataWranglerState

class DataWranglerEnvironment(Environment):
    # Support multiple concurrent connections (crucial for HF spaces)
    SUPPORTS_CONCURRENT_SESSIONS = True  

    def __init__(self):
        self._state = DataWranglerState()
        self.conn = None
        
    def _setup_database(self, difficulty: str):
        """Creates a fresh in-memory database with the specific task data."""
        if self.conn:
            self.conn.close()
        # Fresh in-memory DB for every episode
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()

        if difficulty == "easy":
            self._state.task_description = (
                "Task: Standardize the 'is_active' column in the 'users' table. "
                "Currently, it contains messy string values like 'Yes', 'Y', 'No', 'N'. "
                "Update the table so that 'is_active' is exactly '1' for true/yes values, and '0' for false/no values."
            )
            cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, is_active TEXT)")
            messy_data = [
                (1, 'Alice', 'Yes'), (2, 'Bob', 'N'), (3, 'Charlie', 'Y'), 
                (4, 'Diana', 'No'), (5, 'Eve', 'Yes')
            ]
            cursor.executemany("INSERT INTO users VALUES (?, ?, ?)", messy_data)

        elif difficulty == "medium":
            self._state.task_description = (
                "Task: Impute missing data in the 'orders' table. "
                "Some rows have a NULL 'total_price'. "
                "Update the 'total_price' by multiplying 'quantity' by 'unit_price' for those rows."
            )
            cursor.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT, quantity INTEGER, unit_price REAL, total_price REAL)")
            orders_data = [
                (1, 'Widget', 2, 10.0, 20.0),
                (2, 'Gadget', 1, 15.0, None), # Needs fix (15.0)
                (3, 'Doohickey', 5, 2.0, None), # Needs fix (10.0)
                (4, 'Thingamajig', 3, 5.0, 15.0)
            ]
            cursor.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", orders_data)

        elif difficulty == "hard":
            self._state.task_description = (
                "Task: Deduplicate the 'customers' table and resolve foreign keys in 'purchases'. "
                "Customer 'john.doe@email.com' exists twice (id 1 and id 3). "
                "Update all purchases for customer_id 3 to point to customer_id 1. "
                "Then, delete the duplicate customer (id 3) from the 'customers' table."
            )
            cursor.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT, name TEXT)")
            cursor.execute("CREATE TABLE purchases (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)")
            cursor.executemany("INSERT INTO customers VALUES (?, ?, ?)", [
                (1, 'john.doe@email.com', 'John Doe'),
                (2, 'jane.smith@email.com', 'Jane Smith'),
                (3, 'john.doe@email.com', 'J. Doe') # Duplicate
            ])
            cursor.executemany("INSERT INTO purchases VALUES (?, ?, ?)", [
                (101, 1, 50.0),
                (102, 2, 75.0),
                (103, 3, 120.0) # Belongs to John Doe
            ])
            
        self.conn.commit()

    def _get_schema(self) -> Dict[str, str]:
        """Helper to let the agent know what tables exist."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
        return {row['name']: row['sql'] for row in cursor.fetchall()}

    def reset(self, seed=None, episode_id=None, task="easy", **kwargs) -> DataWranglerObservation:
        # Hackathon requirement: Easy -> Medium -> Hard
        difficulty = task.lower() if task in ["easy", "medium", "hard"] else "easy"
        
        self._state = DataWranglerState(
            episode_id=episode_id or str(uuid.uuid4()),
            step_count=0,
            task_difficulty=difficulty
        )
        self._setup_database(difficulty)

        return DataWranglerObservation(
            done=False,
            reward=0.0,
            feedback_message=f"Environment reset. {self._state.task_description}",
            schema_info=self._get_schema(),
            query_results=None,
            rows_affected=0
        )

    def _grade_task(self) -> float:
        """Deterministic grader returning a score between 0.0 and 1.0."""
        cursor = self.conn.cursor()
        score = 0.0
        
        if self._state.task_difficulty == "easy":
            cursor.execute("SELECT is_active FROM users")
            rows = cursor.fetchall()
            correct = sum(1 for r in rows if r['is_active'] in ('1', '0'))
            score = correct / len(rows) if rows else 0.0

        elif self._state.task_difficulty == "medium":
            cursor.execute("SELECT quantity, unit_price, total_price FROM orders")
            rows = cursor.fetchall()
            correct = sum(1 for r in rows if r['total_price'] == (r['quantity'] * r['unit_price']))
            score = correct / len(rows) if rows else 0.0

        elif self._state.task_difficulty == "hard":
            cursor.execute("SELECT COUNT(*) as c FROM customers WHERE email='john.doe@email.com'")
            dupes = cursor.fetchone()['c']
            cursor.execute("SELECT COUNT(*) as c FROM purchases WHERE customer_id=3")
            orphaned = cursor.fetchone()['c']
            
            # Partial reward shaping!
            if dupes == 1: score += 0.5   # Deleted the duplicate customer
            if orphaned == 0: score += 0.5 # Fixed the foreign keys
            
        return score

    def step(self, action: DataWranglerAction, timeout_s=None, **kwargs) -> DataWranglerObservation:
        self._state.step_count += 1
        reward = 0.0
        done = False
        feedback = ""
        results = None
        rows_affected = 0

        # Action: SUBMIT
        if action.action_type == "submit_task":
            done = True
            reward = self._grade_task()
            if reward == 1.0:
                feedback = "Task submitted. Perfect score!"
            elif reward > 0.0:
                feedback = f"Task submitted. Partial success (Score: {reward})."
            else:
                feedback = "Task submitted. Failed to correct the data."
                
        # Action: EXECUTE SQL
        elif action.action_type == "execute_sql" and action.sql_query:
            try:
                cursor = self.conn.cursor()
                cursor.execute(action.sql_query)
                
                # If it's a SELECT query, fetch results
                if action.sql_query.strip().upper().startswith("SELECT"):
                    fetched = cursor.fetchmany(50) # Limit to 50 to avoid prompt overflow
                    results = [dict(row) for row in fetched]
                    feedback = "SELECT query executed successfully."
                    # Partial reward shaping: small reward for exploring data correctly
                    reward = 0.01 
                else:
                    self.conn.commit()
                    rows_affected = cursor.rowcount
                    feedback = f"Write query executed successfully. {rows_affected} rows affected."
                    # Partial reward shaping: small reward for writing data
                    reward = 0.05
                    
            except Exception as e:
                feedback = f"SQL Error: {str(e)}"
                reward = -0.05 # Penalize bad SQL
                
        else:
            feedback = "Invalid action_type or missing sql_query."

        return DataWranglerObservation(
            done=done,
            reward=reward,
            feedback_message=feedback,
            schema_info=self._get_schema(),
            query_results=results,
            rows_affected=rows_affected
        )

    @property
    def state(self) -> DataWranglerState:
        return self._state