import sqlite3
import uuid
from typing import Dict, Any
from openenv.core.env_server import Environment
from ..models import DataWranglerAction, DataWranglerObservation, DataWranglerState

class DataWranglerEnvironment(Environment):
    SUPPORTS_CONCURRENT_SESSIONS = True  

    def __init__(self):
        self._state = DataWranglerState()
        self.conn = None
        self._last_score = 0.0
        
    def _setup_database(self, difficulty: str):
        if self.conn:
            self.conn.close()
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()

        if difficulty == "easy":
            self._state.task_description = (
                "Task: Standardize 'is_active' in the 'users' table to exactly '1' or '0'."
            )
            cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, is_active TEXT)")
            # 5 rows total. Each row fixed gives exactly 0.2 reward!
            messy_data = [(1, 'Alice', 'Yes'), (2, 'Bob', 'N'), (3, 'Charlie', 'Y'), (4, 'Diana', 'No'), (5, 'Eve', 'Yes')]
            cursor.executemany("INSERT INTO users VALUES (?, ?, ?)", messy_data)

        elif difficulty == "medium":
            self._state.task_description = (
                "Task: Impute missing 'total_price' in 'orders' table (quantity * unit_price)."
            )
            cursor.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT, quantity INTEGER, unit_price REAL, total_price REAL)")
            # 4 rows total. Each row fixed gives exactly 0.25 reward!
            orders_data = [
                (1, 'Widget', 2, 10.0, None),
                (2, 'Gadget', 1, 15.0, None),
                (3, 'Doohickey', 5, 2.0, None),
                (4, 'Thingamajig', 3, 5.0, None)
            ]
            cursor.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", orders_data)

        elif difficulty == "hard":
            self._state.task_description = (
                "Task: Deduplicate 'customers' (delete id 3) and fix 'purchases' foreign keys to point to id 1."
            )
            cursor.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT, name TEXT)")
            cursor.execute("CREATE TABLE purchases (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)")
            cursor.executemany("INSERT INTO customers VALUES (?, ?, ?)", [
                (1, 'john.doe@email.com', 'John Doe'), (2, 'jane.smith@email.com', 'Jane Smith'), (3, 'john.doe@email.com', 'J. Doe')
            ])
            cursor.executemany("INSERT INTO purchases VALUES (?, ?, ?)", [(101, 1, 50.0), (102, 2, 75.0), (103, 3, 120.0)])
            
        self.conn.commit()

    def _get_schema(self) -> Dict[str, str]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
        return {row['name']: row['sql'] for row in cursor.fetchall()}

    def reset(self, seed=None, episode_id=None, task="easy", **kwargs) -> DataWranglerObservation:
        difficulty = task.lower() if task in ["easy", "medium", "hard"] else "easy"
        self._state = DataWranglerState(
            episode_id=episode_id or str(uuid.uuid4()),
            step_count=0,
            task_difficulty=difficulty
        )
        self._setup_database(difficulty)
        self._last_score = 0.0 # Reset tracking

        return DataWranglerObservation(
            done=False, reward=0.0,
            feedback_message=f"Environment reset. {self._state.task_description}",
            schema_info=self._get_schema()
        )

    def _grade_task(self) -> float:
        """Returns a continuous score from 0.0 to 1.0 based on database state."""
        cursor = self.conn.cursor()
        score = 0.0
        
        try:
            if self._state.task_difficulty == "easy":
                cursor.execute("SELECT is_active FROM users")
                rows = cursor.fetchall()
                correct = sum(1 for r in rows if r['is_active'] in ('1', '0'))
                score = correct / len(rows) if rows else 0.0

            elif self._state.task_difficulty == "medium":
                cursor.execute("SELECT quantity, unit_price, total_price FROM orders")
                rows = cursor.fetchall()
                correct = sum(1 for r in rows if r['total_price'] is not None and r['total_price'] == (r['quantity'] * r['unit_price']))
                score = correct / len(rows) if rows else 0.0

            elif self._state.task_difficulty == "hard":
                cursor.execute("SELECT COUNT(*) as c FROM customers WHERE email='john.doe@email.com'")
                dupes = cursor.fetchone()['c']
                cursor.execute("SELECT COUNT(*) as c FROM purchases WHERE customer_id=3")
                orphaned = cursor.fetchone()['c']
                
                # Fractional rewards!
                if dupes == 1: score += 0.5   
                if orphaned == 0: score += 0.5 
        except Exception:
            pass # If they broke the schema, score stays 0.0
            
        return round(score, 2)

    def step(self, action: DataWranglerAction, timeout_s=None, **kwargs) -> DataWranglerObservation:
        self._state.step_count += 1
        reward = 0.0
        done = False
        feedback = ""
        results = None
        rows_affected = 0

        if action.action_type == "submit_task":
            done = True
            current_score = self._grade_task()
            reward = current_score - self._last_score # Final delta
            if current_score == 1.0:
                feedback = "Task submitted. Perfect score!"
            elif current_score > 0.0:
                feedback = f"Task submitted. Partial success (Score: {current_score})."
            else:
                feedback = "Task submitted. Failed to correct the data."
                
        elif action.action_type == "execute_sql" and action.sql_query:
            try:
                cursor = self.conn.cursor()
                cursor.execute(action.sql_query)
                
                if action.sql_query.strip().upper().startswith("SELECT"):
                    fetched = cursor.fetchmany(50)
                    results = [dict(row) for row in fetched]
                    feedback = "SELECT query executed successfully."
                    reward = 0.01 # Small explore bonus
                else:
                    self.conn.commit()
                    rows_affected = cursor.rowcount
                    
                    # CALCULATE PARTIAL PROGRESS REWARD!
                    current_score = self._grade_task()