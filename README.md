# 🗄️ DataWranglerEnv: A Real-World Data Engineering OpenEnv

**A Meta / PyTorch Hackathon Submission**

DataWranglerEnv simulates the real-world tasks of a Data Engineer or Analyst. Agents are placed into an environment with an in-memory SQL database containing messy, incomplete, or duplicate data. The agent must explore the schema, query the data, write DML statements (`UPDATE`, `DELETE`) to clean it, and submit their work.

This environment avoids the "toy problem" trap by mimicking actual 80/20 data science workflows. It forces LLM agents to reason about schema relationships, write exact SQL, and verify their own work before submitting.

---

## 📊 The Tasks & Graders

The environment supports three levels of difficulty. The grader runs deterministic SQL `SELECT` queries on the in-memory database to verify the exact state of the data upon submission, returning a score between `0.0` and `1.0`.

### 🟢 Easy: Standardization
*   **The Problem:** The `users` table has a boolean `is_active` column filled with messy strings (`"Yes"`, `"Y"`, `"No"`, `"N"`). 
*   **The Objective:** Standardize all values to `"1"` (true) and `"0"` (false).
*   **Grader:** Checks the ratio of correctly standardized rows.

### 🟡 Medium: Data Imputation
*   **The Problem:** The `orders` table has missing (`NULL`) values in the `total_price` column.
*   **The Objective:** Calculate and impute the missing values using the formula `quantity * unit_price`.
*   **Grader:** Checks the ratio of rows where `total_price == quantity * unit_price`.

### 🔴 Hard: Entity Resolution & Foreign Key Repair
*   **The Problem:** The `customers` table contains duplicate users with the same email. The `purchases` table has foreign keys pointing to both the original and the duplicate.
*   **The Objective:** Re-point all purchases to the primary customer ID, and `DELETE` the duplicate customer record.
*   **Grader:** Uses **partial reward shaping**: `+0.5` for successfully deleting the duplicate customer, and `+0.5` for successfully migrating all orphaned foreign keys in the purchases table.

---

## 🛠️ Action & Observation Space

### Action Space
The agent responds with a JSON object mapped to the `DataWranglerAction` Pydantic model:
*   `action_type` (str): Either `"execute_sql"` or `"submit_task"`.
*   `sql_query` (str, optional): The raw SQL query to execute.

### Observation Space
The environment returns a `DataWranglerObservation` containing:
*   `feedback_message` (str): Success/error messages from the SQL engine.
*   `schema_info` (dict): The `CREATE TABLE` schemas for all current tables.
*   `query_results` (list): Up to 50 rows returned if the agent runs a `SELECT` query.
*   `rows_affected` (int): Number of rows modified by an `UPDATE`/`DELETE`.

### Reward Shaping
*   **Step Rewards:** `+0.01` for a successful `SELECT` (exploration), `+0.05` for a successful `UPDATE/DELETE` (writing), `-0.05` for SQL syntax errors.
*   **Final Reward:** Between `0.0` and `1.0` based on the deterministic grader when `submit_task` is called.

---

## 🚀 Setup & Installation

### Local Installation
```bash
git clone <your-repo-url>
cd data_wrangler_env
pip install -e .
