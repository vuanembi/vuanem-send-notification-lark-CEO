from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# ================= CONFIG =================
# Thư mục chứa scripts (chính là thư mục hiện tại của file DAG này)
JOB_DIR = os.path.dirname(__file__)

# Danh sách scripts cần chạy
SCRIPTS = [
    "Metrics_Sales.py",
    "Top10AndTop5.py",
    "Top10_cuahang.py"
]

# ================= DAG DEFINITION =================
default_args = {
    "owner": "vuanem",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="run_all_jobs_dag",
    default_args=default_args,
    description="Run all sales scripts and push to Lark",
    schedule_interval="30 8 * * *",   # chạy 8h30 sáng hàng ngày
    start_date=datetime(2025, 9, 10),
    catchup=False,
    tags=["sales", "lark"],
) as dag:

    tasks = []
    for script in SCRIPTS:
        task = BashOperator(
            task_id=f"run_{script.replace('.py','')}",
            bash_command=f"python {os.path.join(JOB_DIR, script)}"
        )
        tasks.append(task)

    # Chain theo thứ tự
    tasks[0] >> tasks[1] >> tasks[2]
