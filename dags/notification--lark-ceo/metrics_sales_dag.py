from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunJobRunOperator
from datetime import datetime

with DAG(
    dag_id="metrics_sales_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["lark", "sales"],
) as dag:

    run_job = CloudRunJobRunOperator(
        task_id="run_metrics_sales",
        project_id="voltaic-country-280607",
        region="us-central1",
        job_name="send-lark-metrics-sales",  # tên Cloud Run Job bạn sẽ deploy
    )
