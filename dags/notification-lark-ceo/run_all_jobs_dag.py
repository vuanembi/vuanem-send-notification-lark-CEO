from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "run_all_jobs_dag",
    default_args=default_args,
    description="Run all notification jobs",
    schedule_interval="30 1 * * *",  # 01:30 UTC hàng ngày
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    run_sales = KubernetesPodOperator(
        task_id="run_Metrics_Sales",
        name="metrics-sales-job",
        namespace="composer-2-13-1-airflow-2-10-5-8b11d9ce",
        image="us-docker.pkg.dev/voltaic-country-280607/docker-1/lark-jobs:latest",
        cmds=["python"],
        arguments=["/Metrics_Sales.py"],
        is_delete_operator_pod=True,
        get_logs=True,
        image_pull_policy="Always",
    )
    
    run_top10_store = KubernetesPodOperator(
        task_id="run_Top10_cuahang",
        name="top10-store-job",
        namespace="composer-2-13-1-airflow-2-10-5-8b11d9ce",
        image="us-docker.pkg.dev/voltaic-country-280607/docker-1/lark-jobs:latest",
        cmds=["python"],
        arguments=["/Top10_cuahang.py"],
        is_delete_operator_pod=True,
        get_logs=True,
        image_pull_policy="Always",
    )
    
    run_top10_5 = KubernetesPodOperator(
        task_id="run_Top10_Top5",
        name="top10-top5-job",
        namespace="composer-2-13-1-airflow-2-10-5-8b11d9ce",
        image="us-docker.pkg.dev/voltaic-country-280607/docker-1/lark-jobs:latest",
        cmds=["python"],
        arguments=["/Top10AndTop5.py"],
        is_delete_operator_pod=True,
        get_logs=True,
        image_pull_policy="Always",
    )
    
    [run_sales, run_top10_store, run_top10_5]
