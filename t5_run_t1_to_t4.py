import os
from datetime import timedelta
import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
#from airflow.utils.dates import days_ago


DAG_ID = os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS = {
    "owner": "chenfeng",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="Run all Data Lake Demonstration DAGs",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=30),
    start_date=datetime(2023, 3, 1),
    schedule_interval=None,
    tags=["data lake"],
) as dag:


    trigger_dag_01 = TriggerDagRunOperator(
        task_id="trigger_dag_01",
        trigger_dag_id="t1_create_table_crawl",
        wait_for_completion=True,
    )

    trigger_dag_02 = TriggerDagRunOperator(
        task_id="trigger_dag_02",
        trigger_dag_id="t2_glue_job_bronze",
        wait_for_completion=True,
    )

    trigger_dag_03 = TriggerDagRunOperator(
        task_id="trigger_dag_03",
        trigger_dag_id="t3_glue_job_silver",
        wait_for_completion=True,
    )

    trigger_dag_04 = TriggerDagRunOperator(
        task_id="trigger_dag_04",
        trigger_dag_id="t4_athena_analytics",
        wait_for_completion=True,
    )


    (trigger_dag_01 >> trigger_dag_02 >> trigger_dag_03 >> trigger_dag_04)

