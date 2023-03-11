import os
import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
#from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")


### fix this one same as previous dag
TABLES = ["song", "artist", "album"]

DEFAULT_ARGS = {
    "owner": "chenfeng",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="Run Glue ETL Jobs - bronze to silver",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=15),
    start_date=datetime(2023, 3, 1),
    schedule_interval=None,
    tags=["data lake", "refined", "silver"],
) as dag:


    list_glue_tables = BashOperator(
        task_id="list_glue_tables",
        bash_command="""aws glue get-tables --database-name spotify_db \
                          --query 'TableList[].Name' --expression "refined_*"  \
                          --output table""",
    )
    
    glue_jobs_tasks=[]
    for table in TABLES:
        glue_jobs_silver = AwsGlueJobOperator(
            task_id=f"start_job_{table}_silver",
            job_name=f"spotify_{table}_silver",
        )
        glue_jobs_tasks.append(glue_jobs_silver)


    (glue_jobs_tasks >> list_glue_tables)

