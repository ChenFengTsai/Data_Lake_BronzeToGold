import os
from datetime import timedelta
import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
#from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator

DAG_ID = os.path.basename(__file__).replace(".py", "")

S3_BUCKET = Variable.get("data_lake_bucket")

### fix this
CRAWLERS = ["song_aurora", "artist_mysql", "album_postgresql"]

DEFAULT_ARGS = {
    "owner": "chenfeng",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="Create Table and Run Crawlers from RDS",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=15),
    start_date=datetime(2023, 3, 1),
    schedule_interval=None,
    tags=["data lake", "source"],
) as dag:
    
    delete_catalog = BashOperator(
        task_id="delete_catalog",
        bash_command='aws glue delete-database --name spotify_db || echo "Database spotify_db not found."',
    )

    create_catalog = BashOperator(
        task_id="create_catalog",
        bash_command="""aws glue create-database --database-input \
            '{"Name": "spotify_db", "Description": "Datasets from relational database"}'""",
    )
    list_glue_tables = BashOperator(
        task_id="list_glue_tables",
        bash_command="""aws glue get-tables --database-name spotify_db \
                          --query 'TableList[].Name' --expression "source_*"  \
                          --output table""",
    )
    
    # Branch for crawlers_run tasks
    
    ## already confiugured each crawler that they are connected to each db
    crawlers_run_tasks = []
    for crawler in CRAWLERS:
        crawlers_run = AwsGlueCrawlerOperator(
            task_id=f"run_{crawler}_crawler", config={"Name": crawler}
        )
        crawlers_run_tasks.append(crawlers_run)


    (delete_catalog >> create_catelog >> crawlers_run >> list_glue_tables)

