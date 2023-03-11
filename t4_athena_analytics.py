import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
#from airflow.utils.dates import days_ago
import Athena_query

DAG_ID = os.path.basename(__file__).replace(".py", "")

S3_BUCKET = Variable.get("data_lake_bucket")
ATHENA_RESULTS = Variable.get("athena_query_results")

DEFAULT_ARGS = {
    "owner": "chenfeng",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
    "output_location": f"s3://{ATHENA_RESULTS}/",
    "database": "spotify_db",
}


with DAG(
    dag_id=DAG_ID,
    description="Submit Amazon Athena CTAS queries",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=15),
    start_date=datetime(2023, 3, 1),
    schedule_interval=None,
    tags=["data lake", "aggregated", "gold"],
) as dag:

    athena_ctas_submit_category = AWSAthenaOperator(
        task_id="athena_ctas_genre", query=Athena_query.AGG_SONG_BY_GENRE
    )

    athena_ctas_submit_date = AWSAthenaOperator(
        task_id="athena_ctas_date", query=Athena_query.AGG_SONG_BY_DATE
    )

    athena_query_by_date = AWSAthenaOperator(
        task_id="athena_query_by_genre", query="sql/query.sql"
    )

    list_glue_tables = BashOperator(
        task_id="list_glue_tables",
        bash_command="""aws glue get-tables --database-name spotify \
                          --query 'TableList[].Name' --expression "agg_*"  \
                          --output table""",
    )


    (athena_ctas_submit_category >> athena_ctas_submit_date)>>athena_query_by_date >> list_glue_tables
