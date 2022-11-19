import datetime
from airflow import models
from airflow.operators import bash
from airflow.providers.google.cloud.operators import bigquery
from airflow.providers.google.cloud.transfers import bigquery_to_gcs
from airflow.utils import trigger_rule


bq_dataset_name = "airflow_bq_notify_dataset_{{ ds_nodash }}"
bq_recent_questions_table_id = "recent_questions"
gcs_bucket = "{{ var.value.gcs_bucket }}"
output_file = f"{gcs_bucket}/recent_questions.csv"
location = "us-east1"
project_id = "{{ var.value.gcp_project }}"

max_query_date = "2018-02-01"
min_query_date = "2018-01-01"

RECENT_QUESTIONS_QUERY = f"""
        SELECT owner_display_name, title, view_count
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE creation_date < CAST('{max_query_date}' AS TIMESTAMP)
            AND creation_date >= CAST('{min_query_date}' AS TIMESTAMP)
        ORDER BY view_count DESC
        LIMIT 100
        """

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)

default_dag_args = {
    "start_date": yesterday,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=5),
}

with models.DAG(
    "composer_sample_bq_notify",
    schedule_interval=datetime.timedelta(weeks=4),
    default_args=default_dag_args,
    catchup=False
) as dag:

    make_bq_dataset = bash.BashOperator(
        task_id="make_bq_dataset",
        bash_command=f"bq ls {bq_dataset_name} || bq mk --location {location} {bq_dataset_name}",
    )

    bq_recent_questions_query = bigquery.BigQueryInsertJobOperator(
        task_id="bq_recent_questions_query",
        configuration={
            "query": {
                "query": RECENT_QUESTIONS_QUERY,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": bq_dataset_name,
                    "tableId": bq_recent_questions_table_id,
                },
            }
        },
        location="US",
    )

    export_questions_to_gcs = bigquery_to_gcs.BigQueryToGCSOperator(
        task_id="export_recent_questions_to_gcs",
        source_project_dataset_table=f"{project_id}.{bq_dataset_name}.{bq_recent_questions_table_id}",
        destination_cloud_storage_uris=[output_file],
        export_format="CSV",
    )

    delete_bq_dataset = bash.BashOperator(
        task_id="delete_bq_dataset",
        bash_command="bq rm -r -f %s" % bq_dataset_name,
        # trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
    )

    (
        make_bq_dataset
        >> bq_recent_questions_query
        >> export_questions_to_gcs
        >> delete_bq_dataset
    )