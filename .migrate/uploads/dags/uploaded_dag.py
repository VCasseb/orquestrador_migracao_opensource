from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

with DAG("uploaded_demo", schedule_interval="@daily", start_date=datetime(2024, 1, 1)) as dag:
    t = BigQueryInsertJobOperator(
        task_id="run_query",
        configuration={"query": {"query": "SELECT 1 FROM `prj-data-prod.silver.foo`",
                                  "destinationTable": {"projectId": "prj-data-prod",
                                                        "datasetId": "gold",
                                                        "tableId": "uploaded_demo"}}},
    )
