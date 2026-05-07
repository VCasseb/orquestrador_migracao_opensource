"""Synthetic inventory for demo/dev when no GCP credentials are configured."""
from __future__ import annotations

from datetime import datetime, timezone

from migrate.core.inventory.models import (
    ColumnMetadata, DAGMetadata, DAGTask, Inventory,
    NotebookCell, NotebookMetadata, ScheduledQuery, TableMetadata,
)


def _cols(*specs: tuple[str, str]) -> list[ColumnMetadata]:
    return [ColumnMetadata(name=n, type=t) for n, t in specs]


def _build_tables() -> list[TableMetadata]:
    rows: list[TableMetadata] = []

    rows.append(TableMetadata(
        project="prj-data-prod", dataset="raw", name="orders",
        type="TABLE", size_bytes=82 * 1024**3, row_count=950_000_000,
        partitioning="event_date", clustering=["customer_id"], query_count_30d=1240,
        source_kind="dag_python",
        source_dag_id="ingest_orders_streaming",
        source_dag_task="batch_load",
        columns=_cols(
            ("order_id", "STRING"), ("customer_id", "STRING"), ("amount", "NUMERIC"),
            ("currency", "STRING"), ("event_ts", "TIMESTAMP"), ("event_date", "DATE"),
        ),
    ))
    rows.append(TableMetadata(
        project="prj-data-prod", dataset="raw", name="customers",
        type="TABLE", size_bytes=2 * 1024**3, row_count=4_500_000, query_count_30d=820,
        source_kind="manual",
        columns=_cols(
            ("customer_id", "STRING"), ("email", "STRING"), ("country", "STRING"),
            ("created_at", "TIMESTAMP"), ("status", "STRING"),
        ),
    ))
    rows.append(TableMetadata(
        project="prj-data-prod", dataset="raw", name="products",
        type="TABLE", size_bytes=200 * 1024**2, row_count=180_000, query_count_30d=410,
        source_kind="manual",
        columns=_cols(
            ("product_id", "STRING"), ("name", "STRING"), ("category", "STRING"),
            ("price", "NUMERIC"), ("active", "BOOL"),
        ),
    ))
    rows.append(TableMetadata(
        project="prj-data-prod", dataset="raw", name="events",
        type="TABLE", size_bytes=420 * 1024**3, row_count=12_000_000_000,
        partitioning="event_date", query_count_30d=2300,
        source_kind="manual",
        columns=_cols(
            ("event_id", "STRING"), ("user_id", "STRING"), ("event_type", "STRING"),
            ("event_ts", "TIMESTAMP"), ("event_date", "DATE"), ("payload", "JSON"),
        ),
    ))

    rows.append(TableMetadata(
        project="prj-data-prod", dataset="silver", name="orders_enriched",
        type="VIEW", query_count_30d=540,
        source_kind="view",
        view_query="""SELECT o.order_id, o.customer_id, o.amount, o.currency,
       c.country, c.email, p.category, o.event_ts
FROM `prj-data-prod.raw.orders` o
LEFT JOIN `prj-data-prod.raw.customers` c USING (customer_id)
LEFT JOIN `prj-data-prod.raw.products` p USING (product_id)""",
        columns=_cols(("order_id", "STRING"), ("customer_id", "STRING"),
                      ("amount", "NUMERIC"), ("country", "STRING")),
    ))
    rows.append(TableMetadata(
        project="prj-data-prod", dataset="silver", name="user_sessions",
        type="VIEW", query_count_30d=720,
        source_kind="view",
        view_query="""SELECT user_id,
       MIN(event_ts) AS session_start,
       MAX(event_ts) AS session_end,
       COUNT(*) AS event_count
FROM `prj-data-prod.raw.events`
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY user_id""",
        columns=_cols(("user_id", "STRING"), ("session_start", "TIMESTAMP"),
                      ("session_end", "TIMESTAMP"), ("event_count", "INT64")),
    ))

    rows.append(TableMetadata(
        project="prj-finance-prod", dataset="gold", name="revenue_daily",
        type="TABLE", size_bytes=120 * 1024**2, row_count=900_000, query_count_30d=1850,
        source_kind="dag_sql",
        source_dag_id="finance_daily_aggregations",
        source_dag_task="build_revenue_daily",
        source_schedule="0 3 * * *",
        source_sql="""SELECT DATE(event_ts) AS revenue_date,
       country,
       SUM(amount) AS revenue_total,
       COUNT(DISTINCT customer_id) AS unique_buyers
FROM `prj-data-prod.silver.orders_enriched`
GROUP BY revenue_date, country
QUALIFY ROW_NUMBER() OVER (PARTITION BY revenue_date, country ORDER BY revenue_total DESC) = 1""",
        columns=_cols(("revenue_date", "DATE"), ("country", "STRING"),
                      ("revenue_total", "NUMERIC"), ("unique_buyers", "INT64")),
    ))
    rows.append(TableMetadata(
        project="prj-finance-prod", dataset="gold", name="cohort_retention",
        type="TABLE", size_bytes=15 * 1024**2, row_count=120_000, query_count_30d=180,
        source_kind="scheduled_query",
        source_scheduled_query_id="finance-cohort-weekly",
        source_schedule="every monday 06:00",
        source_sql="""WITH cohorts AS (
  SELECT customer_id, DATE_TRUNC(MIN(DATE(event_ts)), MONTH) AS cohort_month
  FROM `prj-data-prod.silver.orders_enriched`
  GROUP BY customer_id
)
SELECT cohort_month,
       DATE_DIFF(DATE(o.event_ts), cohort_month, MONTH) AS month_offset,
       COUNT(DISTINCT o.customer_id) AS retained
FROM cohorts c
JOIN `prj-data-prod.silver.orders_enriched` o USING (customer_id)
GROUP BY cohort_month, month_offset""",
        columns=_cols(("cohort_month", "DATE"), ("month_offset", "INT64"),
                      ("retained", "INT64")),
    ))
    rows.append(TableMetadata(
        project="prj-finance-prod", dataset="gold", name="exec_dashboard_kpis",
        type="VIEW", query_count_30d=2100,
        source_kind="view",
        view_query="""SELECT revenue_date, SUM(revenue_total) AS revenue,
       SUM(unique_buyers) AS buyers
FROM `prj-finance-prod.gold.revenue_daily`
GROUP BY revenue_date""",
        columns=_cols(("revenue_date", "DATE"), ("revenue", "NUMERIC"),
                      ("buyers", "INT64")),
    ))

    rows.append(TableMetadata(
        project="prj-marketing-prod", dataset="raw", name="campaigns",
        type="TABLE", size_bytes=15 * 1024**2, row_count=3_400, query_count_30d=92,
        source_kind="manual",
        columns=_cols(
            ("campaign_id", "STRING"), ("name", "STRING"), ("channel", "STRING"),
            ("started_at", "TIMESTAMP"), ("budget", "NUMERIC"),
        ),
    ))
    rows.append(TableMetadata(
        project="prj-marketing-prod", dataset="raw", name="impressions",
        type="TABLE", size_bytes=180 * 1024**3, row_count=4_800_000_000,
        partitioning="event_date", query_count_30d=620,
        source_kind="manual",
        columns=_cols(
            ("impression_id", "STRING"), ("campaign_id", "STRING"), ("user_id", "STRING"),
            ("event_ts", "TIMESTAMP"), ("event_date", "DATE"),
        ),
    ))
    rows.append(TableMetadata(
        project="prj-marketing-prod", dataset="silver", name="campaign_performance",
        type="TABLE", size_bytes=80 * 1024**2, row_count=420_000, query_count_30d=240,
        source_kind="dag_notebook",
        source_dag_id="marketing_attribution",
        source_dag_task="run_attribution_notebook",
        source_notebook_id="marketing_attribution_v3",
        source_schedule="0 4 * * *",
        columns=_cols(("campaign_id", "STRING"), ("name", "STRING"),
                      ("impressions", "INT64"), ("reach", "INT64")),
    ))
    rows.append(TableMetadata(
        project="prj-marketing-prod", dataset="gold", name="attribution_model",
        type="VIEW", query_count_30d=410,
        source_kind="view",
        view_query="""SELECT cp.campaign_id, cp.channel,
       cp.impressions, oe.amount AS attributed_revenue
FROM `prj-marketing-prod.silver.campaign_performance` cp
LEFT JOIN `prj-data-prod.silver.orders_enriched` oe
  ON oe.customer_id IN (SELECT user_id FROM `prj-marketing-prod.raw.impressions` WHERE campaign_id = cp.campaign_id)""",
        columns=_cols(("campaign_id", "STRING"), ("channel", "STRING"),
                      ("attributed_revenue", "NUMERIC")),
    ))

    rows.append(TableMetadata(
        project="prj-data-prod", dataset="raw", name="legacy_etl_log",
        type="TABLE", size_bytes=3 * 1024**3, row_count=18_000_000, query_count_30d=4,
        source_kind="manual",
        columns=_cols(("run_id", "STRING"), ("status", "STRING"),
                      ("created_at", "TIMESTAMP")),
        description="Legacy ETL log — likely cold, candidate for archival.",
    ))

    return rows


_FINANCE_DAILY_SRC = '''"""Daily finance gold-layer aggregations.

Owner: finance-data@company.com
Schedule: 03:00 UTC every day
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

default_args = {
    "owner": "finance-data@company.com",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

REVENUE_DAILY_SQL = """
SELECT
  DATE(event_ts) AS revenue_date,
  country,
  SUM(amount) AS revenue_total,
  COUNT(DISTINCT customer_id) AS unique_buyers
FROM `prj-data-prod.silver.orders_enriched`
GROUP BY revenue_date, country
QUALIFY ROW_NUMBER() OVER (PARTITION BY revenue_date, country ORDER BY revenue_total DESC) = 1
"""

with DAG(
    "finance_daily_aggregations",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finance", "daily", "gold"],
) as dag:

    build_revenue_daily = BigQueryInsertJobOperator(
        task_id="build_revenue_daily",
        configuration={
            "query": {
                "query": REVENUE_DAILY_SQL,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "prj-finance-prod",
                    "datasetId": "gold",
                    "tableId": "revenue_daily",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    notify_finance = SlackWebhookOperator(
        task_id="notify_finance",
        http_conn_id="slack_default",
        message="Revenue daily refresh complete: {{ ds }}",
    )

    build_revenue_daily >> notify_finance
'''

_INGEST_ORDERS_SRC = '''"""Streams orders from Pub/Sub into BQ raw.

15-minute micro-batches. Producer: order-service in prod GKE.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import pubsub_v1, bigquery


def batch_load(**context):
    """Pull messages from `projects/prj-data-prod/topics/orders-events`,
    insert into `prj-data-prod.raw.orders`."""
    subscriber = pubsub_v1.SubscriberClient()
    bq = bigquery.Client(project="prj-data-prod")
    sub_path = subscriber.subscription_path(
        "prj-data-prod", "orders-batch-loader"
    )
    response = subscriber.pull(
        request={"subscription": sub_path, "max_messages": 5000},
    )
    rows = [_parse_event(m.message.data) for m in response.received_messages]
    if rows:
        errors = bq.insert_rows_json("prj-data-prod.raw.orders", rows)
        if errors:
            raise RuntimeError(f"BQ insert failed: {errors}")
    subscriber.acknowledge(request={
        "subscription": sub_path,
        "ack_ids": [m.ack_id for m in response.received_messages],
    })


with DAG(
    "ingest_orders_streaming",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingest", "raw", "streaming"],
) as dag:
    batch_load_task = PythonOperator(
        task_id="batch_load",
        python_callable=batch_load,
    )
'''

_MARKETING_ATTRIBUTION_SRC = '''"""Daily marketing attribution.

Runs the multi-touch attribution notebook on Vertex AI Workbench, then
materializes the result into BigQuery.
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.vertex_ai.notebook import (
    VertexAINotebookExecuteOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {"owner": "marketing-data@company.com"}

ATTRIBUTION_SQL = """
SELECT
  cp.campaign_id, cp.name, cp.channel,
  COUNT(*) AS impressions,
  COUNT(DISTINCT i.user_id) AS reach
FROM `prj-marketing-prod.raw.campaigns` cp
LEFT JOIN `prj-marketing-prod.raw.impressions` i USING (campaign_id)
GROUP BY cp.campaign_id, cp.name, cp.channel
"""

with DAG(
    "marketing_attribution",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    tags=["marketing", "ml"],
) as dag:

    run_attribution_notebook = VertexAINotebookExecuteOperator(
        task_id="run_attribution_notebook",
        notebook_uri="gs://prj-mkt-notebooks/attribution/marketing_attribution_v3.ipynb",
        parameters={"date": "{{ ds }}"},
    )

    materialize_to_bq = BigQueryInsertJobOperator(
        task_id="materialize_to_bq",
        configuration={
            "query": {
                "query": ATTRIBUTION_SQL,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "prj-marketing-prod",
                    "datasetId": "silver",
                    "tableId": "campaign_performance",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    run_attribution_notebook >> materialize_to_bq
'''


def _build_dags() -> list[DAGMetadata]:
    return [
        DAGMetadata(
            name="finance_daily_aggregations",
            file_path="gs://prj-composer-bucket/dags/finance_daily_aggregations.py",
            schedule="0 3 * * *", owner="finance-data@company.com",
            tags=["finance", "daily", "gold"],
            description="Builds gold-layer finance aggregations every morning.",
            source_code=_FINANCE_DAILY_SRC,
            tasks=[
                DAGTask(
                    task_id="build_revenue_daily",
                    operator="BigQueryInsertJobOperator",
                    sql="SELECT DATE(event_ts) AS revenue_date, country, SUM(amount) AS revenue_total, COUNT(DISTINCT customer_id) AS unique_buyers FROM `prj-data-prod.silver.orders_enriched` GROUP BY revenue_date, country QUALIFY ROW_NUMBER() OVER (PARTITION BY revenue_date, country ORDER BY revenue_total DESC) = 1",
                    destination_table="prj-finance-prod.gold.revenue_daily",
                    referenced_tables=["prj-data-prod.silver.orders_enriched"],
                ),
                DAGTask(
                    task_id="notify_finance",
                    operator="SlackWebhookOperator",
                ),
            ],
        ),
        DAGMetadata(
            name="ingest_orders_streaming",
            file_path="gs://prj-composer-bucket/dags/ingest_orders_streaming.py",
            schedule="*/15 * * * *", owner="data-platform@company.com",
            tags=["ingest", "raw", "streaming"],
            description="Pulls order events from Pub/Sub topic, batches into BQ raw table.",
            source_code=_INGEST_ORDERS_SRC,
            tasks=[
                DAGTask(
                    task_id="batch_load",
                    operator="PythonOperator",
                    destination_table="prj-data-prod.raw.orders",
                ),
            ],
        ),
        DAGMetadata(
            name="marketing_attribution",
            file_path="gs://prj-composer-bucket/dags/marketing_attribution.py",
            schedule="0 4 * * *", owner="marketing-data@company.com",
            tags=["marketing", "ml"],
            description="Runs the attribution notebook on Vertex AI Workbench.",
            source_code=_MARKETING_ATTRIBUTION_SRC,
            tasks=[
                DAGTask(
                    task_id="run_attribution_notebook",
                    operator="VertexAINotebookExecuteOperator",
                    notebook_uri="gs://prj-mkt-notebooks/attribution/marketing_attribution_v3.ipynb",
                ),
                DAGTask(
                    task_id="materialize_to_bq",
                    operator="BigQueryInsertJobOperator",
                    destination_table="prj-marketing-prod.silver.campaign_performance",
                    referenced_tables=["prj-marketing-prod.raw.campaigns", "prj-marketing-prod.raw.impressions"],
                ),
            ],
        ),
    ]


def _build_notebooks() -> list[NotebookMetadata]:
    code1 = (
        "from google.cloud import bigquery\n"
        "import pandas as pd\n"
        "client = bigquery.Client()\n"
        "df = client.query(\"\"\"\n"
        "    SELECT campaign_id, COUNT(*) AS impressions\n"
        "    FROM `prj-marketing-prod.raw.impressions`\n"
        "    GROUP BY campaign_id\n"
        "\"\"\").to_dataframe()\n"
        "# attribution model fits here\n"
        "import pandas_gbq\n"
        "pandas_gbq.to_gbq(df, 'prj-marketing-prod.silver.campaign_performance')\n"
    )
    return [
        NotebookMetadata(
            name="marketing_attribution_v3",
            location="gs://prj-mkt-notebooks/attribution/marketing_attribution_v3.ipynb",
            kind="vertex_workbench",
            description="Multi-touch attribution model. Runs daily via Composer.",
            libraries=["google.cloud.bigquery", "pandas_gbq"],
            cells=[
                NotebookCell(cell_type="markdown", source="# Marketing attribution v3"),
                NotebookCell(
                    cell_type="code", source=code1, has_sql=True,
                    sql_extracted="SELECT campaign_id, COUNT(*) AS impressions FROM `prj-marketing-prod.raw.impressions` GROUP BY campaign_id",
                    referenced_tables=["prj-marketing-prod.raw.impressions"],
                    writes_tables=["prj-marketing-prod.silver.campaign_performance"],
                ),
            ],
        ),
        NotebookMetadata(
            name="ml_churn_training",
            location="gs://prj-data-notebooks/ml/churn_training.ipynb",
            kind="vertex_workbench",
            description="Trains churn prediction model — out of scope for migration.",
            libraries=["google.cloud.bigquery", "tensorflow"],
            cells=[
                NotebookCell(cell_type="markdown", source="# Churn model training"),
                NotebookCell(cell_type="code", source="# tensorflow training code...\n"),
            ],
        ),
    ]


def _build_scheduled_queries() -> list[ScheduledQuery]:
    return [
        ScheduledQuery(
            name="finance-cohort-weekly",
            config_id="projects/prj-finance-prod/locations/us/transferConfigs/abc123",
            schedule="every monday 06:00",
            query=(
                "WITH cohorts AS ("
                "  SELECT customer_id, DATE_TRUNC(MIN(DATE(event_ts)), MONTH) AS cohort_month "
                "  FROM `prj-data-prod.silver.orders_enriched` GROUP BY customer_id"
                ") SELECT cohort_month, "
                "DATE_DIFF(DATE(o.event_ts), cohort_month, MONTH) AS month_offset, "
                "COUNT(DISTINCT o.customer_id) AS retained "
                "FROM cohorts c JOIN `prj-data-prod.silver.orders_enriched` o USING (customer_id) "
                "GROUP BY cohort_month, month_offset"
            ),
            destination_table="prj-finance-prod.gold.cohort_retention",
            project="prj-finance-prod",
        ),
    ]


def build_sample() -> Inventory:
    return Inventory(
        scanned_at=datetime.now(timezone.utc),
        projects=["prj-data-prod", "prj-finance-prod", "prj-marketing-prod"],
        tables=_build_tables(),
        dags=_build_dags(),
        notebooks=_build_notebooks(),
        scheduled_queries=_build_scheduled_queries(),
    )
