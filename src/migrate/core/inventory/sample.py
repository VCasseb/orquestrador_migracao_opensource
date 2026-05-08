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

    # ---- Medallion chain: cartoes (raw → bronze → silver → gold) ----
    # 4 BQ tables, each produced by a notebook of the same name
    for layer, size_gb, rows_count, hot in [
        ("raw",     12,    180_000_000, 410),
        ("bronze",  6.5,    98_000_000, 540),
        ("silver",  3.2,    52_000_000, 880),
        ("gold",    0.8,    14_500_000, 1620),
    ]:
        rows.append(TableMetadata(
            project="prj-data-prod", dataset=layer, name="cartoes",
            type="TABLE",
            size_bytes=int(size_gb * 1024**3),
            row_count=rows_count,
            partitioning="event_date" if layer in ("raw", "bronze") else None,
            query_count_30d=hot,
            source_kind="notebook",
            source_notebook_id=f"{layer}.cartoes",
            columns=_cols(
                ("card_id", "STRING"), ("customer_id", "STRING"), ("amount", "NUMERIC"),
                ("currency", "STRING"), ("event_ts", "TIMESTAMP"),
                ("event_date", "DATE"),
                *([("merchant_segment", "STRING"), ("risk_score", "FLOAT64")]
                  if layer in ("silver", "gold") else []),
            ),
            description=f"{layer.upper()}-layer cartoes (cards). Produced by {layer}.cartoes.py.",
        ))

    rows.append(TableMetadata(
        project="prj-data-prod", dataset="raw", name="legacy_etl_log",
        type="TABLE", size_bytes=3 * 1024**3, row_count=18_000_000, query_count_30d=4,
        source_kind="manual",
        columns=_cols(("run_id", "STRING"), ("status", "STRING"),
                      ("created_at", "TIMESTAMP")),
        description="Legacy ETL log — likely cold, candidate for archival.",
    ))

    rows.append(TableMetadata(
        project="prj-data-prod", dataset="raw", name="events_archive_external",
        type="EXTERNAL", size_bytes=None, row_count=None, query_count_30d=320,
        partitioning="event_date",
        source_kind="external_gcs",
        external_source_uris=["gs://prj-data-archive/events/year=*/month=*/day=*/*.parquet"],
        external_format="PARQUET",
        external_hive_partitioning=True,
        external_compression="SNAPPY",
        columns=_cols(
            ("event_id", "STRING"), ("user_id", "STRING"), ("event_type", "STRING"),
            ("event_ts", "TIMESTAMP"), ("event_date", "DATE"), ("payload", "JSON"),
        ),
        description="External table over Parquet archive in GCS. Hive-partitioned by date.",
    ))
    rows.append(TableMetadata(
        project="prj-marketing-prod", dataset="raw", name="impressions_csv_external",
        type="EXTERNAL", size_bytes=None, row_count=None, query_count_30d=12,
        source_kind="external_gcs",
        external_source_uris=["gs://prj-mkt-incoming/feeds/impressions/*.csv.gz"],
        external_format="CSV",
        external_hive_partitioning=False,
        external_compression="GZIP",
        columns=_cols(
            ("impression_id", "STRING"), ("campaign_id", "STRING"),
            ("user_id", "STRING"), ("event_ts", "TIMESTAMP"),
        ),
        description="External CSV feed from third-party DSP. Lands daily.",
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

_CARTOES_PIPELINE_SRC = '''"""cartoes_pipeline — orchestrates the medallion chain for `cartoes`.

Runs nightly. Each task executes the corresponding notebook on Vertex AI Workbench
in sequence: raw → bronze → silver → gold. If any layer fails the chain is aborted
(downstream notebooks would read stale/missing data).
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.vertex_ai.notebook import (
    VertexAINotebookExecuteOperator,
)

default_args = {"owner": "cards-data@company.com", "retries": 1}

with DAG(
    "cartoes_pipeline",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cartoes", "medallion", "daily"],
) as dag:

    run_raw = VertexAINotebookExecuteOperator(
        task_id="run_raw",
        notebook_uri="gs://acme-data-notebooks/raw/cartoes.py",
    )
    run_bronze = VertexAINotebookExecuteOperator(
        task_id="run_bronze",
        notebook_uri="gs://acme-data-notebooks/bronze/cartoes.py",
    )
    run_silver = VertexAINotebookExecuteOperator(
        task_id="run_silver",
        notebook_uri="gs://acme-data-notebooks/silver/cartoes.py",
    )
    run_gold = VertexAINotebookExecuteOperator(
        task_id="run_gold",
        notebook_uri="gs://acme-data-notebooks/gold/cartoes.py",
    )

    run_raw >> run_bronze >> run_silver >> run_gold
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
            name="cartoes_pipeline",
            file_path="gs://prj-composer-bucket/dags/cartoes_pipeline.py",
            schedule="0 4 * * *", owner="cards-data@company.com",
            tags=["cartoes", "medallion", "daily"],
            description="Orchestrates raw→bronze→silver→gold notebooks for the cartoes pipeline.",
            source_code=_CARTOES_PIPELINE_SRC,
            tasks=[
                DAGTask(task_id="run_raw",
                        operator="VertexAINotebookExecuteOperator",
                        notebook_uri="gs://acme-data-notebooks/raw/cartoes.py",
                        destination_table="prj-data-prod.raw.cartoes"),
                DAGTask(task_id="run_bronze",
                        operator="VertexAINotebookExecuteOperator",
                        notebook_uri="gs://acme-data-notebooks/bronze/cartoes.py",
                        referenced_tables=["prj-data-prod.raw.cartoes"],
                        destination_table="prj-data-prod.bronze.cartoes"),
                DAGTask(task_id="run_silver",
                        operator="VertexAINotebookExecuteOperator",
                        notebook_uri="gs://acme-data-notebooks/silver/cartoes.py",
                        referenced_tables=["prj-data-prod.bronze.cartoes"],
                        destination_table="prj-data-prod.silver.cartoes"),
                DAGTask(task_id="run_gold",
                        operator="VertexAINotebookExecuteOperator",
                        notebook_uri="gs://acme-data-notebooks/gold/cartoes.py",
                        referenced_tables=["prj-data-prod.silver.cartoes"],
                        destination_table="prj-data-prod.gold.cartoes"),
            ],
        ),
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


_CARTOES_RAW_CODE = '''"""raw.cartoes — pulls latest card transactions from Cloud SQL into BQ raw layer.

Owned by: cards-data@company.com
Schedule: 0 4 * * * (via cartoes_pipeline DAG)
"""
from google.cloud import bigquery
import pandas as pd
import sqlalchemy

PROJECT = "prj-data-prod"
DEST = f"{PROJECT}.raw.cartoes"

engine = sqlalchemy.create_engine("postgresql+pg8000://cards-cloudsql:5432/cards")
df = pd.read_sql("""
  SELECT card_id, customer_id, amount::numeric AS amount,
         currency, event_ts, event_ts::date AS event_date
  FROM card_transactions
  WHERE event_ts >= NOW() - INTERVAL '1 day'
""", engine)

import pandas_gbq
pandas_gbq.to_gbq(df, DEST, project_id=PROJECT, if_exists="append")
'''

_CARTOES_BRONZE_CODE = '''"""bronze.cartoes — cleans and conforms raw transactions, dedupe + null filtering."""
from google.cloud import bigquery
import pandas_gbq

SQL = """
SELECT
  card_id, customer_id,
  CAST(amount AS NUMERIC) AS amount,
  UPPER(currency) AS currency,
  event_ts, event_date
FROM `prj-data-prod.raw.cartoes`
WHERE card_id IS NOT NULL
  AND amount IS NOT NULL
  AND event_ts > '2020-01-01'
QUALIFY ROW_NUMBER() OVER (PARTITION BY card_id, event_ts ORDER BY event_ts DESC) = 1
"""

df = pandas_gbq.read_gbq(SQL, project_id="prj-data-prod")
pandas_gbq.to_gbq(df, "prj-data-prod.bronze.cartoes", project_id="prj-data-prod", if_exists="replace")
'''

_CARTOES_SILVER_CODE = '''"""silver.cartoes — enriches bronze with merchant segments + simple fraud risk."""
import pandas_gbq

SQL = """
SELECT
  b.card_id, b.customer_id, b.amount, b.currency, b.event_ts, b.event_date,
  m.segment AS merchant_segment,
  CASE WHEN b.amount > 5000 OR m.risk_flag THEN 0.85 ELSE 0.10 END AS risk_score
FROM `prj-data-prod.bronze.cartoes` b
LEFT JOIN `prj-data-prod.raw.merchants` m
  ON SUBSTR(b.card_id, 1, 4) = m.bin_code
"""

df = pandas_gbq.read_gbq(SQL, project_id="prj-data-prod")
pandas_gbq.to_gbq(df, "prj-data-prod.silver.cartoes", project_id="prj-data-prod", if_exists="replace")
'''

_CARTOES_GOLD_CODE = '''"""gold.cartoes — daily customer-level KPIs on top of silver."""
import pandas_gbq

SQL = """
SELECT
  customer_id,
  event_date,
  COUNT(*) AS txn_count,
  SUM(amount) AS total_amount,
  AVG(risk_score) AS avg_risk_score,
  COUNT(DISTINCT merchant_segment) AS unique_segments
FROM `prj-data-prod.silver.cartoes`
GROUP BY customer_id, event_date
"""

df = pandas_gbq.read_gbq(SQL, project_id="prj-data-prod")
pandas_gbq.to_gbq(df, "prj-data-prod.gold.cartoes", project_id="prj-data-prod", if_exists="replace")
'''


def _build_notebooks() -> list[NotebookMetadata]:
    # ---- Cartoes medallion chain ----
    cartoes_chain = []
    for layer, code in [
        ("raw",    _CARTOES_RAW_CODE),
        ("bronze", _CARTOES_BRONZE_CODE),
        ("silver", _CARTOES_SILVER_CODE),
        ("gold",   _CARTOES_GOLD_CODE),
    ]:
        if layer == "raw":
            reads = []
            writes = ["prj-data-prod.raw.cartoes"]
        else:
            prev = {"bronze": "raw", "silver": "bronze", "gold": "silver"}[layer]
            reads = [f"prj-data-prod.{prev}.cartoes"]
            writes = [f"prj-data-prod.{layer}.cartoes"]
        cartoes_chain.append(NotebookMetadata(
            name=f"{layer}.cartoes",
            location=f"gs://acme-data-notebooks/{layer}/cartoes.py",
            kind="vertex_workbench",
            description=f"{layer.upper()}-layer cartoes notebook. Reads {', '.join(reads) or '(external)'}; writes {writes[0]}.",
            libraries=["google.cloud.bigquery", "pandas_gbq"] + (["sqlalchemy"] if layer == "raw" else []),
            cells=[
                NotebookCell(cell_type="markdown", source=f"# {layer.upper()}-layer cartoes\n\nProduces `{writes[0]}`."),
                NotebookCell(
                    cell_type="code", source=code, has_sql=True,
                    sql_extracted=code,  # code IS SQL-flavored
                    referenced_tables=reads,
                    writes_tables=writes,
                ),
            ],
        ))

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
    return cartoes_chain + [
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
