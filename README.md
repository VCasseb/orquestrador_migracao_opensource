# migrate

AI-driven framework for migrating **GCP Airflow pipelines (Composer DAGs `.py`) and notebooks (`.ipynb` / `.py`)** to **Databricks** or **AWS** (MWAA + EMR / S3).

> **Scope, in plain words:** *we migrate **code artifacts** — the orchestration (DAGs) and the transformation logic (notebooks). We do NOT migrate table data.* BQ tables, views and scheduled queries are scanned for **lineage context only** (so you can see "this `gold.cartoes.py` notebook reads `silver.cartoes` and writes `gold.cartoes`").

## What's migrated, what's referenced

| Object | Role | Migration target |
|---|---|---|
| **Composer DAG** (`.py`) | First-class — primary migration target | Databricks Workflow (Asset Bundle YAML) **or** MWAA-compatible Airflow `.py` |
| **Notebook** (`.py` / `.ipynb`) | First-class — primary migration target | Databricks notebook **or** PySpark on EMR/Glue |
| **Table / View / Materialized View** (BQ) | Informational — to show *which notebook produces what* | Not migrated. Tables are referenced by the notebooks; data movement (federation, CTAS, Auto Loader) is OUT of scope. |
| **Scheduled Query** (BQ DTS) | Informational + can be converted to a Databricks notebook | The SQL becomes a notebook + Workflow scheduled task |
| **External GCS table** | Informational | Tagged with the `gs://` path so you know what files need to move (the actual GCS→S3 sync is a separate workstream — Storage Transfer Service / DataSync) |

## Mental model — the medallion chain

For each entity (e.g. `cartoes`), the framework walks the **chain of `.py` notebooks** that produce each layer:

```
raw.cartoes.py    ─► writes prj.raw.cartoes
       │
       ▼
bronze.cartoes.py ─► reads  prj.raw.cartoes
                  ─► writes prj.bronze.cartoes
       │
       ▼
silver.cartoes.py ─► reads  prj.bronze.cartoes
                  ─► writes prj.silver.cartoes
       │
       ▼
gold.cartoes.py   ─► reads  prj.silver.cartoes
                  ─► writes prj.gold.cartoes  ◄── this is what stakeholders see
```

The whole chain (4 notebooks) gets converted and uploaded to the new platform. The tables are just nodes in the lineage graph that show what each notebook reads/writes.

The orchestrator DAG (`cartoes_pipeline`) referencing those notebooks also gets converted (DAG `.py` → Databricks Workflow YAML or MWAA-compatible operators).

## What you can do

| Page | What it shows |
|---|---|
| **Connections** | GCP / Databricks / 4 LLM providers (Anthropic, OpenAI, Gemini, AWS Bedrock — pick one) |
| **Inventory** | **Notebooks** + **DAGs** as primary tabs · **Tables (informational)** as a third tab showing context |
| **Lineage** | Notebooks (📓) and DAGs (▣) as the primary nodes; tables shown as smaller annotations |
| **Plan Waves** | Dependency-ordered batches for the chain |
| **1. Convert** | AI-driven `.py` → `.py` / `.yaml` conversion. Side-by-side Monaco editors (source ↔ converted). Custom prompt slot. Per-conversion target override. |
| **2. Review** | Approve / reject converted artifacts before deploy |
| **3. Deploy** | Upload saved local artifacts to the target (Databricks Workspace via `databricks-sdk`, AWS via `boto3 s3.put_object`). Sample / execute modes. |
| **4. Validate** | Profile source vs deployed target — runs *after* deploy on the tables produced by the migrated notebooks |
| **Docs** | Auto-generate Azure DevOps Wiki Markdown per migrated artifact |

## Setup — 3 commands

### Linux / macOS
```bash
git clone https://github.com/VCasseb/orquestrador_migracao_opensource.git migrate
cd migrate
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
migrate init && migrate web
```

### Windows
```cmd
git clone https://github.com/VCasseb/orquestrador_migracao_opensource.git migrate
cd migrate
python -m venv .venv
.venv\Scripts\activate
pip install -e .
migrate init && migrate web
```

Open `http://127.0.0.1:8000` and start at the **Connections** tab.

## Configure where artifacts live

In `.env`:

```bash
# Where to find the artifacts to migrate (sources in GCP)
GCP_PROJECT_IDS=acme-data-prod,acme-finance-prod,acme-mkt-prod
GCP_COMPOSER_DAG_BUCKET=gs://us-central1-acme-composer-bucket/dags
GCP_NOTEBOOKS_BUCKET=gs://acme-data-notebooks
GCP_REGION=us-east-1            # or southamerica-east1, europe-west2, etc.

# Where converted artifacts should go on the new platform
TARGET_PLATFORM=databricks       # or 'aws'
TARGET_DATABRICKS_WORKSPACE_PREFIX=/Workspace/migration
TARGET_S3_NOTEBOOKS_PREFIX=s3://acme-data-notebooks/migration
TARGET_MWAA_DAGS_PREFIX=s3://acme-mwaa-bucket/dags

# AI engine — pick one (UI: Connections page → AI engine dropdown)
LLM_PROVIDER=anthropic           # anthropic | openai | gemini | bedrock
ANTHROPIC_API_KEY=sk-ant-...
ANTHROPIC_MODEL=claude-sonnet-4-6
```

## Try it without any cloud credentials

```bash
migrate init
migrate inventory --sample      # loads a realistic medallion chain (cartoes pipeline)
migrate web
```

Sample inventory ships with:
- 4 notebooks: `raw.cartoes`, `bronze.cartoes`, `silver.cartoes`, `gold.cartoes` (full medallion chain)
- 1 orchestrator DAG `cartoes_pipeline` running them in sequence
- Plus marketing/ML examples and BQ tables for context

You'll see the Inventory tabs flip to **Notebooks** by default, with the chain laid out in **Lineage**.

## CLI

```bash
migrate init                                        # bootstrap .migrate/ + .env
migrate web                                         # local UI

migrate inventory --sample                          # synthetic medallion chain demo
migrate inventory                                   # scan real GCP — DAGs, notebooks, tables

migrate convert dag <dag_id> --prompt "..."         # AI conversion of a Composer DAG
migrate convert notebook <nb_id> --prompt "..."     # AI conversion of a notebook
migrate convert table <fqn>                         # legacy SQL conversion (informational only now)

migrate plan sprint-1                               # dependency-ordered waves for selected items
migrate deploy sprint-1 --mode sample               # simulated deploy
migrate deploy sprint-1 --mode execute --auto-validate

migrate validate <fqn> --sample                     # synthetic validation
migrate validate <fqn>                              # real BQ vs deployed target diff

migrate approve / reject / history / rollback       # state machine + audit
migrate docs <fqn> | --plan <name>                  # generate Azure DevOps wiki Markdown
migrate status                                      # quick summary
```

## What is NOT in scope

- **Data movement** (BQ → Delta, GCS → S3 bytes). Use Storage Transfer Service / DataSync for GCS→S3; let Databricks Auto Loader / federation handle the load on the target side. The framework documents what needs moving but doesn't move bytes.
- **Networking / IAM / VPC topology**. Use Terraform.
- **BI dashboards** (Looker, Looker Studio). Rebuild on Databricks Dashboards / QuickSight separately.
- **Vertex AI MLOps**. Different project entirely.

## Architecture

```
src/migrate/
├── core/
│   ├── inventory/        # scanners: bq.py, composer.py (DAGs), notebooks.py, scheduled_queries.py
│   ├── lineage/          # sqlglot parser + dependency graph + topological waves
│   ├── convert/          # code.py = LLM-driven (DAGs/notebooks); runner.py + sql.py = legacy SQL
│   ├── deploy/           # code.py = upload artifacts; runner.py = legacy data deploy
│   ├── validate/         # profile + diff + LLM diagnose
│   ├── docs/             # Azure DevOps wiki generator
│   ├── llm/              # 4 providers (anthropic/openai/gemini/bedrock) with dispatcher
│   └── state/            # audit log + approval state machine + rollback
├── cli.py                # typer entrypoint (CI / power users)
└── web/                  # FastAPI + HTMX + Tailwind + Alpine + Monaco (local UI)
```

CLI and Web are thin layers over `core/` — neither holds business logic.

## License

MIT.
