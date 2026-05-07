# migrate

AI-assisted framework for migrating data projects from **GCP (BigQuery)** to **Databricks/AWS**, with human-in-the-loop approval and auditable artifacts.

Built for real migration teams, not slideware. Scoped explicitly:

**In scope:** BQ tables / views / scheduled queries → Delta + Unity Catalog. Lineage discovery, SQL transpilation, schema/data load, notebook generation, semantic validation, wave-based orchestration.

**Out of scope:** Dataflow / Beam pipelines, Pub/Sub topology, Vertex AI / ML, complex Composer DAG logic, network/IAM topology, BI dashboards.

## Setup

### Option A — with `uv` (recommended)

```bash
# install uv if needed: curl -LsSf https://astral.sh/uv/install.sh | sh
git clone <repo-url>
cd migrate
uv sync
uv run migrate init        # creates .migrate/config.yaml + .env
uv run migrate web         # opens http://127.0.0.1:8000
```

### Option B — with `pip` + venv

```bash
git clone <repo-url>
cd migrate
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
migrate init               # creates .migrate/config.yaml + .env
migrate web                # opens http://127.0.0.1:8000
```

After `migrate web` is running, open the **Connections** page in the browser to configure credentials (or edit `.env` directly).

## What you can do — v0.3

| Page | Status | Function |
|---|---|---|
| Connections | ✅ | Connect to GCP, Databricks, Anthropic with test buttons |
| Inventory | ✅ | Scan GCP projects → tables, views; filter by project/type/complexity/heat; select for migration |
| Lineage | ✅ | Cross-project dependency graph (cytoscape.js) with click-to-highlight upstream/downstream |
| Plan Waves | ✅ | Topologically-ordered migration waves; save as `.migrate/plans/<name>.yaml` |
| Convert | ✅ | sqlglot + Claude LLM fallback + Delta DDL + standardized notebook + **side-by-side Monaco diff** |
| Validate | ✅ | Profile BQ vs Delta (row count, NULL drift, distinct, avg); synthetic mode for demos |
| Review | ✅ | Approval queue: per-item approve/reject + bulk auto-approve filters |
| Deploy | ✅ | Sample / dry-run / execute modes; wave-orchestrated; concurrent; resumable; idempotent |
| Docs | ✅ | Generate Azure DevOps Wiki Markdown per table or per plan (mermaid, schema diff, audit trail) |

Cross-cutting:
- **Audit log** — every action recorded in `.migrate/runs/*.jsonl` (per-FQN + global)
- **Approval lifecycle** — `pending → approved | rejected → revoked` for conversion / validation / deployment
- **Rollback** — undo conversion / validation / approval / deployment (deployment supports `--execute` for real DROP TABLE)

## CLI commands

```bash
migrate init                                # bootstrap .migrate/ + .env
migrate web                                 # launch local UI

migrate inventory --sample                  # load 14-table synthetic demo
migrate inventory                           # scan real GCP_PROJECT_IDS

migrate plan sprint-1                       # build wave plan from selection
migrate convert prj.ds.table                # sqlglot + LLM + DDL + notebook
migrate validate prj.ds.table --sample      # synthetic validation
migrate validate prj.ds.table               # real BQ vs Delta profiling

migrate approve  prj.ds.table conversion -m "LGTM"
migrate reject   prj.ds.table validation -m "row count off by 12%"

migrate deploy sprint-1 --mode sample       # simulated wave deploy
migrate deploy sprint-1 --mode dry-run      # plan only
migrate deploy sprint-1 --mode execute --concurrency 8
migrate deploy sprint-1 --mode execute --create-workflow

migrate docs prj.ds.table                   # 1 markdown for Azure DevOps Wiki
migrate docs --plan sprint-1                # all tables in plan + index

migrate history prj.ds.table                # audit trail for one object
migrate history -n 50                       # global recent log

migrate rollback conversion prj.ds.table
migrate rollback validation prj.ds.table
migrate rollback approval   prj.ds.table --stage conversion
migrate rollback deployment prj.ds.table              # dry-run plan
migrate rollback deployment prj.ds.table --execute    # actually DROP + remove notebook

migrate status                              # quick summary
```

## Try it without any cloud credentials

```bash
migrate init
migrate inventory --sample            # 14 fake tables across 3 projects with realistic deps
migrate web                           # browse Inventory, Lineage, Plan, Convert, Validate
```

In the web UI: pick tables in Inventory → see them in Lineage → build waves in Plan → convert in Convert → run a synthetic validation.

## Architecture

```
src/migrate/
├── core/           # business logic — no UI dependencies
├── cli.py          # typer entrypoint (CI / power users)
└── web/            # FastAPI + Jinja + HTMX + Tailwind (local UI)
```

All commands work both in CLI and web — both are thin layers over `core/`.

## Credentials

Credentials live in `.env` (gitignored). The Connections page reads/writes this file. Never put credentials in `.migrate/config.yaml` (which IS git-tracked).

| Service | Auth method |
|---|---|
| GCP | Application Default Credentials (`gcloud auth application-default login`) **or** Service Account JSON |
| Databricks | Personal Access Token + workspace URL |
| Anthropic | API key (only if you want LLM fallback for conversion) |
| AWS | Not needed — handled by Unity Catalog External Locations |
