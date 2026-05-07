from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

ObjectType = Literal["TABLE", "VIEW", "MATERIALIZED_VIEW", "EXTERNAL"]
Complexity = Literal["low", "medium", "high"]
SourceKind = Literal[
    "view",                 # BQ view — view_query is the source
    "scheduled_query",      # BQ Data Transfer Service scheduled query
    "dag_sql",              # Composer DAG with BigQueryInsertJobOperator (SQL inline)
    "dag_python",           # Composer DAG with PythonOperator transformation
    "dag_notebook",         # Composer DAG that executes a notebook
    "notebook",             # Vertex Workbench / Colab / GCS .ipynb (no DAG)
    "external_gcs",         # BQ EXTERNAL table — data lives in GCS, not in BQ storage
    "manual",               # Loaded by humans / external pipeline
    "unknown",
]


class ColumnMetadata(BaseModel):
    name: str
    type: str
    nullable: bool = True
    description: str = ""


class TableMetadata(BaseModel):
    project: str
    dataset: str
    name: str
    type: ObjectType
    columns: list[ColumnMetadata] = Field(default_factory=list)
    row_count: int | None = None
    size_bytes: int | None = None
    partitioning: str | None = None
    clustering: list[str] = Field(default_factory=list)
    view_query: str | None = None
    description: str = ""
    labels: dict[str, str] = Field(default_factory=dict)
    created: datetime | None = None
    modified: datetime | None = None
    query_count_30d: int = 0

    # Provenance — what produces this table?
    source_kind: SourceKind = "unknown"
    source_sql: str | None = None              # SQL extracted from DAG/scheduled_query (used when view_query is None)
    source_schedule: str | None = None         # cron / "@daily" / etc — becomes Workflow schedule
    source_dag_id: str | None = None           # DAG name (links to DAGMetadata)
    source_dag_task: str | None = None         # task within the DAG
    source_notebook_id: str | None = None      # notebook name (links to NotebookMetadata)
    source_scheduled_query_id: str | None = None  # BQ DTS config id

    # External table info (when type == EXTERNAL — data lives in GCS, not BQ storage)
    external_source_uris: list[str] = Field(default_factory=list)  # ["gs://bucket/path/*.parquet"]
    external_format: str | None = None          # PARQUET / AVRO / CSV / JSON / ORC / ICEBERG
    external_hive_partitioning: bool = False    # detected gs://.../year=/month=/day=/ layout
    external_compression: str | None = None     # GZIP / SNAPPY / etc

    @property
    def fqn(self) -> str:
        return f"{self.project}.{self.dataset}.{self.name}"

    @property
    def complexity(self) -> Complexity:
        if self.type == "VIEW" and self.view_query:
            sql_len = len(self.view_query)
            if sql_len > 4000:
                return "high"
            if sql_len > 1500:
                return "medium"
        if self.size_bytes and self.size_bytes > 100 * 1024**3:
            return "high"
        if self.size_bytes and self.size_bytes > 10 * 1024**3:
            return "medium"
        return "low"

    @property
    def heat(self) -> Literal["hot", "warm", "cold"]:
        if self.query_count_30d > 500:
            return "hot"
        if self.query_count_30d > 50:
            return "warm"
        return "cold"


class DAGTask(BaseModel):
    task_id: str
    operator: str                     # ex: "BigQueryInsertJobOperator", "VertexAINotebookExecuteOperator"
    sql: str | None = None            # extracted SQL if any
    destination_table: str | None = None  # FQN written by this task, if any
    referenced_tables: list[str] = Field(default_factory=list)  # FQNs read
    notebook_uri: str | None = None   # if task executes a notebook
    parameters: dict = Field(default_factory=dict)


class DAGMetadata(BaseModel):
    name: str
    file_path: str                    # gs://<bucket>/dags/<file>.py
    schedule: str | None = None
    owner: str | None = None
    tags: list[str] = Field(default_factory=list)
    tasks: list[DAGTask] = Field(default_factory=list)
    description: str = ""
    source_code: str | None = None    # raw .py content (full file)

    @property
    def fqn(self) -> str:
        return f"composer.{self.name}"

    @property
    def writes_tables(self) -> list[str]:
        return [t.destination_table for t in self.tasks if t.destination_table]

    @property
    def reads_tables(self) -> list[str]:
        out: list[str] = []
        for t in self.tasks:
            out.extend(t.referenced_tables)
        return list(dict.fromkeys(out))

    @property
    def referenced_notebooks(self) -> list[str]:
        return [t.notebook_uri for t in self.tasks if t.notebook_uri]


class NotebookCell(BaseModel):
    cell_type: Literal["code", "markdown", "raw"] = "code"
    source: str = ""
    has_sql: bool = False
    sql_extracted: str | None = None
    referenced_tables: list[str] = Field(default_factory=list)
    writes_tables: list[str] = Field(default_factory=list)


class NotebookMetadata(BaseModel):
    name: str
    location: str                     # gs://<bucket>/<path>.ipynb or workbench instance path
    kind: Literal["vertex_workbench", "colab", "gcs_ipynb", "dataproc"] = "gcs_ipynb"
    cells: list[NotebookCell] = Field(default_factory=list)
    libraries: list[str] = Field(default_factory=list)   # detected imports of interest
    description: str = ""

    @property
    def fqn(self) -> str:
        return f"notebook.{self.name}"

    @property
    def all_referenced_tables(self) -> list[str]:
        out: list[str] = []
        for c in self.cells:
            out.extend(c.referenced_tables)
        return list(dict.fromkeys(out))

    @property
    def all_written_tables(self) -> list[str]:
        out: list[str] = []
        for c in self.cells:
            out.extend(c.writes_tables)
        return list(dict.fromkeys(out))


class ScheduledQuery(BaseModel):
    name: str                         # display name from DTS
    config_id: str                    # transferConfigs/...
    schedule: str                     # cron-like
    query: str
    destination_table: str            # FQN written
    project: str

    @property
    def fqn(self) -> str:
        return f"sq.{self.name}"


class Inventory(BaseModel):
    scanned_at: datetime
    projects: list[str]
    tables: list[TableMetadata]
    dags: list[DAGMetadata] = Field(default_factory=list)
    notebooks: list[NotebookMetadata] = Field(default_factory=list)
    scheduled_queries: list[ScheduledQuery] = Field(default_factory=list)

    @property
    def by_fqn(self) -> dict[str, TableMetadata]:
        return {t.fqn: t for t in self.tables}

    @property
    def dags_by_id(self) -> dict[str, DAGMetadata]:
        return {d.name: d for d in self.dags}

    @property
    def notebooks_by_id(self) -> dict[str, NotebookMetadata]:
        return {n.name: n for n in self.notebooks}

    def filter(
        self,
        *,
        project: str | None = None,
        dataset: str | None = None,
        type: ObjectType | None = None,
        complexity: Complexity | None = None,
        heat: str | None = None,
        source_kind: str | None = None,
        search: str | None = None,
    ) -> list[TableMetadata]:
        out = self.tables
        if project:
            out = [t for t in out if t.project == project]
        if dataset:
            out = [t for t in out if t.dataset == dataset]
        if type:
            out = [t for t in out if t.type == type]
        if complexity:
            out = [t for t in out if t.complexity == complexity]
        if heat:
            out = [t for t in out if t.heat == heat]
        if source_kind:
            out = [t for t in out if t.source_kind == source_kind]
        if search:
            s = search.lower()
            out = [t for t in out if s in t.fqn.lower()]
        return out
