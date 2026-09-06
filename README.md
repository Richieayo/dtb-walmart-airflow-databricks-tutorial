# DBT + Airflow + Databricks Tutorial (Walmart Sales Data)

> **This is a learning/tutorial project.** It demonstrates how to orchestrate a modern
> ELT pipeline with **Apache Airflow**, using a Databricks Job to ingest data from a
> **Postgres** source database into **Databricks**, and then running **dbt** transformations
> (also orchestrated by Airflow) directly against Databricks. The dataset is a mock
> Walmart sales schema (orders, customers, products, stores, employees) used purely to
> illustrate the pipeline pattern — it is not production data or a production pipeline.

## What this project shows

1. **Ingestion (Postgres → Databricks):** An Airflow DAG triggers a Databricks Job (via
   the Databricks SDK) that pulls source data from a Postgres database into a `bronze`
   schema/catalog in Databricks, then polls the job run until it succeeds or fails.
2. **Transformation (dbt on Databricks):** Once ingestion completes, the same DAG runs
   a series of `dbt` commands (`source freshness`, `run`, `test`, `snapshot`) via
   Airflow `BashOperator`/`@task.bash` tasks to build the `silver` and `gold` layers on
   top of the ingested `bronze` data.
3. **Orchestration:** Everything is wired together as a single Airflow DAG
   ([`airflow/dags/orchestrate.py`](airflow/dags/orchestrate.py)) with task dependencies
   enforcing the order: ingest → check source freshness → silver (technical) → silver
   tests → silver (business) → silver tests → gold (ephemeral models) → gold
   (snapshots/dimensions) → gold (facts).

## Architecture

```
Postgres (source)
      │
      ▼
Databricks Job  ◄── triggered & polled by Airflow (Databricks SDK)
      │  (writes to bronze schema)
      ▼
dbt (silver_t → silver_b → gold) ◄── run via Airflow BashOperator against Databricks SQL Warehouse
      │
      ▼
Databricks (silver_t, silver_b, gold schemas)
```

Airflow itself runs locally in Docker (via the standard Airflow `docker-compose`, using
its own internal Postgres for Airflow metadata — not the source data). The `dbt` project
runs from inside the Airflow worker container against a Databricks SQL Warehouse.

## Repository layout

| Path | Purpose |
|---|---|
| [`airflow/`](airflow) | Airflow project: `docker-compose.yaml`, `Dockerfile`, DAGs |
| [`airflow/dags/orchestrate.py`](airflow/dags/orchestrate.py) | The DAG: triggers the Databricks ingestion job, then runs dbt via `BashOperator` |
| [`walmart_project/`](walmart_project) | The dbt project (models, snapshots, tests, macros) — volume-mounted into the Airflow containers at `/opt/airflow/walmart_project` (see `docker-compose.yaml`) |
| `walmart_project/models/source` | dbt source definitions pointing at the Databricks `bronze` schema |
| `walmart_project/models/silver_t` | "Technical" silver layer — cleaned, typed staging models |
| `walmart_project/models/silver_b` | "Business" silver layer — a wide/one-big-table model built on top of silver_t |
| `walmart_project/models/gold` | Gold layer — ephemeral helper models, fact table(s) |
| `walmart_project/snapshots` | Type-2 dimension snapshots (customers, employees, orders, products, stores) |
| `main.py` | Placeholder entry point for the Python project (not part of the pipeline) |

## Pipeline (DAG) steps

The `orchestrate` DAG runs the following tasks in order:

1. `ingest_cdc` — triggers a Databricks Job (by `JOB_ID`) that ingests data from Postgres
   into Databricks bronze tables, and polls the run until it terminates successfully.
2. `source_freshness` — runs `dbt source freshness` against the bronze sources.
3. `silver_technical` / `silver_technical_tests` — `dbt run`/`dbt test --select silver_t`.
4. `silver_business` / `silver_business_tests` — `dbt run`/`dbt test --select silver_b`.
5. `gold_ephemeral` — `dbt run --select gold/ephemeral`.
6. `gold_dimensions` — `dbt snapshot` (Type-2 dimension tables).
7. `gold_facts` — `dbt run --select gold/fact`.

## Prerequisites

- Docker and Docker Compose (to run Airflow locally)
- A Databricks workspace with:
  - A SQL Warehouse (for dbt to connect to)
  - A Job configured to perform the Postgres → Databricks ingestion, and its `JOB_ID`
  - A personal access token with permission to run jobs and query the warehouse
- A source Postgres database reachable by the Databricks ingestion job

## Configuration

Create `airflow/.env` (git-ignored, not committed) with the following variables:

```
AIRFLOW_UID=<your uid, e.g. 50000>
JOB_ID=<Databricks job id for the Postgres ingestion job>
DTK_HOST=<Databricks workspace host, used to trigger/poll the job>
DTK_TOKEN=<Databricks personal access token>
DBT_HOST=<Databricks SQL Warehouse host, used by dbt-databricks>
DBT_TOKEN=<Databricks personal access token for dbt>
DBT_HTTP_PATH=<Databricks SQL Warehouse HTTP path>
```

`.env` files are git-ignored — never commit real tokens.

## Running it locally

```bash
cd airflow
docker compose up airflow-init
docker compose up
```

Then open the Airflow UI at `http://localhost:8080` (default credentials `airflow`/`airflow`),
un-pause the `orchestrate` DAG, and trigger a run.

## Running dbt on its own

If you just want to iterate on the dbt project without Airflow:

```bash
cd walmart_project
dbt deps
dbt run
dbt test
```

`profiles.yml` is git-ignored (standard dbt practice, since it's environment-specific),
so it won't come down when you clone this repo — recreate it locally as:

```yaml
walmart_project:
  outputs:
    dev:
      catalog: walmart
      host: "{{ env_var('DBT_HOST') }}"
      http_path: "{{ env_var('DBT_HTTP_PATH') }}"
      schema: dbt_schema
      threads: 1
      token: "{{ env_var('DBT_TOKEN') }}"
      type: databricks
  target: dev
```

with `DBT_HOST`, `DBT_TOKEN`, and `DBT_HTTP_PATH` exported in your shell (same values as
in `airflow/.env`).
