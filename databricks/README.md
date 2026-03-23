# Databricks Notebooks (GCP)

Esta carpeta contiene los notebooks (exportados a `.py`) que ejecutan el pipeline end-to-end en Databricks sobre GCP:

**01 → 02 → 03 → 04 → 05**

> Nota: Estos archivos están pensados para ejecutarse como **Databricks Jobs**, usando **widgets** como parámetros.

---

## Flujo del pipeline

### 01 — `01_bronze_ingestion.py`
**Objetivo:** ingestión de archivos CSV (Letterboxd) desde `landing/` hacia **Bronze** en GCS (Parquet particionado por `ingestion_date`).

**Inputs:**
- Archivos `.csv` en `gs://<bucket>/landing/letterboxd/`

**Outputs:**
- `gs://<bucket>/bronze/letterboxd/ingestion_date=YYYY-MM-DD/<table_name>/` (Parquet)

---

### 02 — `02_silver_transformation.py`
**Objetivo:** limpiar/normalizar datos, enriquecer catálogo de películas con **TMDB API**, y escribir tablas **Silver** en Delta Lake sobre GCS.

**Inputs:**
- Bronze (Parquet) en GCS

**Outputs:**
- `gs://<bucket>/silver/movies` (Delta)
- `gs://<bucket>/silver/diary_entries` (Delta)
- `gs://<bucket>/silver/ratings` (Delta)
- `gs://<bucket>/silver/_audit_log` (Delta)

---

### 03 — `03_staging_to_bq.py`
**Objetivo:** exportar las 3 tablas Silver (Delta) a BigQuery como tablas `src_silver_*`.

**Outputs en BigQuery:**
- `<project>.<dataset>.src_silver_movies`
- `<project>.<dataset>.src_silver_diary_entries`
- `<project>.<dataset>.src_silver_ratings`

Estas tablas son las fuentes que consume el proyecto dbt (`cine_analytics/`).

---

### 04 — `04_dbt_run.py`
**Objetivo:** ejecutar dbt (deps/snapshot/build/docs, según configuración) para construir el modelo dimensional en BigQuery:
- `stg_*` (views)
- `dim_*`, `fact_*`, `bridge_*` (tablas)
- `snap_*` (snapshots)

> Este notebook debe estar parametrizado (sin rutas personales hardcodeadas) y alineado con `cine_analytics/README.md`.

#### Parámetros (widgets) — 04_dbt_run.py

Este notebook **genera automáticamente** `~/.dbt/profiles.yml` en el cluster usando el método `service-account` (keyfile JSON en Workspace).

Widgets requeridos:

- `dbt_project_dir`: ruta a la carpeta dbt dentro del repo (ej: `/Workspace/Repos/<tu_usuario>/etl-letterboxd/cine_analytics`)
- `gcp_project_id`: GCP Project ID real
- `bq_dataset`: dataset donde viven **inputs** (`src_silver_*`) y **outputs** (tablas dbt)
- `bq_location`: ubicación de BigQuery (ej: `US`)
- `json_key_path`: ruta del keyfile JSON dentro del Workspace (ej: `/Workspace/Users/<tu_usuario>/secrets/gcp-sa-key.json`)

Comandos ejecutados:
- `dbt deps`
- `dbt snapshot`
- `dbt build`

---

### 05 — `05_dataplex_scan.py`
**Objetivo:** disparar scans de **Dataplex Data Quality** sobre tablas Gold (ej: facts/dims) vía API.

**Parámetros:**
- `project_id`
- `location` (ej: `us-central1`)
- `scan_ids` (comma-separated)

---

## Parámetros (widgets) recomendados

En general, los notebooks usan (o deberían usar) estos parámetros:

- `project_id` / `gcp_project_id`
- `bucket_name` / `gcs_bucket`
- `bq_dataset`
- `ingestion_date` (opcional)
- `run_mode` (`incremental` / `full_refresh`)
- `tmdb_concurrency` (solo en Silver)
- `secret_scope` (si usas Databricks Secrets)
- `location` y `scan_ids` (Dataplex)

---

## Secrets

Para producción (recomendado):
- Guardar `tmdb-api-key` en un Secret Scope de Databricks (o usar fallback a GCP Secret Manager via ADC).

Nunca commitear llaves o JSON de service accounts en este repo.