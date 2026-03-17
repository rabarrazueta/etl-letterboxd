# cine_analytics (dbt)

Proyecto **dbt Core** que construye la **Gold Layer (Star Schema)** en **BigQuery** a partir de tablas Silver exportadas desde Databricks.

Este proyecto vive dentro del repo `etl-letterboxd` en la carpeta `cine_analytics/`.

---

## Prerrequisitos (inputs)

Antes de correr dbt, deben existir en BigQuery las siguientes tablas fuente (cargadas/actualizadas por Databricks):

- `src_silver_movies`
- `src_silver_diary_entries`
- `src_silver_ratings`

> Estas tablas son generadas por el notebook `databricks/03_staging_to_bq.py`.

---

## Configuración (profiles.yml)

El profile debe llamarse **`cine_analytics`** (ver `dbt_project.yml`).

Crea `~/.dbt/profiles.yml` (o define `DBT_PROFILES_DIR`) con una configuración como esta:

```yaml
cine_analytics:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: YOUR_GCP_PROJECT_ID
      dataset: portfolio_gold
      location: US
      threads: 4
      keyfile: /path/to/key.json
      timeout_seconds: 300
```

> Este proyecto asume que **inputs (`src_*`) y outputs (modelos dbt) viven en el mismo dataset** (`dataset:` del profile).

---

## Ejecución (comandos recomendados)

Desde esta carpeta (`cine_analytics/`):

```bash
dbt deps
dbt debug
```

### 1) Snapshots (SCD Tipo 2)
```bash
dbt snapshot
```

### 2) Modelos + tests
```bash
dbt build
```

### Documentación
```bash
dbt docs generate
dbt docs serve
```

---

## Modelos (overview)

### Fuentes (`models/staging/_sources.yml`)
- `silver_staging.src_silver_movies`
- `silver_staging.src_silver_diary_entries`
- `silver_staging.src_silver_ratings`

### Staging models (`models/staging`) — views `stg_*`
- `stg_silver_movies`
- `stg_silver_diary_entries`
- `stg_silver_ratings`

### Dimensiones (`models/dimensions`)
- `dim_movie` (tabla con **versión actual** del SCD2; el historial vive en `snap_dim_movie`)
- `dim_date`
- `dim_genre`
- `dim_director`

### Bridge (`models/bridges`)
- `bridge_movie_genre` (relación N:M película↔género)

### Facts (`models/facts`) — incrementales
- `fact_diary_entries`
- `fact_ratings`

### Snapshots (`snapshots/`)
- `snap_dim_movie` (SCD Tipo 2, estrategia `check`)

---

## Tests

Los tests están declarados junto a cada capa:

- `models/staging/_staging.yml`
- `models/dimensions/_dimensions.yml`
- `models/facts/_facts.yml`
- `models/bridges/_bridges.yml`

Incluyen `not_null`, `unique`, `relationships` y validaciones de rango (ratings).

---

## Portabilidad (BigQuery ↔ Postgres)

Las macros en `macros/portability.sql` encapsulan diferencias de sintaxis entre BigQuery y Postgres.
El target principal actual es **BigQuery**.