# 🎬 Cine Analytics — End-to-End Data Engineering Portfolio

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.9-FF694B?style=flat&logo=dbt&logoColor=white)](https://getdbt.com)
[![BigQuery](https://img.shields.io/badge/BigQuery-GCP-4285F4?style=flat&logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![Databricks](https://img.shields.io/badge/Databricks-GCP-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-2.x-003366?style=flat&logo=delta&logoColor=white)](https://delta.io)
[![Terraform](https://img.shields.io/badge/Terraform-1.x-7B42BC?style=flat&logo=terraform&logoColor=white)](https://terraform.io)
[![Dataplex](https://img.shields.io/badge/Dataplex-Data_Governance-4285F4?style=flat&logo=google-cloud&logoColor=white)](https://cloud.google.com/dataplex)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)](https://docker.com)
[![n8n](https://img.shields.io/badge/n8n-Orchestration-EA4B71?style=flat&logo=n8n&logoColor=white)](https://n8n.io)
[![Looker Studio](https://img.shields.io/badge/Looker_Studio-Dashboard-4285F4?style=flat&logo=looker&logoColor=white)](https://lookerstudio.google.com)

> **Este es un pipeline de datos enterprise-grade que analiza hábitos cinematográficos personales** — desde CSVs crudos de Letterboxd hasta un dashboard interactivo con gobernanza de datos activa, desplegado en Google Cloud Platform con arquitectura Medallion y Star Schema.

🔗 **[Ver Dashboard en Vivo →](https://lookerstudio.google.com/reporting/a0360354-2529-4b3c-ba17-3939eef22704)** &nbsp;|&nbsp; 📊 **[Explorar BigQuery Gold Layer →](#)** &nbsp;|&nbsp; 📝 **[Arquitectura Completa →](#arquitectura)**

---

## ¿Por qué este proyecto es diferente?

La mayoría de proyectos de portafolio de Data Engineering mueven datos de A a B.
Este proyecto construye un **producto de datos observable y certificado**:

| Qué hace la mayoría | Qué hace este proyecto |
|---|---|
| Pipeline simple sin gobernanza | **Data Quality automatizada con Dataplex** (contratos en YAML) |
| Una sola arquitectura | **Doble arquitectura desacoplada** (GCP Native + VPS local - en desarrollo) |
| Parquet plano | **Delta Lake con Upserts ACID** y SCD Tipo 1/2 |
| SQL sin lineaje | **Data Lineage automático** de GCS → Databricks → BigQuery → Looker Studio |
| Dashboard de prueba | **Panel de calidad de datos** conectado a resultados de DQ reales |
| Credenciales hardcodeadas | **Secret Manager + Service Accounts** con principio de mínimo privilegio |
| Infraestructura manual | **Terraform end-to-end** (GCS, BigQuery, IAM, Secret Manager, Databricks) |

---

## Arquitectura

### Visión General — Doble Arquitectura Desacoplada

El código de transformación es **agnóstico a la infraestructura** mediante una capa de abstracción con `fsspec`. Cambiar de GCP a un VPS de $15/mes requiere modificar **únicamente el `.env`**.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         FASE 1 — GCP Native                         │
│                                                                     │
│  Letterboxd CSV ──► GCS Bronze ──► Databricks/PySpark ──► GCS Silver│
│       +                                  │                          │
│  TMDB API ───────────────────────────────┘                          │
│                                          │                          │
│                               Delta Lake (ACID Upserts)             │
│                                          │                          │
│                              dbt-bigquery │                         │
│                                          ▼                          │
│                              BigQuery Gold (Star Schema)            │
│                                    │           │                    │
│                             Dataplex DQ    Looker Studio            │
│                             (YAML Rules)   (Dashboard)              │
│                                    │                                │
│                    Cloud Logging → Pub/Sub → n8n → Telegram         │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│        FASE 2 — DigitalOcean VPS - en desarrollo ($15/mes)          │
│                                                                     │
│  Letterboxd CSV ──► /data/bronze ──► DuckDB (Python puro) ──► /data/silver │
│       +                  │                                          │
│  TMDB API ───────────────┘           dbt-postgres                   │
│                                          │                          │
│                               PostgreSQL Gold (Star Schema)         │
│                                          │                          │
│                              n8n (Orquestador principal)            │
│                           docker-compose (RAM < 1.5 GB)             │
└─────────────────────────────────────────────────────────────────────┘
```

### Arquitectura Medallion

```
Bronze (Raw)          Silver (Enriched)         Gold (Star Schema)
─────────────         ─────────────────         ──────────────────
CSV Letterboxd   →    Delta Lake / Parquet   →   fact_diary_entries
diary.csv             -  Limpieza de nulos        fact_ratings
ratings.csv           -  Normalización tipos      dim_movie (SCD Tipo 2)
watched.csv           -  Enriquecimiento TMDB     dim_date
                      -  Rate limiting 40r/10s    dim_genre (bridge)
                      -  Upserts por tmdb_id      dim_director
```

---

## 🛠️ Stack Tecnológico

### Fase 1 — GCP Native
| Componente | Tecnología | Justificación |
|---|---|---|
| **Ingesta & Almacenamiento** | Google Cloud Storage | Object Versioning en `/bronze/` — fuente de verdad inmutable |
| **Procesamiento Bronze→Silver** | Databricks on GCP + PySpark | Enterprise-grade, Delta Lake, Upserts ACID |
| **Formato de datos** | Delta Lake | ACID transactions sin lógica manual de merge |
| **Data Warehouse** | BigQuery | Serverless, free tier permanente, conector nativo Looker Studio |
| **Transformación Silver → Gold** | dbt Core (`dbt-bigquery`) | SQL versionado, tests, documentación, SCD snapshots |
| **Gobernanza** | Google Cloud Dataplex | DQ como código (YAML), lineaje automático, business glossary |
| **Seguridad** | Secret Manager + IAM | Zero credenciales hardcodeadas, principio de mínimo privilegio |
| **Orquestación pipeline** | Databricks Workflows + Cloud Scheduler | DAG nativo, Jobs Compute (~$0.34/hr) separado de All-Purpose |
| **Alertas operacionales** | n8n + Pub/Sub + Cloud Logging | Log Sink → Pub/Sub → webhook → Telegram/email |
| **Infraestructura como Código** | Terraform (GCP + Databricks providers) | GCS, BigQuery, IAM, Secret Manager, Databricks workspace |
| **Dashboard** | Looker Studio | URL pública compartible, conector nativo BigQuery |

### Fase 2 — DigitalOcean (Producción Personal) - en Proceso
| Componente | Tecnología |
|---|---|
| **Procesamiento** | DuckDB + Polars (Python puro, sin JVM) |
| **Data Warehouse** | PostgreSQL 16 contenerizado (RAM-optimizado) |
| **Transformación** | dbt Core (`dbt-postgres`) |
| **Orquestación** | n8n como orquestador principal |
| **Despliegue** | Docker Compose (<1.5 GB RAM total) |

---

## 📊 Dashboard en Vivo

[**→ Abrir Dashboard en Looker Studio**](https://lookerstudio.google.com/reporting/a0360354-2529-4b3c-ba17-3939eef22704)

El dashboard tiene **6 secciones estratégicas**:

1. **KPIs Globales** — Total películas, rating promedio personal, directores únicos, tasa de enriquecimiento TMDB
2. **Línea de Tiempo** — Películas vistas por mes/año con granularidad configurable
3. **Distribución de Ratings** — Rating personal vs `tmdb_vote_average` (¿soy más crítico que el promedio global?)
4. **Mapa de Géneros** — Treemap con porcentaje de tiempo por género (arrays BigQuery → unnest en Looker)
5. **Top Directores & Películas** — Tablas interactivas con filtros por año y género
6. **🏥 Panel de Calidad de Datos** *(diferenciador enterprise)* — Score de COMPLETENESS, VALIDITY, UNIQUENESS y CONFORMANCE por tabla Gold, con evolución temporal y formato condicional (Verde = 100%, Rojo < 100%)

> **Nota técnica:** El fan-out effect (producto cartesiano en relaciones N:M de géneros) fue resuelto mediante Data Blending de solo dos tablas (`fact_diary_entries` + `dim_movie`) aprovechando el tipo Array nativo de BigQuery, con `COUNT DISTINCT` sobre `movie_id` para garantizar métricas sin inflación.

---

## Modelo de Datos — Star Schema

```sql
-- Tablas Fact (carga incremental, NO full refresh)
fact_diary_entries    -- Cada entrada del diario Letterboxd
fact_ratings          -- Ratings históricos

-- Dimensiones
dim_movie             -- Atributos TMDB (SCD Tipo 2 — historial de cambios)
dim_date              -- Dimensión fecha generada
dim_genre             -- Géneros cinematográficos
bridge_movie_genre    -- Bridge table para relación N:M película↔género
dim_director          -- Directores

-- Metadata de linaje en todas las tablas
_dbt_updated_at, _dbt_created_at
```

### Estrategias de Carga por Tipo de Tabla
- **Fact tables:** `strategy: incremental` con `unique_key` — zero full refresh
- **`dim_movie`:** `dbt snapshot` SCD Tipo 2 — preserva historial de cambios editoriales de TMDB
- **`dim_*` estáticas:** SCD Tipo 1 (sobrescribe sin historial)

---

## Gobernanza de Datos — Dataplex

Las reglas de calidad son **código versionado en Git**, no configuración manual en una UI:

```yaml
# dataplex/dq_rules/fact_diary_entries.yaml
rules:
  - column: letterboxd_uri
    rule_type: REGEX
    # Letterboxd exporta URLs acortadas boxd.it, no URLs canónicas
    # Descubierto durante auditoría de datos Bronze — contrato ajustado
    regex: "^https://boxd\\.it/[a-zA-Z0-9]+$"
    dimension: CONFORMANCE

  - column: watched_date
    rule_type: CUSTOM_SQL_EXPR
    custom_sql_expr: "watched_date <= CURRENT_DATE()"
    dimension: VALIDITY

  - column: my_rating
    rule_type: RANGE
    range: { min_value: 0.5, max_value: 5.0 }
    ignore_null: true
    dimension: VALIDITY
```

### Incidentes Resueltos Durante el Despliegue

| Incidente | Síntoma | Causa Raíz | Solución |
|---|---|---|---|
| **Conformidad 0%** en `fact_diary_entries` | 100% errores en regex de URL | Letterboxd exporta `boxd.it` (acortador), no la URL canónica | Ajuste del contrato de datos tras auditoría de Bronze |
| **Unicidad 43% errores** en `dim_movie` | `tmdb_id` duplicado | `dim_movie` es SCD Tipo 2 — múltiples filas por diseño | Regla redirigida a la llave subrogada (`movie_id`) generada por dbt |

**Estado final:** ✅ 100% de registros aprobados en UNIQUENESS, VALIDITY, COMPLETENESS, CONSISTENCY y CONFORMANCE.

### Linaje Automático

```
GCS Bronze (CSV) → Databricks Job → GCS Silver (Delta Lake) → dbt model → BigQuery Gold → Looker Studio
```

---

## Desacoplamiento `fsspec` — El Núcleo Arquitectónico

El principal cambio entre Fase 1 (GCP) y Fase 2 (VPS local) es el archivo `.env`:

```bash
# Fase 1 — GCP
STORAGE_BACKEND=gcs
GCP_PROJECT_ID=mi-proyecto
GCS_BUCKET=mi-proyecto

# Fase 2 — DigitalOcean
STORAGE_BACKEND=local
LOCAL_DATA_PATH=/data
```

```python
# ingestion/storage/backend.py — NINGÚN otro módulo importa google.cloud.storage
class StorageBackend:
    def __init__(self):
        backend = os.getenv("STORAGE_BACKEND", "local")
        if backend == "gcs":
            self.fs = fsspec.filesystem("gcs", project=os.environ["GCP_PROJECT_ID"])
            self._base = f"gs://{os.environ['GCS_BUCKET']}-datalake"
        elif backend == "local":
            self.fs = fsspec.filesystem("file")
            self._base = os.getenv("LOCAL_DATA_PATH", "/data")
```

---

## 📁 Estructura del Repositorio

```
etl-letterboxd/
│
├── terraform/                    # IaC — GCS, BigQuery, IAM, Secret Manager, Databricks
│   ├── main.tf
│   ├── variables.tf
│   └── .terraform.lock.hcl
│   └── providers.tf
│   └── terraform.tfvars.example
│   └── iam.tf
│   └── backend.tf
│
├── databricks/                   # Notebooks PySpark por capa
│   └── 01_bronze_ingestion.py
│   └── 02_silver_transformation.py
│   └── 03_staging_to_bq.py
│   └── 04_dbt_run.py
│   └── 05_dataplex_scan.py
│
├── ingestion/                    # Módulos Python puros — portables entre fases
│   ├── storage/
│   │   └── backend.py            # Abstracción fsspec (GCS ↔ local)
│   ├── api/
│   │   ├── tmdb_client.py        # Rate limiting 40r/10s + backoff exponencial
│   │   └── letterboxd_parser.py
│   └── pipeline/
│       ├── silver_processor.py   # DuckDB (Fase 2) / PySpark (Fase 1)
│       └── cli.py                # Entrypoint para n8n
│
│
├── dataplex/
│   └── dq_rules/                 # Contratos de calidad como código (YAML)
│       ├── fact_diary_entries.yaml
│       ├── fact_ratings.yaml
│       └── dim_movie.yaml
│
├── docker/                       # Fase 2 — VPS DigitalOcean
│   ├── docker-compose.yml        # RAM total < 1.5 GB
│   ├── postgres/
│   │   ├── postgresql.conf       # shared_buffers=128MB, max_connections=20
│   │   └── init.sql
│   └── pipeline/
│       ├── Dockerfile
│       └── requirements.txt
│
├── tests/                        # pytest — ingesta y transformación
│   ├── test_tmdb_client.py       # Primera unidad testeada (rate limit crítico)
│   ├── test_silver_processor.py
│   └── test_storage_backend.py
│
├── .env.example                  # Variables documentadas — .env NUNCA se commitea
├── .gitignore
└── README.md
```

---

## Quick Start

### Prerrequisitos
- Cuenta GCP con créditos activos
- Terraform >= 1.5
- Docker & Docker Compose
- Cuenta TMDB API (gratuita)
- Export CSV de Letterboxd (`Settings → Import & Export → Export Your Data`)

### Fase 1 — Despliegue GCP

```bash
# 1. Clonar y configurar variables
git clone https://github.com/rabarrazueta/etl-letterboxd.git
cd etl-letterboxd
> Importante: **Databricks Jobs NO consumen `.env`**.  
> En **Fase 1**, la configuración se hace con:
> - Variables de Terraform (`terraform.tfvars` / `terraform.tfvars.example`)
> - Secrets (Databricks Secret Scope o GCP Secret Manager vía ADC)
> - Widgets de Databricks Jobs

# 2. Configurar alertas de presupuesto ANTES de desplegar
# GCP Billing Console → Budgets & Alerts → 25%, 50%, 75%, 90%

# 3. Provisionar infraestructura
cd terraform
terraform init
terraform plan
terraform apply

# 4. Subir CSVs de Letterboxd a Bronze
gsutil cp *.csv gs://<proyecto>-datalake/bronze/letterboxd/ingestion_date=$(date +%Y-%m-%d)/

# 5. Ejecutar pipeline desde Databricks Workflows (Jobs Compute, no All-Purpose)
# O disparar desde n8n via HTTP Request → Databricks REST API
```
---

## Buenas Prácticas de Ingeniería Implementadas

- **🔒 Zero credenciales hardcodeadas** — Secret Manager en GCP, variables de entorno en Fase 2
- **📦 Infrastructure as Code** — Terraform gestiona el 100% de los recursos GCP
- **🧪 Tests unitarios** — pytest cubre los módulos de ingesta (TMDB client es la primera unidad testeada)
- **⚡ Separación de compute en Databricks** — All-Purpose solo para desarrollo, Jobs Compute para producción
- **🔄 Upserts ACID** — Delta Lake elimina lógica manual de merge/dedup
- **📊 Data Quality como código** — Reglas YAML versionadas en Git, no configuración en UI
- **🔍 Linaje automático** — Dataplex traza el camino completo sin instrumentación manual
- **💸 Budget Alerts** — Obligatorio antes de desplegar cualquier servicio (25/50/75/90%)
- **🏗️ Portabilidad total** — `fsspec` + macros dbt abstraen 100% de las diferencias entre proveedores

---

## 🎓 Competencias Técnicas Demostradas

| Área | Habilidades |
|---|---|
| **Cloud Engineering** | GCP architecture, Terraform IaC, IAM least privilege, Secret Manager |
| **Data Processing** | PySpark (Databricks), DuckDB, Delta Lake, Medallion Architecture |
| **Data Modeling** | Star Schema, SCD Tipo 1/2, dbt snapshots, bridge tables |
| **Data Governance** | Dataplex DQ rules (YAML), data lineage, business glossary, incident debugging |
| **Orchestration** | Databricks Workflows, Cloud Scheduler, n8n (dual rol: alertas GCP / orquestador VPS) |
| **BI & Visualization** | Looker Studio, fan-out prevention, COUNT DISTINCT, array unnesting |
| **DevOps** | Docker Compose, RAM budgeting, resource limits, ephemeral containers |
| **Software Engineering** | fsspec abstraction, modular Python, pytest, `.env` management |

---

## 👤 Autor

**Robinson Barrazueta**
Data Engineer | Automation Solutions Developer | Founder @ Processia Ops

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat&logo=linkedin)](https://www.linkedin.com/in/robinson-barrazueta/)
[![GitHub](https://img.shields.io/badge/GitHub-rabarrazueta-181717?style=flat&logo=github)](https://github.com/rabarrazueta)

---

<div align="center">
  <sub>Datos enriquecidos con <a href="https://www.themoviedb.org">TMDB API</a> · Fuente de datos: <a href="https://letterboxd.com">Letterboxd</a> export CSV</sub>
</div>
