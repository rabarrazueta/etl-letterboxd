# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Transformation Pipeline
# MAGIC
# MAGIC **Arquitectura:** Bronze (GCS CSV) → PySpark Clean → TMDB Enrichment → Delta Lake Silver (GCS)
# MAGIC
# MAGIC | Capa | Formato | Path |
# MAGIC |------|---------|------|
# MAGIC | Bronze | CSV | `gs://{bucket}/bronze/letterboxd/ingestion_date=YYYY-MM-DD/` |
# MAGIC | Silver Movies | Delta Lake | `gs://{bucket}/silver/movies/` |
# MAGIC | Silver Diary | Delta Lake | `gs://{bucket}/silver/diary_entries/` |
# MAGIC | Silver Ratings | Delta Lake | `gs://{bucket}/silver/ratings/` |
# MAGIC **Clave de Upsert:** `letterboxd_uri` (siempre disponible) + `tmdb_id` cuando resuelto.
# MAGIC **Restricción:** Este notebook corre SOLO en Jobs Compute. Prohibido All-Purpose en producción.

# COMMAND ----------

# DBTITLE 1,Widget Parameters — Parametriza toda ejecución aquí
dbutils.widgets.removeAll()
dbutils.widgets.text("project_id", "YOUR_GCP_PROJECT_ID", "GCP Project ID")
dbutils.widgets.text("bucket_name", "YOUR_GCS_BUCKET", "GCS Bucket (sin gs://)")
dbutils.widgets.text("ingestion_date", "", "Bronze Partition (vacío = última disponible)")
dbutils.widgets.dropdown("run_mode", "incremental", ["incremental", "full_refresh"], "Run Mode")
dbutils.widgets.text("tmdb_concurrency", "40", "Concurrent TMDB requests (max 45)")

# COMMAND ----------

# DBTITLE 1,Imports
import os
import re
import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType,
    ArrayType, LongType,
)
from delta.tables import DeltaTable

import gcsfs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("silver_pipeline")

# COMMAND ----------

# DBTITLE 1,Runtime Configuration
PROJECT_ID      = dbutils.widgets.get("project_id")
BUCKET          = dbutils.widgets.get("bucket_name")
INGESTION_DATE  = dbutils.widgets.get("ingestion_date").strip() or None
RUN_MODE        = dbutils.widgets.get("run_mode")
TMDB_CONCURRENCY = min(int(dbutils.widgets.get("tmdb_concurrency")), 45)

# Paths — toda la lógica de storage parametrizada, nunca hardcodeada
BRONZE_BASE          = f"gs://{BUCKET}/bronze/letterboxd"
SILVER_MOVIES_PATH   = f"gs://{BUCKET}/silver/movies"
SILVER_DIARY_PATH    = f"gs://{BUCKET}/silver/diary_entries"
SILVER_RATINGS_PATH  = f"gs://{BUCKET}/silver/ratings"

PIPELINE_RUN_TS   = datetime.now(timezone.utc)
PIPELINE_RUN_STR  = PIPELINE_RUN_TS.isoformat()

# Registrar en Spark para queries SQL posteriores
spark.conf.set("pipeline.run_ts", PIPELINE_RUN_STR)
spark.conf.set("pipeline.run_mode", RUN_MODE)

print(f"▶ Project:       {PROJECT_ID}")
print(f"▶ Bucket:        {BUCKET}")
print(f"▶ Run Mode:      {RUN_MODE}")
print(f"▶ TMDB Workers:  {TMDB_CONCURRENCY}")
print(f"▶ Pipeline TS:   {PIPELINE_RUN_STR}")

# COMMAND ----------

# DBTITLE 1,Secret Retrieval — GCP Secret Manager via ADC
# En Databricks on GCP (Marketplace), el clúster usa el SA configurado en el workspace.
# Ese SA tiene roles/secretmanager.secretAccessor asignado por Terraform.
# Acceso via Application Default Credentials (ADC) — sin hardcodear credenciales.
# Referencia: https://cloud.google.com/secret-manager/docs/authentication [web:19]

def get_gcp_secret(secret_id: str, project_id: str = PROJECT_ID) -> str:
    """
    Recupera un secreto desde GCP Secret Manager usando ADC del clúster.
    Equivalente GCP-nativo al Key Vault-backed scope de Azure Databricks.
    """
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    name   = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    resp   = client.access_secret_version(request={"name": name})
    return resp.payload.data.decode("UTF-8")


# Intentar primero desde Databricks scope (si fue registrado manualmente),
# fallback directo a Secret Manager vía ADC.
try:
    TMDB_API_KEY = dbutils.secrets.get(
        scope="portfolio-gcp-secrets", key="tmdb-api-key"
    )
    logger.info("✅ TMDB key cargada desde Databricks Secret Scope")
except Exception:
    TMDB_API_KEY = get_gcp_secret("tmdb-api-key")
    logger.info("✅ TMDB key cargada desde GCP Secret Manager (ADC directo)")

# Validar sin revelar el valor
assert len(TMDB_API_KEY) > 20, "⛔ TMDB API Key inválida — verificar Secret Manager"

# COMMAND ----------

# DBTITLE 1,Storage Utilities — fsspec/gcsfs (layer de abstracción)
# Toda operación Python sobre GCS usa gcsfs.
# Spark usa gs:// nativo (GCS Connector pre-instalado en DBR 14.x on GCP).
# Cambiar a local en Fase 2 = solo cambiar el backend en .env, no este código.

def get_fs(backend: str = "gcs") -> Any:
    if backend == "gcs":
        # Agregamos token='cloud' para que use la Service Account del clúster
        return gcsfs.GCSFileSystem(project=PROJECT_ID, token='cloud')
    elif backend == "local":
        import fsspec
        return fsspec.filesystem("file")
    raise ValueError(f"Backend no soportado: {backend}")


def resolve_latest_bronze_partition(source: str = "letterboxd") -> str:
    """
    Versión ultra-robusta usando dbutils (evita errores de gcsfs).
    """
    # Usamos la ruta completa con gs:// y el bucket que pusiste en el widget
    base = f"gs://{BUCKET}/bronze/{source}"
    
    # dbutils ya sabemos que tiene tus permisos de Google Cloud
    try:
        paths = dbutils.fs.ls(base)
        dates = [
            p.path.split("ingestion_date=")[-1].rstrip("/")
            for p in paths
            if "ingestion_date=" in p.path
        ]
        if not dates:
            raise FileNotFoundError(f"No hay particiones Bronze en {base}")
        
        latest = sorted(dates)[-1]
        logger.info(f"✅ Partición Bronze detectada: ingestion_date={latest}")
        return latest
    except Exception as e:
        raise Exception(f"Error accediendo a {base}. Revisa el nombre del bucket y permisos. Detalle: {e}")

# COMMAND ----------

# DBTITLE 1,Determine Target Bronze Partition
target_date      = INGESTION_DATE or resolve_latest_bronze_partition("letterboxd")
BRONZE_PARTITION = f"{BRONZE_BASE}/ingestion_date={target_date}"

print(f"📂 Bronze partition seleccionada: {BRONZE_PARTITION}")

# COMMAND ----------

# DBTITLE 1,Load Bronze CSVs — Schema-on-Read (todo StringType inicial)
def read_bronze_csv(partition_path: str, filename: str) -> DataFrame:
    """
    Lee datos desde Bronze en formato Parquet.
    La tabla corresponde al nombre del archivo sin extensión .csv
    """
    table_name = filename.replace(".csv", "")
    table_path = f"{partition_path}/{table_name}/"
    
    return (
        spark.read
        .format("parquet")
        .load(table_path)
    )

df_diary_raw   = read_bronze_csv(BRONZE_PARTITION, "diary.csv")
df_ratings_raw = read_bronze_csv(BRONZE_PARTITION, "ratings.csv")
df_watched_raw = read_bronze_csv(BRONZE_PARTITION, "watched.csv")

print(f"📊 diary.csv:   {df_diary_raw.count():,} filas")
print(f"📊 ratings.csv: {df_ratings_raw.count():,} filas")
print(f"📊 watched.csv: {df_watched_raw.count():,} filas")

# COMMAND ----------

# DBTITLE 1,Cleaning Utilities — Funciones Compartidas
def normalize_title(col_expr) -> F.Column:
    """Normalización canónica de título para fuzzy matching contra TMDB."""
    return F.regexp_replace(
        F.trim(F.lower(col_expr)), r"[^a-z0-9\s]", ""
    )

def safe_year(col_name: str) -> F.Column:
    """Año de 4 dígitos a IntegerType; null si formato inválido."""
    return F.when(
        F.col(col_name).rlike(r"^\d{4}$"),
        F.col(col_name).cast(IntegerType())
    ).otherwise(F.lit(None).cast(IntegerType()))

def safe_rating(col_name: str) -> F.Column:
    """Rating Letterboxd (0.5–5.0) a FloatType; null si vacío."""
    return F.when(
        F.col(col_name).rlike(r"^\d+\.?\d*$"),
        F.col(col_name).cast(FloatType())
    ).otherwise(F.lit(None).cast(FloatType()))

def add_pipeline_metadata(df: DataFrame, source: str) -> DataFrame:
    """Agrega columnas de linaje a todos los DataFrames Silver."""
    return (
        df
        .withColumn("source_file",          F.lit(source))
        .withColumn("_ingestion_date",       F.lit(target_date))
        .withColumn("_silver_created_at",    F.lit(PIPELINE_RUN_STR).cast(TimestampType()))
        .withColumn("_silver_updated_at",    F.lit(PIPELINE_RUN_STR).cast(TimestampType()))
    )

# COMMAND ----------

# DBTITLE 1,Clean — diary.csv
def clean_diary(df: DataFrame) -> DataFrame:
    return (
        df
        # Renombrar columnas Letterboxd → snake_case
        .withColumnRenamed("Date",          "diary_date_str")
        .withColumnRenamed("Name",          "movie_title")
        .withColumnRenamed("Year",          "movie_year_str")
        .withColumnRenamed("Letterboxd URI","letterboxd_uri")
        .withColumnRenamed("Rating",        "my_rating_str")
        .withColumnRenamed("Rewatch",       "is_rewatch_str")
        .withColumnRenamed("Tags",          "tags_raw")
        .withColumnRenamed("Watched Date",  "watched_date_str")
        # Tipos
        .withColumn("diary_date",   F.to_date("diary_date_str",   "yyyy-MM-dd"))
        .withColumn("watched_date", F.to_date("watched_date_str", "yyyy-MM-dd"))
        .withColumn("movie_year",   safe_year("movie_year_str"))
        .withColumn("my_rating",    safe_rating("my_rating_str"))
        .withColumn("is_rewatch",   F.col("is_rewatch_str").isin("Yes", "yes", "TRUE", "1"))
        # Derivados
        .withColumn("movie_title_normalized", normalize_title(F.col("movie_title")))
        .withColumn("tags", F.split(F.col("tags_raw"), ","))   # Array<String>
        # Metadata de linaje
        .transform(lambda d: add_pipeline_metadata(d, "diary"))
        # Limpiar raw
        .drop("diary_date_str", "watched_date_str", "movie_year_str",
              "my_rating_str", "is_rewatch_str", "tags_raw")
        # Filtros de calidad mínima
        .filter(F.col("letterboxd_uri").isNotNull())
        .filter(F.col("movie_title").isNotNull() & (F.trim(F.col("movie_title")) != ""))
        # Dedup: una entrada de diario = URI + fecha vista
        .dropDuplicates(["letterboxd_uri", "watched_date"])
    )

df_diary = clean_diary(df_diary_raw)
print(f"✅ diary limpio: {df_diary.count():,} filas")

# COMMAND ----------

# DBTITLE 1,Clean — ratings.csv
def clean_ratings(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumnRenamed("Date",          "rating_date_str")
        .withColumnRenamed("Name",          "movie_title")
        .withColumnRenamed("Year",          "movie_year_str")
        .withColumnRenamed("Letterboxd URI","letterboxd_uri")
        .withColumnRenamed("Rating",        "my_rating_str")
        .withColumn("rating_date", F.to_date("rating_date_str", "yyyy-MM-dd"))
        .withColumn("movie_year",  safe_year("movie_year_str"))
        .withColumn("my_rating",   safe_rating("my_rating_str"))
        .withColumn("movie_title_normalized", normalize_title(F.col("movie_title")))
        .transform(lambda d: add_pipeline_metadata(d, "ratings"))
        .drop("rating_date_str", "movie_year_str", "my_rating_str")
        .filter(F.col("letterboxd_uri").isNotNull())
        .dropDuplicates(["letterboxd_uri", "rating_date"])
    )

df_ratings = clean_ratings(df_ratings_raw)
print(f"✅ ratings limpio: {df_ratings.count():,} filas")

# COMMAND ----------

# DBTITLE 1,Clean — watched.csv
def clean_watched(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumnRenamed("Date",          "watched_date_str")
        .withColumnRenamed("Name",          "movie_title")
        .withColumnRenamed("Year",          "movie_year_str")
        .withColumnRenamed("Letterboxd URI","letterboxd_uri")
        .withColumn("watched_date", F.to_date("watched_date_str", "yyyy-MM-dd"))
        .withColumn("movie_year",   safe_year("movie_year_str"))
        .withColumn("movie_title_normalized", normalize_title(F.col("movie_title")))
        .transform(lambda d: add_pipeline_metadata(d, "watched"))
        .drop("watched_date_str", "movie_year_str")
        .filter(F.col("letterboxd_uri").isNotNull())
        .dropDuplicates(["letterboxd_uri", "watched_date"])
    )

df_watched = clean_watched(df_watched_raw)
print(f"✅ watched limpio: {df_watched.count():,} filas")

# COMMAND ----------

# DBTITLE 1,Build Unique Movies Catalog — Universo a enriquecer
# Universo completo de películas únicas a través de todos los fuentes.
# El enriquecimiento ocurre AQUÍ, en el driver, antes de escribir Silver.
# Colectar al driver es correcto para este volumen (< 10k películas Letterboxd).

df_all_movies = (
    df_diary.select("letterboxd_uri", "movie_title", "movie_year")
    .union(df_ratings.select("letterboxd_uri", "movie_title", "movie_year"))
    .union(df_watched.select("letterboxd_uri", "movie_title", "movie_year"))
    .dropDuplicates(["letterboxd_uri"])
    .filter(F.col("movie_title").isNotNull())
)

# Modo incremental: filtrar películas ya enriquecidas en Silver anterior
if RUN_MODE == "incremental" and DeltaTable.isDeltaTable(spark, SILVER_MOVIES_PATH):
    already_enriched = (
        spark.read.format("delta").load(SILVER_MOVIES_PATH)
        .filter(F.col("enrichment_status") == "SUCCESS")
        .select("letterboxd_uri")
    )
    df_to_enrich = df_all_movies.join(
        already_enriched, on="letterboxd_uri", how="left_anti"
    )
    logger.info(f"Modo incremental: {df_to_enrich.count()} nuevas películas a enriquecer")
else:
    df_to_enrich = df_all_movies
    logger.info(f"Modo full_refresh: {df_to_enrich.count()} películas totales")

movies_to_enrich: List[Dict] = df_to_enrich.toPandas().to_dict("records")
print(f"🎬 Películas a enriquecer con TMDB: {len(movies_to_enrich):,}")

# COMMAND ----------

# DBTITLE 1,TMDB API Client — Async, Rate-Limited, con Tenacity
# Rate limit TMDB: ~50 req/segundo (límite legacy de 40/10s fue eliminado en 2019).
# Estrategia conservadora: Semaphore(40) + retry exponencial en 429. [web:6][web:12]
# append_to_response=credits evita una llamada extra por película. [web:20][web:28]

class TMDBClient:
    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self, api_key: str, max_concurrent: int = 40):
        self._api_key     = api_key
        self._semaphore   = asyncio.Semaphore(max_concurrent)
        self._headers     = {"Authorization": f"Bearer {api_key}"}

    @retry(
        retry=retry_if_exception_type(httpx.HTTPStatusError),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def _get(self, client: httpx.AsyncClient, path: str, params: Dict) -> Dict:
        async with self._semaphore:
            resp = await client.get(
                f"{self.BASE_URL}{path}",
                params={**params, "language": "en-US"},
                headers=self._headers,
                timeout=15.0,
            )
            if resp.status_code == 429:
                raise httpx.HTTPStatusError(
                    "Rate limited", request=resp.request, response=resp
                )
            resp.raise_for_status()
            return resp.json()

    async def search_movie(
        self,
        client:  httpx.AsyncClient,
        title:   str,
        year:    Optional[int],
    ) -> Optional[Dict]:
        """
        Busca película en TMDB por título + año.
        Estrategia: exact year match → relax to ±1 year → title-only.
        Retorna primer resultado con score de confianza, o None.
        """
        params = {"query": title, "include_adult": "false", "page": 1}

        # Intento 1: título + año exacto
        if year:
            data = await self._get(client, "/search/movie", {**params, "year": year})
            if data.get("results"):
                return data["results"][0]

        # Intento 2: solo título (sin año) — acepta primer resultado plausible
        data = await self._get(client, "/search/movie", params)
        results = data.get("results", [])
        if not results:
            return None

        # Verificar plausibilidad: año del resultado ≤ 2 años de diferencia
        for result in results[:3]:
            release_year = (result.get("release_date") or "")[:4]
            if release_year.isdigit() and year:
                if abs(int(release_year) - year) <= 2:
                    return result
        # Sin año de referencia: retornar primer resultado
        return results[0] if not year else None

    async def get_details(
        self,
        client:   httpx.AsyncClient,
        tmdb_id:  int,
    ) -> Dict:
        """
        Obtiene detalles completos + credits en una sola llamada HTTP.
        append_to_response=credits incluye director sin request adicional.
        """
        return await self._get(
            client,
            f"/movie/{tmdb_id}",
            {"append_to_response": "credits"},
        )

    @staticmethod
    def extract_director(credits: Dict) -> Tuple[Optional[str], Optional[int]]:
        """Extrae nombre e ID del director desde el objeto credits.crew."""
        for member in credits.get("crew", []):
            if member.get("job") == "Director":
                return member.get("name"), member.get("id")
        return None, None

    async def enrich_one(
        self,
        client:         httpx.AsyncClient,
        letterboxd_uri: str,
        movie_title:    str,
        movie_year:     Optional[int],
    ) -> Dict:
        """
        Enriquece una película individual. Maneja todos los estados:
        SUCCESS | FUZZY_MATCH | NOT_FOUND | ERROR
        """
        base = {
            "letterboxd_uri": letterboxd_uri,
            "lb_movie_title": movie_title,
            "lb_movie_year":  movie_year,
            "enrichment_timestamp": PIPELINE_RUN_STR,
        }
        try:
            match = await self.search_movie(client, movie_title, movie_year)

            if not match:
                return {**base, "enrichment_status": "NOT_FOUND",
                        "tmdb_id": None}

            tmdb_id = match["id"]
            details = await self.get_details(client, tmdb_id)

            director_name, director_tmdb_id = self.extract_director(
                details.get("credits", {})
            )

            # Determinar si fue match exacto o fuzzy
            release_year = (details.get("release_date") or "")[:4]
            status = "SUCCESS"
            if movie_year and release_year.isdigit():
                if abs(int(release_year) - movie_year) > 2:
                    status = "FUZZY_MATCH"

            return {
                **base,
                "enrichment_status":  status,
                "tmdb_id":            tmdb_id,
                "tmdb_title":         details.get("title"),
                "tmdb_original_title":details.get("original_title"),
                "release_date":       details.get("release_date"),
                "genres":             [g["name"] for g in details.get("genres", [])],
                "runtime_minutes":    details.get("runtime"),
                "budget_usd":         details.get("budget") or None,
                "revenue_usd":        details.get("revenue") or None,
                "vote_average":       details.get("vote_average"),
                "vote_count":         details.get("vote_count"),
                "overview":           details.get("overview"),
                "original_language":  details.get("original_language"),
                "poster_path":        details.get("poster_path"),
                "director_name":      director_name,
                "director_tmdb_id":   director_tmdb_id,
            }

        except Exception as exc:
            logger.error(f"Error enriching '{movie_title}' ({movie_year}): {exc}")
            return {**base, "enrichment_status": "ERROR",
                    "tmdb_id": None, "error_msg": str(exc)}

    async def enrich_batch(self, movies: List[Dict]) -> List[Dict]:
        """Procesa el batch completo con concurrencia controlada por Semaphore."""
        async with httpx.AsyncClient(http2=True, timeout=20.0) as client:
            tasks = [
                self.enrich_one(
                    client,
                    m["letterboxd_uri"],
                    m["movie_title"],
                    m.get("movie_year"),
                )
                for m in movies
            ]
            return await asyncio.gather(*tasks, return_exceptions=False)

# COMMAND ----------

# DBTITLE 1,Execute TMDB Enrichment — Driver Node
# El enriquecimiento ocurre en el driver, no en workers.
# Razón: rate limiting centralizado + el volumen no justifica distribución.
# Para el dataset Letterboxd (< 5k películas), asyncio en driver es óptimo.

if movies_to_enrich:
    tmdb_client    = TMDBClient(TMDB_API_KEY, max_concurrent=TMDB_CONCURRENCY)
    enriched_list = await tmdb_client.enrich_batch(movies_to_enrich)

    # Resumen de enriquecimiento
    status_counts = {}
    for r in enriched_list:
        s = r.get("enrichment_status", "UNKNOWN")
        status_counts[s] = status_counts.get(s, 0) + 1

    print("📊 Resultado del enriquecimiento TMDB:")
    for status, count in sorted(status_counts.items()):
        emoji = {"SUCCESS": "✅", "FUZZY_MATCH": "🟡",
                 "NOT_FOUND": "⚠️", "ERROR": "❌"}.get(status, "❓")
        print(f"   {emoji} {status}: {count:,}")
else:
    enriched_list = []
    print("ℹ️ No hay películas nuevas a enriquecer (modo incremental, todo cached)")

# COMMAND ----------

# DBTITLE 1,Silver Movies Schema — StructType explícito
# Schema explícito evita inferencia incorrecta al crear la tabla Delta.
# Las columnas de metadata de linaje siguen la convención dbt (_dbt_updated_at).

SILVER_MOVIES_SCHEMA = StructType([
    StructField("letterboxd_uri",     StringType(),  False),  # PK natural
    StructField("lb_movie_title",     StringType(),  True),
    StructField("lb_movie_year",      IntegerType(), True),
    StructField("enrichment_status",  StringType(),  False),  # SUCCESS|FUZZY_MATCH|NOT_FOUND|ERROR
    StructField("enrichment_timestamp", StringType(),True),
    # TMDB fields
    StructField("tmdb_id",            IntegerType(), True),
    StructField("tmdb_title",         StringType(),  True),
    StructField("tmdb_original_title",StringType(),  True),
    StructField("release_date",       StringType(),  True),   # Cast a DateType en Gold
    StructField("genres",             ArrayType(StringType()), True),
    StructField("runtime_minutes",    IntegerType(), True),
    StructField("budget_usd",         LongType(),    True),
    StructField("revenue_usd",        LongType(),    True),
    StructField("vote_average",       DoubleType(),  True),
    StructField("vote_count",         IntegerType(), True),
    StructField("overview",           StringType(),  True),
    StructField("original_language",  StringType(),  True),
    StructField("poster_path",        StringType(),  True),
    StructField("director_name",      StringType(),  True),
    StructField("director_tmdb_id",   IntegerType(), True),
    # Linaje — compatibles con convención dbt
    StructField("_silver_created_at", TimestampType(), True),
    StructField("_silver_updated_at", TimestampType(), True),
    StructField("_ingestion_date",    StringType(),    True),
])

# COMMAND ----------

# DBTITLE 1,Upsert Silver Movies — Delta MERGE por letterboxd_uri
# Estrategia MERGE:
# - MATCHED: actualizar si enrichment_status mejoró (NOT_FOUND → SUCCESS, etc.)
#   o si la nueva versión TMDB tiene más datos (vote_count mayor).
# - NOT MATCHED: insertar nuevo registro.
# Referencia: DeltaTable.merge() API [web:1]

def upsert_silver_movies(enriched_records: List[Dict], path: str) -> int:
    if not enriched_records:
        logger.info("Sin registros nuevos para silver_movies")
        return 0

    # Crear DataFrame con schema explícito
    enriched_pd = spark.createDataFrame(
        enriched_records, schema=SILVER_MOVIES_SCHEMA
    ).withColumn(
        "_silver_created_at", F.lit(PIPELINE_RUN_STR).cast(TimestampType())
    ).withColumn(
        "_silver_updated_at", F.lit(PIPELINE_RUN_STR).cast(TimestampType())
    ).withColumn(
        "_ingestion_date", F.lit(target_date)
    )

    if not DeltaTable.isDeltaTable(spark, path):
        # Primera ejecución: crear tabla Delta
        enriched_pd.write.format("delta").mode("overwrite").save(path)
        logger.info(f"✅ silver_movies creada con {enriched_pd.count():,} registros")
        return enriched_pd.count()

    # Ejecuciones subsiguientes: MERGE
    delta_table = DeltaTable.forPath(spark, path)
    (
        delta_table.alias("target")
        .merge(
            enriched_pd.alias("source"),
            "target.letterboxd_uri = source.letterboxd_uri"
        )
        # Actualizar si el nuevo enriquecimiento es mejor o más reciente
        .whenMatchedUpdate(
            condition="""
                source.enrichment_status = 'SUCCESS'
                AND (
                    target.enrichment_status != 'SUCCESS'
                    OR source.vote_count > target.vote_count
                    OR target.tmdb_id IS NULL
                )
            """,
            set={
                "tmdb_id":            "source.tmdb_id",
                "tmdb_title":         "source.tmdb_title",
                "tmdb_original_title":"source.tmdb_original_title",
                "release_date":       "source.release_date",
                "genres":             "source.genres",
                "runtime_minutes":    "source.runtime_minutes",
                "budget_usd":         "source.budget_usd",
                "revenue_usd":        "source.revenue_usd",
                "vote_average":       "source.vote_average",
                "vote_count":         "source.vote_count",
                "overview":           "source.overview",
                "original_language":  "source.original_language",
                "poster_path":        "source.poster_path",
                "director_name":      "source.director_name",
                "director_tmdb_id":   "source.director_tmdb_id",
                "enrichment_status":  "source.enrichment_status",
                "_silver_updated_at": "source._silver_updated_at",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    total = spark.read.format("delta").load(path).count()
    logger.info(f"✅ silver_movies después de MERGE: {total:,} registros")
    return total


movies_count = upsert_silver_movies(enriched_list, SILVER_MOVIES_PATH)

# COMMAND ----------

# DBTITLE 1,Upsert Silver Diary Entries — Delta MERGE por letterboxd_uri + watched_date
# Clave compuesta: letterboxd_uri + watched_date
# Razón: una misma película puede verse múltiples veces (is_rewatch=True).
# Cada (URI, fecha) es una entrada única e independiente del diario.
# [web:1][web:4]

def upsert_silver_diary(df_source: DataFrame, path: str) -> int:
    if df_source.rdd.isEmpty():
        logger.info("Sin filas nuevas para silver_diary_entries")
        return 0

    if not DeltaTable.isDeltaTable(spark, path):
        df_source.write.format("delta").mode("overwrite").save(path)
        count = df_source.count()
        logger.info(f"✅ silver_diary_entries creada: {count:,} filas")
        return count

    delta_table = DeltaTable.forPath(spark, path)
    (
        delta_table.alias("target")
        .merge(
            df_source.alias("source"),
            """
            target.letterboxd_uri = source.letterboxd_uri
            AND target.watched_date = source.watched_date
            """
        )
        # Actualizar rating si fue editado en Letterboxd
        .whenMatchedUpdate(
            condition="source.my_rating IS NOT NULL AND source.my_rating != target.my_rating",
            set={
                "my_rating":        "source.my_rating",
                "tags":             "source.tags",
                "_silver_updated_at":"source._silver_updated_at",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    total = spark.read.format("delta").load(path).count()
    logger.info(f"✅ silver_diary_entries después de MERGE: {total:,} filas")
    return total


diary_count = upsert_silver_diary(df_diary, SILVER_DIARY_PATH)

# COMMAND ----------

# DBTITLE 1,Upsert Silver Ratings — Delta MERGE por letterboxd_uri + rating_date
def upsert_silver_ratings(df_source: DataFrame, path: str) -> int:
    if df_source.isEmpty():
        return 0

    if not DeltaTable.isDeltaTable(spark, path):
        df_source.write.format("delta").mode("overwrite").save(path)
        return df_source.count()

    delta_table = DeltaTable.forPath(spark, path)
    (
        delta_table.alias("target")
        .merge(
            df_source.alias("source"),
            """
            target.letterboxd_uri = source.letterboxd_uri
            AND target.rating_date = source.rating_date
            """
        )
        .whenMatchedUpdate(
            condition="source.my_rating != target.my_rating",
            set={
                "my_rating":         "source.my_rating",
                "_silver_updated_at":"source._silver_updated_at",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    total = spark.read.format("delta").load(path).count()
    logger.info(f"✅ silver_ratings después de MERGE: {total:,} filas")
    return total


ratings_count = upsert_silver_ratings(df_ratings, SILVER_RATINGS_PATH)

# COMMAND ----------

# DBTITLE 1,Post-Write Quality Assertions — Fallar rápido ante datos inválidos
# Las aserciones aquí son pre-Dataplex: validaciones mínimas que deben pasar
# antes de considerar el job exitoso. Si fallan → el Databricks Workflow falla
# y dispara la alerta a n8n → Telegram.
# Las reglas formales de DQ están en /dataplex/dq_rules/ (YAML versionado).

def assert_table_quality(path: str, table_name: str) -> None:
    df = spark.read.format("delta").load(path)
    total = df.count()

    assert total > 0, f"⛔ {table_name}: tabla vacía después del MERGE"

    # Verificar que no se perdieron filas (anti-regresión)
    null_uri_count = df.filter(F.col("letterboxd_uri").isNull()).count()
    assert null_uri_count == 0, (
        f"⛔ {table_name}: {null_uri_count} filas con letterboxd_uri NULL"
    )
    logger.info(f"✅ {table_name}: {total:,} filas, 0 letterboxd_uri nulos")


def assert_movies_enrichment_rate(path: str, min_success_rate: float = 0.70) -> None:
    df      = spark.read.format("delta").load(path)
    total   = df.count()
    success = df.filter(F.col("enrichment_status") == "SUCCESS").count()
    rate    = success / total if total > 0 else 0

    logger.info(f"📊 Tasa de enriquecimiento TMDB: {rate:.1%} ({success}/{total})")
    assert rate >= min_success_rate, (
        f"⛔ Tasa de enriquecimiento {rate:.1%} < umbral {min_success_rate:.0%}. "
        f"Revisar TMDB API o datos Bronze."
    )

# Ejecutar todas las aserciones
assert_table_quality(SILVER_MOVIES_PATH,   "silver_movies")
assert_table_quality(SILVER_DIARY_PATH,    "silver_diary_entries")
assert_table_quality(SILVER_RATINGS_PATH,  "silver_ratings")
assert_movies_enrichment_rate(SILVER_MOVIES_PATH, min_success_rate=0.70)

print("✅ Todas las aserciones de calidad pasaron")

# COMMAND ----------

# DBTITLE 1,Audit Log — Escribir en Delta para trazabilidad del pipeline
# El audit log en Delta permite a Looker Studio mostrar el historial
# de ejecuciones del pipeline. También alimenta el panel de calidad.

AUDIT_LOG_PATH = f"gs://{BUCKET}/silver/_audit_log"

audit_record = {
    "pipeline_run_ts":         PIPELINE_RUN_STR,
    "run_mode":                RUN_MODE,
    "bronze_partition":        target_date,
    "movies_processed":        len(movies_to_enrich),
    "silver_movies_total":     movies_count,
    "silver_diary_total":      diary_count,
    "silver_ratings_total":    ratings_count,
    "tmdb_success":            status_counts.get("SUCCESS", 0),
    "tmdb_fuzzy_match":        status_counts.get("FUZZY_MATCH", 0),
    "tmdb_not_found":          status_counts.get("NOT_FOUND", 0),
    "tmdb_error":              status_counts.get("ERROR", 0),
    "job_status":              "SUCCESS",
}

audit_df = spark.createDataFrame([audit_record])

if DeltaTable.isDeltaTable(spark, AUDIT_LOG_PATH):
    audit_df.write.format("delta").mode("append").save(AUDIT_LOG_PATH)
else:
    audit_df.write.format("delta").mode("overwrite").save(AUDIT_LOG_PATH)

print("📋 Audit log escrito en Delta:")
audit_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Silver Transformation Completada
# MAGIC
# MAGIC | Tabla | Path | Formato |
# MAGIC |-------|------|---------|
# MAGIC | silver_movies | `gs://{bucket}/silver/movies/` | Delta Lake |
# MAGIC | silver_diary_entries | `gs://{bucket}/silver/diary_entries/` | Delta Lake |
# MAGIC | silver_ratings | `gs://{bucket}/silver/ratings/` | Delta Lake |
# MAGIC
# MAGIC **Siguiente paso:** Módulo 3 — dbt Gold Layer (BigQuery Star Schema)

# COMMAND ----------

# Verificar que las 3 tablas Delta existen y tienen filas
for path, name in [
    (SILVER_MOVIES_PATH,  "silver_movies"),
    (SILVER_DIARY_PATH,   "silver_diary_entries"),
    (SILVER_RATINGS_PATH, "silver_ratings"),
]:
    df = spark.read.format("delta").load(path)
    print(f"✅ {name}: {df.count():,} filas — columnas: {len(df.columns)}")


# COMMAND ----------

# Muestra 5 películas enriquecidas con sus datos de TMDB
spark.read.format("delta").load(SILVER_MOVIES_PATH) \
    .filter("enrichment_status = 'SUCCESS'") \
    .select("lb_movie_title", "lb_movie_year", "tmdb_id",
            "director_name", "genres", "vote_average", "runtime_minutes") \
    .show(5, truncate=False)


# COMMAND ----------

# Ver qué películas no encontró TMDB
spark.read.format("delta").load(SILVER_MOVIES_PATH) \
    .filter("enrichment_status = 'NOT_FOUND'") \
    .select("lb_movie_title", "lb_movie_year") \
    .show(10, truncate=False)


# COMMAND ----------

# Ver todas las películas NOT_FOUND (incluyendo de ejecuciones anteriores)
spark.read.format("delta").load(SILVER_MOVIES_PATH) \
    .filter("enrichment_status = 'NOT_FOUND'") \
    .orderBy("_silver_created_at", ascending=False) \
    .select("lb_movie_title", "lb_movie_year", "_silver_created_at") \
    .show(20, truncate=False)
