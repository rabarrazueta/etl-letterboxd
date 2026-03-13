# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver (Delta Lake) → BigQuery Staging
# MAGIC Exporta las 3 tablas Silver a BigQuery como tablas de staging.
# MAGIC dbt las consumirá desde BigQuery para construir el Star Schema Gold.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("project_id",  "raspberry-9dcf6",          "GCP Project ID")
dbutils.widgets.text("bucket_name", "robinson-adrian-letterboxd-portfolio-datalake", "GCS Bucket")
dbutils.widgets.text("bq_dataset",  "portfolio_gold",           "BigQuery Dataset")

PROJECT_ID = dbutils.widgets.get("project_id")
BUCKET     = dbutils.widgets.get("bucket_name")
BQ_DATASET = dbutils.widgets.get("bq_dataset")

SILVER_MOVIES_PATH   = f"gs://{BUCKET}/silver/movies"
SILVER_DIARY_PATH    = f"gs://{BUCKET}/silver/diary_entries"
SILVER_RATINGS_PATH  = f"gs://{BUCKET}/silver/ratings"

# El conector BigQuery integrado en DBR 14.x usa GCS como área temporal
spark.conf.set("temporaryGcsBucket", BUCKET)

# COMMAND ----------

def write_to_bq(path: str, table_name: str) -> None:
    df = spark.read.format("delta").load(path)
    full_table = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"
    print(f"📤 {path.split('/')[-1]} → {full_table} ({df.count():,} filas)")
    (
        df.write
        .format("bigquery")
        .mode("overwrite")
        .option("table", full_table)
        .option("temporaryGcsBucket", BUCKET)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_TRUNCATE")
        .save()
    )
    print(f"   ✅ Completado")

# COMMAND ----------

write_to_bq(SILVER_MOVIES_PATH,  "src_silver_movies")
write_to_bq(SILVER_DIARY_PATH,   "src_silver_diary_entries")
write_to_bq(SILVER_RATINGS_PATH, "src_silver_ratings")

# COMMAND ----------

# Verificación post-carga
print("\n📊 Verificación en BigQuery:")
for table in ["src_silver_movies", "src_silver_diary_entries", "src_silver_ratings"]:
    count = (
        spark.read.format("bigquery")
        .option("table", f"{PROJECT_ID}.{BQ_DATASET}.{table}")
        .load()
        .count()
    )
    print(f"   {table}: {count:,} filas ✅")