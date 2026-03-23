# Databricks notebook source

# COMMAND ----------

# ── CELDA 1: Parámetros del Job ──────────────────────────────────────────
dbutils.widgets.text("env", "development", "Entorno")
dbutils.widgets.text("source", "letterboxd", "Fuente de datos")
dbutils.widgets.text("ingestion_date", "", "Fecha (vacío = hoy)")

ENV            = dbutils.widgets.get("env")
SOURCE         = dbutils.widgets.get("source")
INGESTION_DATE = dbutils.widgets.get("ingestion_date")

print(f"▶ Entorno:  {ENV}")
print(f"▶ Fuente:   {SOURCE}")
print(f"▶ Fecha:    {INGESTION_DATE or 'hoy (automático)'}")


# COMMAND ----------

# ── CELDA 2: Configuración (sin hardcodear NADA) ─────────────────────────
from datetime import date

# Leer configuración desde el Secret Scope (que apunta a GCP Secret Manager)
PROJECT_ID = dbutils.secrets.get(scope="gcp-secrets", key="project-id")
BUCKET     = dbutils.secrets.get(scope="gcp-secrets", key="gcs-bucket")

# Calcular fecha de ingesta
RUN_DATE = INGESTION_DATE if INGESTION_DATE else str(date.today())

# Rutas en GCS
LANDING_PATH = f"gs://{BUCKET}/landing/{SOURCE}/"
BRONZE_BASE  = f"gs://{BUCKET}/bronze/{SOURCE}/"
BRONZE_PATH  = f"{BRONZE_BASE}ingestion_date={RUN_DATE}/"

print(f"📦 Bucket:        {BUCKET}")
print(f"📥 Landing zone:  {LANDING_PATH}")
print(f"🥉 Bronze target: {BRONZE_PATH}")
print(f"📅 Fecha run:     {RUN_DATE}")


# COMMAND ----------

# ── CELDA 3: Detectar archivos en Landing Zone ───────────────────────────
# En Databricks en GCP, el clúster lee GCS directamente
# gracias a la Service Account configurada en el cluster

# Archivos esperados según la fuente
EXPECTED_FILES = {
    "letterboxd": ["diary.csv", "ratings.csv", "watched.csv", 
                   "reviews.csv", "watchlist.csv"]
}

expected = EXPECTED_FILES.get(SOURCE, [])

# Verificar cuáles archivos existen en landing
try:
    landing_files = dbutils.fs.ls(LANDING_PATH)
    found_names   = [f.name for f in landing_files if f.name.endswith(".csv")]
    missing       = [f for f in expected if f not in found_names]
    
    print(f"Archivos encontrados en landing ({len(found_names)}):")
    for f in found_names:
        print(f"   - {f}")
    
    if missing:
        print(f"\n Archivos ausentes: {missing}")
        print("   El pipeline continuará con los archivos disponibles.")
    
    FILES_TO_PROCESS = [f"{LANDING_PATH}{f}" for f in found_names]
    
except Exception as e:
    # Si landing está vacía pero Bronze ya tiene datos de hoy, no es error
    try:
        existing_bronze = dbutils.fs.ls(BRONZE_PATH)
        print(f"ℹ Landing vacía pero Bronze ya tiene datos para {RUN_DATE}.")
        print(f"   Archivos en Bronze: {len(existing_bronze)}")
        FILES_TO_PROCESS = []
    except:
        raise Exception(
            f"No hay archivos en landing ({LANDING_PATH}) "
            f"ni en Bronze ({BRONZE_PATH}). "
            f"Sube los CSV de Letterboxd a la landing zone antes de ejecutar."
        )

print(f"\n Total a procesar: {len(FILES_TO_PROCESS)} archivos")


# COMMAND ----------

# ── CELDA 4: Ingesta Bronze ───────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def ingest_csv_to_bronze(file_path: str, bronze_path: str, run_date: str, source: str):
    """
    Lee un CSV desde landing, agrega metadata de ingesta,
    y lo escribe en Bronze en formato Parquet particionado.
    Es idempotente: si la partición ya existe, la sobreescribe.
    """
    file_name = file_path.split("/")[-1]          # "diary.csv"
    table_name = file_name.replace(".csv", "")    # "diary"
    output_path = f"{bronze_path}{table_name}/"
    
    print(f"\n{'─'*50}")
    print(f"Procesando: {file_name}")
    
    # 1. Leer el CSV (header=True infiere los nombres de columna)
    df = (spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "false")   # Bronze = todo como string, sin inferir tipos
          .option("encoding", "UTF-8")
          .load(file_path))
    
    row_count = df.count()
    print(f"   Filas leídas: {row_count}")
    print(f"   Columnas:     {df.columns}")
    
    # 2. Agregar columnas de metadata (linaje de datos)
    df_enriched = (df
        .withColumn("_ingestion_date",    F.lit(run_date))
        .withColumn("_source_system",     F.lit(source))
        .withColumn("_source_file",       F.lit(file_name))
        .withColumn("_ingested_at",       F.current_timestamp())
        .withColumn("_pipeline_version",  F.lit("1.0.0")))
    
    # 3. Escribir en Bronze como Parquet (sobre Hive-style partitioning)
    # mode="overwrite" + partitionBy hace la escritura idempotente:
    # si vuelves a correr hoy, sobreescribe solo la partición de hoy
    (df_enriched.write
        .format("parquet")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .save(output_path))
    
    print(f"   Escrito en: {output_path}")
    print(f"   Columnas finales: {df_enriched.columns}")
    
    return {"file": file_name, "rows": row_count, "status": "SUCCESS"}


# ── Ejecutar para todos los archivos encontrados ──────────────────────────
results = []

if FILES_TO_PROCESS:
    for file_path in FILES_TO_PROCESS:
        result = ingest_csv_to_bronze(file_path, BRONZE_PATH, RUN_DATE, SOURCE)
        results.append(result)
else:
    print("ℹ Sin archivos nuevos que procesar. Bronze ya está actualizado.")


# COMMAND ----------

# ── CELDA 5: Validación Post-Ingesta ─────────────────────────────────────

print("\n" + "="*60)
print("RESUMEN DE INGESTA BRONZE")
print("="*60)

if results:
    total_rows = sum(r["rows"] for r in results)
    print(f"Archivos procesados: {len(results)}")
    print(f"Total filas ingestadas: {total_rows:,}")
    print(f"Partición creada: ingestion_date={RUN_DATE}")
    print()
    
    for r in results:
        status_icon = "✅" if r["status"] == "SUCCESS" else "❌"
        print(f"  {status_icon} {r['file']:<25} {r['rows']:>6} filas")
    
    # Verificar que los archivos realmente existen en GCS
    print("\n🔍 Verificación en GCS:")
    try:
        bronze_contents = dbutils.fs.ls(BRONZE_PATH)
        for item in bronze_contents:
            print(f"   ✓ {item.path} ({item.size:,} bytes)")
    except Exception as e:
        print(f"   ⚠️ No se pudo listar Bronze: {e}")

else:
    # Verificar el estado actual de Bronze (datos previos del Módulo 1)
    print("ℹ No se procesaron archivos nuevos en este run.")
    print("\nEstado actual de Bronze (datos existentes):")
    try:
        all_bronze = dbutils.fs.ls(f"gs://{BUCKET}/bronze/{SOURCE}/")
        for partition in all_bronze:
            print(f"   📁 {partition.name}")
    except:
        print("   (sin datos en Bronze)")

print("\nNotebook 01_bronze_ingestion completado.")


# COMMAND ----------

# ── CELDA 6: Cleanup ──────────────────────────────────────────────────────
# Remover widgets al finalizar para evitar interferencia entre runs
# (Solo descomentar en producción — comentado para facilitar re-ejecución en desarrollo)

# dbutils.widgets.removeAll()
print("✅ Notebook finalizado correctamente.")
