# Databricks notebook source
# databricks/notebooks/04_dbt_run.py
import subprocess
import os
import sys
import shutil

# ─── 1. INSTALACIÓN DE LIBRERÍAS ────────────────────────────────────────────
print("📦 1/4 Instalando dbt y fijando versión de numpy...")
subprocess.run([
    sys.executable, "-m", "pip", "install", 
    "dbt-core==1.8.7", 
    "dbt-bigquery==1.8.3", 
    "numpy==1.26.4"
], check=True)

# ─── 2. CONFIGURACIÓN DE RUTAS ──────────────────────────────────────────────
dbutils.widgets.removeAll()
dbutils.widgets.text("dbt_project_dir", "/Workspace/Repos/<tu_usuario>/etl-letterboxd/cine_analytics", "DBT Project Dir")
dbutils.widgets.text("gcp_project_id", "YOUR_GCP_PROJECT_ID", "GCP Project ID")
dbutils.widgets.text("bq_dataset", "portfolio_gold", "BigQuery Dataset")
dbutils.widgets.text("bq_location", "US", "BigQuery Location (ej: US)")
dbutils.widgets.text("json_key_path", "/Workspace/Users/<tu_usuario>/secrets/gcp-sa-key.json", "Service Account JSON (Workspace path)")

DBT_PROJECT_DIR = dbutils.widgets.get("dbt_project_dir")
GCP_PROJECT_ID  = dbutils.widgets.get("gcp_project_id")
BQ_DATASET      = dbutils.widgets.get("bq_dataset")
BQ_LOCATION     = dbutils.widgets.get("bq_location")
JSON_KEY_PATH   = dbutils.widgets.get("json_key_path")

if GCP_PROJECT_ID in ("", "YOUR_GCP_PROJECT_ID"):
    raise ValueError("Config inválida: setea el widget 'gcp_project_id' con tu GCP Project ID real.")
if BQ_DATASET.strip() == "":
    raise ValueError("Config inválida: setea el widget 'bq_dataset' con tu dataset real.")
if BQ_LOCATION.strip() == "":
    raise ValueError("Config inválida: setea el widget 'bq_location' (ej: US).")
if JSON_KEY_PATH.strip() == "":
    raise ValueError("Config inválida: setea el widget 'json_key_path' con la ruta del JSON en Workspace.")
if not os.path.isdir(DBT_PROJECT_DIR):
    raise ValueError(f"DBT project dir no existe: {DBT_PROJECT_DIR}")
if not os.path.isfile(JSON_KEY_PATH):
    raise ValueError(f"JSON keyfile no existe: {JSON_KEY_PATH}")

DBT_PROFILES_DIR = os.path.expanduser("~/.dbt")

# ─── 3. CREACIÓN DEL PROFILES.YML (MÉTODO SERVICE-ACCOUNT) ──────────────────
print(f"2/4 Generando conexión segura con BigQuery...")

profiles_content = f"""
cine_analytics:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: {GCP_PROJECT_ID}
      dataset: {BQ_DATASET}
      keyfile: {JSON_KEY_PATH}
      location: {BQ_LOCATION}
      threads: 4
      timeout_seconds: 300
"""

os.makedirs(DBT_PROFILES_DIR, exist_ok=True)
with open(os.path.join(DBT_PROFILES_DIR, "profiles.yml"), "w") as f:
    f.write(profiles_content)

# Limpieza de paquetes viejos para evitar el error de "malformed packages.yml"
shutil.rmtree(os.path.join(DBT_PROJECT_DIR, "dbt_packages"), ignore_errors=True)

# ─── 4. EJECUCIÓN DEL PIPELINE ──────────────────────────────────────────────
def run_dbt_command(command: list[str], step_name: str) -> None:
    cmd = ["dbt", *command, "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROFILES_DIR, "--target", "dev", "--no-use-colors"]
    print(f"\nEjecutando: {step_name}...")
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"Error en {step_name}. Revisa el log.")

run_dbt_command(["deps"], "dbt deps")
run_dbt_command(["snapshot"], "dbt snapshot")
run_dbt_command(["build"], "dbt build")

print("\n🎬 PROCESO FINALIZADO CON ÉXITO")