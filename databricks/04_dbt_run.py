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
# Ruta de tu código dbt (el que bajaste de GitHub a Repos)
DBT_PROJECT_DIR = "/Workspace/Users/robinsonbarrazueta@gmail.com/cine-analytics"

# Ruta del JSON que acabas de subir al Workspace (Ajusta el nombre del archivo)
JSON_KEY_PATH = "/Workspace/Users/robinsonbarrazueta@gmail.com/cine-analytics/secrets/gcp-sa-key.json"

DBT_PROFILES_DIR = os.path.expanduser("~/.dbt")

# ─── 3. CREACIÓN DEL PROFILES.YML (MÉTODO SERVICE-ACCOUNT) ──────────────────
print(f"🔑 2/4 Generando conexión segura con BigQuery...")

profiles_content = f"""
cine_analytics:
  target: bigquery
  outputs:
    bigquery:
      type: bigquery
      method: service-account
      project: raspberry-9dcf6
      dataset: portfolio_gold
      keyfile: {JSON_KEY_PATH}
      location: us-central1
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
    cmd = ["dbt", *command, "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROFILES_DIR, "--target", "bigquery", "--no-use-colors"]
    print(f"\n▶ Ejecutando: {step_name}...")
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"Error en {step_name}. Revisa el log.")

run_dbt_command(["deps"], "dbt deps")
run_dbt_command(["snapshot"], "dbt snapshot")
run_dbt_command(["run"], "dbt run")
run_dbt_command(["test"], "dbt test")

print("\n🎬 PROCESO FINALIZADO CON ÉXITO")