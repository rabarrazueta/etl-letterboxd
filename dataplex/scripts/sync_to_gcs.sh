#!/bin/bash
# Archivo: dataplex/scripts/sync_to_gcs.sh
# Propósito: Sincronizar las reglas locales de DQ hacia el Data Lake en GCP.

# 1. Definir variables estrictas (fácil de cambiar si clonas el repo en otro entorno)
PROJECT_ID="raspberry-9dcf6"
BUCKET_NAME="robinson-adrian-letterboxd-portfolio-datalake" # REEMPLAZA ESTO por el nombre real de tu bucket
LOCAL_RULES_DIR="../dq_rules"
GCS_TARGET_DIR="gs://${BUCKET_NAME}/dataplex_rules"

echo "======================================================"
echo "🚀 Iniciando sincronización de reglas Dataplex DQ..."
echo "======================================================"

# 2. Validar que el directorio local existe antes de intentar subir
if [ ! -d "$LOCAL_RULES_DIR" ]; then
  echo "Error: El directorio $LOCAL_RULES_DIR no existe localmente."
  echo "Asegúrate de ejecutar este script desde la carpeta /scripts."
  exit 1
fi

# 3. Ejecutar la copia usando gcloud storage (reemplazo moderno de gsutil)
echo "Copiando archivos YAML desde $LOCAL_RULES_DIR hacia $GCS_TARGET_DIR..."
gcloud storage cp "${LOCAL_RULES_DIR}/*.yaml" "${GCS_TARGET_DIR}/"

# 4. Verificar el código de salida del comando anterior
if [ $? -eq 0 ]; then
  echo "Sincronización completada con éxito."
  echo "Archivos actualmente en el bucket:"
  gcloud storage ls "${GCS_TARGET_DIR}/"
else
  echo "Falla crítica en la sincronización. Revisa tus permisos de GCP."
  exit 1
fi