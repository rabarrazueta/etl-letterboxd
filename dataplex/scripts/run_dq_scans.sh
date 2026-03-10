#!/bin/bash
# Archivo: dataplex/scripts/run_dq_scans.sh
# Propósito: Disparar manualmente los escaneos de calidad (Útil para pruebas y CI/CD).

LOCATION="us-central1"

echo "Disparando Dataplex Scan: fact-diary-dq-scan..."
gcloud dataplex datascans run fact-diary-dq-scan \
  --location=$LOCATION \
  --format="value(name)"

echo "Disparando Dataplex Scan: dim-movie-dq-scan..."
gcloud dataplex datascans run dim-movie-dq-scan \
  --location=$LOCATION \
  --format="value(name)"

echo "Jobs de escaneo enviados a GCP. Los resultados aparecerán en BigQuery (dq_results) en un par de minutos."