# Databricks notebook source
# databricks/notebooks/05_dataplex_scan.py
# Dispara el scan de Data Quality en Dataplex via REST API
import requests
from google.auth import default
from google.auth.transport.requests import Request

dbutils.widgets.text("project_id", "mi-proyecto-default")
PROJECT_ID = dbutils.widgets.get("project_id")
SCAN_IDS    = [
    "fact-diary-entries-dq-scan",
    "fact-ratings-dq-scan",
    "dim-movie-dq-scan",
]

# Obtener credenciales y verificar qué cuenta se está usando
credentials, project = default()
credentials.refresh(Request())

# Mostrar información de la cuenta para debugging
print(f"🔐 Using project: {project or PROJECT_ID}")
if hasattr(credentials, 'service_account_email'):
    print(f"🔐 Service Account: {credentials.service_account_email}")
else:
    print(f"🔐 Credential type: {type(credentials).__name__}")
print(f"🔐 Token expires: {credentials.expiry}")
print()

headers = {
    "Authorization": f"Bearer {credentials.token}",
    "Content-Type": "application/json",
}

for scan_id in SCAN_IDS:
    url = (
        f"https://dataplex.googleapis.com/v1/projects/{PROJECT_ID}"
        f"/locations/{LOCATION}/dataScans/{scan_id}:run"
    )
    
    try:
        response = requests.post(url, headers=headers, json={})
        response.raise_for_status()
        job_name = response.json().get('job', {}).get('name', 'N/A')
        print(f"✅ Scan disparado: {scan_id} — jobId: {job_name}")
    except requests.exceptions.HTTPError as e:
        print(f"❌ Error para {scan_id}: {e}")
        if e.response.status_code == 403:
            print(f"   ⚠️  403 Forbidden - La cuenta no tiene permisos suficientes")
            print(f"   ⚠️  Asegúrate de que la service account tenga el rol:")
            print(f"   ⚠️  - roles/dataplex.dataScanner (o dataplex.admin)")
            print(f"   ⚠️  Response: {e.response.text}")
        elif e.response.status_code == 404:
            print(f"   ⚠️  404 Not Found - El scan '{scan_id}' no existe en {LOCATION}")
        print()