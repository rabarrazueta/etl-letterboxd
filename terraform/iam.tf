# iam.tf

# ------------------------------------------------------------------------
# Service Account para Databricks
# ------------------------------------------------------------------------
resource "google_service_account" "databricks_sa" {
  account_id   = "databricks-portfolio-sa"
  display_name = "Databricks Pipeline Service Account"
  description  = "SA utilizada por el clúster de Databricks para acceder a GCS, BQ y Secret Manager"
}

# Acceso al Bucket del Data Lake (Object Admin para leer Bronze y escribir Silver)
resource "google_storage_bucket_iam_member" "databricks_storage_admin" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.databricks_sa.email}"
}

# Acceso estricto SOLO al secreto de TMDB (No a todos los secretos del proyecto)
resource "google_secret_manager_secret_iam_member" "databricks_secret_accessor" {
  secret_id = google_secret_manager_secret.tmdb_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.databricks_sa.email}"
}

# Permisos para consultar y modificar BigQuery (útil para dbt o ingestas directas)
resource "google_project_iam_member" "databricks_bq_job_user" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.databricks_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "databricks_bq_data_editor" {
  dataset_id = google_bigquery_dataset.gold_layer.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.databricks_sa.email}"
}