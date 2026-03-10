# main.tf

# ------------------------------------------------------------------------
# Google Cloud Storage - Data Lake
# ------------------------------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name                        = var.data_lake_bucket_name
  location                    = var.gcp_region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = false

  # Activamos versionamiento a nivel global del bucket
  versioning {
    enabled = true
  }

  # Regla de ciclo de vida: Simulación de versionamiento solo en Bronze
  # Elimina versiones antiguas (no actuales) que NO estén en la carpeta bronze/
  lifecycle_rule {
    condition {
      num_newer_versions = 1
      # Cualquier prefijo que no empiece con bronze/ sufrirá la eliminación de sus versiones pasadas
      matches_prefix = ["silver/", "gold-staging/"] 
    }
    action {
      type = "Delete"
    }
  }
}

# ------------------------------------------------------------------------
# BigQuery - Gold Layer
# ------------------------------------------------------------------------
resource "google_bigquery_dataset" "gold_layer" {
  dataset_id                  = "portfolio_gold"
  friendly_name               = "Portfolio Gold Layer"
  description                 = "Data Warehouse analítico para hábitos de cine"
  location                    = var.gcp_region
  delete_contents_on_destroy  = false

  # Mantenemos los datos indefinidamente, aprovechando el Free Tier de BigQuery
  # No se define default_table_expiration_ms
}

# ------------------------------------------------------------------------
# Secret Manager - TMDB API Key
# ------------------------------------------------------------------------
resource "google_secret_manager_secret" "tmdb_api_key" {
  secret_id = "tmdb-api-key"
  replication {
    auto {} # Replicación automática gestionada por GCP
  }
}

resource "google_secret_manager_secret_version" "tmdb_api_key_data" {
  secret      = google_secret_manager_secret.tmdb_api_key.id
  secret_data = var.tmdb_api_key_secret
}