# variables.tf
variable "gcp_project_id" {
  description = "El ID del proyecto de Google Cloud"
  type        = string
}

variable "gcp_region" {
  description = "Región principal para los recursos (ej. us-central1)"
  type        = string
  default     = "us-central1"
}

variable "data_lake_bucket_name" {
  description = "Nombre único global para el bucket del Data Lake"
  type        = string
}

variable "tmdb_api_key_secret" {
  description = "Valor del API Key de TMDB (inyectado vía variable de entorno TF_VAR_tmdb_api_key_secret)"
  type        = string
  sensitive   = true
}