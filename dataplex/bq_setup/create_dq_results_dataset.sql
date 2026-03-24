-- Archivo: dataplex/bq_setup/create_dq_results_dataset.sql
-- Propósito: Crear el entorno analítico para los metadatos de calidad de datos.
-- Reemplaza YOUR_GCP_PROJECT_ID por tu Project ID real antes de ejecutar.

CREATE SCHEMA IF NOT EXISTS `YOUR_GCP_PROJECT_ID.dq_results`
OPTIONS (
  description = 'Dataset dedicado a almacenar los resultados históricos de los escaneos de Google Cloud Dataplex Auto DQ.',
  location = 'YOUR_BQ_LOCATION',
  labels = [('environment', 'production'), ('domain', 'data_governance')]
);

-- Nota: Las tablas internas (ej. fact_diary_entries_quality) 
-- serán creadas e insertadas automáticamente por la API de Dataplex 
-- durante la ejecución del job de Data Quality. No requieren DDL manual.