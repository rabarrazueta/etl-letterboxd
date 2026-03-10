-- Archivo: dataplex/bq_setup/create_dq_results_dataset.sql
-- Propósito: Crear el entorno analítico para los metadatos de calidad de datos.

CREATE SCHEMA IF NOT EXISTS `raspberry-9dcf6.dq_results`
OPTIONS (
  description = 'Dataset dedicado a almacenar los resultados históricos de los escaneos de Google Cloud Dataplex Auto DQ.',
  location = 'us-central1',
  labels = [('environment', 'production'), ('domain', 'data_governance')]
);

-- Nota: Las tablas internas (ej. fact_diary_entries_quality) 
-- serán creadas e insertadas automáticamente por la API de Dataplex 
-- durante la ejecución del job de Data Quality. No requieren DDL manual.