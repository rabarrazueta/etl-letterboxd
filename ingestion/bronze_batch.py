# ingestion/bronze_batch.py
import os
# --- PARCHE PARA BLOQUEAR EL ERROR SSL DE WINDOWS/ANTIVIRUS ---
os.environ.pop("SSLKEYLOGFILE", None)
from datetime import datetime
from dotenv import load_dotenv
from ingestion.storage import StorageManager

def run_letterboxd_ingestion():
    """
    Función principal para ingerir archivos locales de Letterboxd 
    hacia la capa Bronze (Raw) del Data Lake, preservando la inmutabilidad.
    """
    print("🚀 Iniciando Pipeline de Ingesta Bronze...")
    
    # 1. Cargar variables de entorno de forma forzada
    # override=True asegura que lea el .env y no variables huérfanas del sistema
    load_dotenv(override=True) 

    backend = os.getenv("STORAGE_BACKEND")
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    local_source_dir = os.getenv("LOCAL_STORAGE_PATH", "./data")

    # Validación de seguridad ("Fail Fast")
    if not backend or not bucket_name:
        raise ValueError("Faltan variables críticas en el archivo .env (STORAGE_BACKEND o GCS_BUCKET_NAME)")

    # 2. Instanciar la capa de abstracción
    storage = StorageManager(
        backend=backend,
        bucket_name=bucket_name,
        local_path=local_source_dir
    )
    
    # 3. Particionamiento Lógico por fecha de ejecución (YYYY-MM-DD)
    # Esto es crucial para la Medallion Architecture en Bronze
    today = datetime.now().strftime("%Y-%m-%d")
    bronze_prefix = f"bronze/letterboxd/ingestion_date={today}"
    
    # 4. Archivos detectados en tu estructura de Letterboxd
    target_files = [
        "diary.csv",
        "ratings.csv",
        "watched.csv",
        "reviews.csv",   # Archivo pesado con texto
        "watchlist.csv"
    ]
    
    print(f"Buscando archivos en origen local: {local_source_dir}")
    print(f"Destino lógico planificado: {bronze_prefix}/...")
    print("-" * 50)
    
    success_count = 0
    
    # 5. Bucle de Ingesta
    for filename in target_files:
        local_file_path = os.path.join(local_source_dir, filename)
        
        if os.path.exists(local_file_path):
            file_size_kb = os.path.getsize(local_file_path) / 1024
            print(f"⏳ Procesando: {filename} ({file_size_kb:.2f} KB)")
            
            # Construir la ruta relativa de destino
            # Ejemplo: bronze/letterboxd/ingestion_date=2026-03-04/diary.csv
            remote_path = f"{bronze_prefix}/{filename}"
            
            # Invocar la abstracción para subir
            storage.upload_file(local_file_path, remote_path)
            success_count += 1
        else:
            print(f"Omitido: No se encontró el archivo '{filename}' en {local_source_dir}")

    print("-" * 50)
    print(f"Ingesta finalizada. {success_count}/{len(target_files)} archivos procesados.")

if __name__ == "__main__":
    run_letterboxd_ingestion()