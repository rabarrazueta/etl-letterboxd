# ingestion/storage.py
import os
import fsspec
from typing import List

class StorageManager:
    """
    Abstracción agnóstica de infraestructura para el sistema de archivos.
    Utiliza fsspec para enrutar el tráfico hacia GCS (Fase 1) o Local (Fase 2)
    basado puramente en variables de entorno.
    """
    def __init__(self, backend: str, bucket_name: str, local_path: str):
        self.backend = backend.lower()
        self.bucket_name = bucket_name
        self.local_path = local_path.rstrip("/")
        
        # Configuración dinámica del filesystem base
        if self.backend == "gcs":
            self.base_uri = f"gs://{self.bucket_name}"
            # Requiere que GOOGLE_APPLICATION_CREDENTIALS esté en el entorno
            self.fs = fsspec.filesystem("gcs") 
            print(f"🔧 StorageManager inicializado en modo CLOUD (GCS: {self.base_uri})")
        elif self.backend == "local":
            # Para la futura Fase 2 en DigitalOcean
            self.base_uri = f"{self.local_path}/lake"
            self.fs = fsspec.filesystem("file")
            # Asegurar que el directorio base exista localmente
            os.makedirs(self.base_uri, exist_ok=True)
            print(f"🔧 StorageManager inicializado en modo LOCAL (Dir: {self.base_uri})")
        else:
            raise ValueError(f"Backend de storage no soportado: {self.backend}")

    def _build_full_path(self, relative_path: str) -> str:
        """Construye la URI final dependiendo del backend."""
        clean_path = relative_path.lstrip("/")
        return f"{self.base_uri}/{clean_path}"

    def upload_file(self, local_source_path: str, remote_destination_path: str) -> None:
        """
        Sube un archivo local al destino configurado.
        Garantiza que la subida sea unívoca.
        """
        full_remote_uri = self._build_full_path(remote_destination_path)
        
        try:
            # fsspec.put maneja internamente la subida a GCS o la copia local
            self.fs.put(local_source_path, full_remote_uri)
            print(f"✅ Subida exitosa: {local_source_path} -> {full_remote_uri}")
        except Exception as e:
            print(f"❌ Error crítico subiendo {local_source_path}: {str(e)}")
            raise e