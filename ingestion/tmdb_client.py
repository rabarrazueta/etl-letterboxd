# ingestion/tmdb_client.py
import os
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from dotenv import load_dotenv

# Cargar .env
load_dotenv()

class TMDBRateLimitError(Exception):
    """Excepción personalizada para errores 429 de TMDB."""
    pass

class TMDBClient:
    """
    Cliente robusto para The Movie Database API.
    Implementa Rate Limiting defensivo y backoff exponencial para evitar bloqueos.
    """
    def __init__(self):
        self.api_key = os.getenv("TMDB_API_KEY")
        if not self.api_key:
            raise ValueError("TMDB_API_KEY no encontrada en las variables de entorno.")
            
        self.base_url = "https://api.themoviedb.org/3"
        
        # httpx Client maneja connection pooling, lo que lo hace más rápido que 'requests'
        self.client = httpx.Client(
            headers={"Authorization": f"Bearer {self.api_key}", "accept": "application/json"},
            timeout=10.0
        )

    # El decorador @retry implementa el backoff exponencial si la API nos bloquea temporalmente
    @retry(
        retry=retry_if_exception_type((TMDBRateLimitError, httpx.RequestError)),
        wait=wait_exponential(multiplier=1, min=2, max=10), # Espera 2s, 4s, 8s...
        stop=stop_after_attempt(5)
    )
    def search_movie(self, title: str, year: int = None) -> dict:
        """
        Busca una película por título y año.
        Retorna el JSON de la API o lanza excepción si hay error de red.
        """
        endpoint = f"{self.base_url}/search/movie"
        params = {"query": title, "include_adult": "false", "language": "en-US"}
        if year:
            params["primary_release_year"] = year

        response = self.client.get(endpoint, params=params)

        # Manejo estricto de códigos HTTP
        if response.status_code == 429:
            raise TMDBRateLimitError(f"Rate limit excedido consultando: {title}")
        
        response.raise_for_status() # Lanza error para 4xx y 5xx
        
        data = response.json()
        if data.get("results"):
            return data["results"][0] # Retorna el mejor match
        return None

    def close(self):
        """Cierra el pool de conexiones."""
        self.client.close()