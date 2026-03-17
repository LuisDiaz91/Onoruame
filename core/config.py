from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # Database
    DB_HOST: str = "postgres"
    DB_PORT: int = 5432
    DB_NAME: str = "rutas_db"
    DB_USER: str = "rutas_user"
    DB_PASSWORD: str = "rutas_password"
    
    # Redis
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: Optional[str] = None
    
    # Google Maps
    GOOGLE_MAPS_API_KEY: str
    
    # Origen
    ORIGEN_COORDS: str = "19.419934402889545, -99.15019273007405"
    ORIGEN_NOMBRE: str = "TSJCDMX - Niños Héroes 150"
    
    # Configuración rutas
    MAX_EDIFICIOS_POR_RUTA: int = 8
    
    # Cache
    CACHE_FILE: str = "geocode_cache.json"
    
    # API
    SECRET_KEY: str = "super-secret-key"
    
    class Config:
        env_file = ".env"

settings = Settings()
