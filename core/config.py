# core/config.py
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):

    # ── Base de datos ──────────────────────────────────────────
    DB_HOST:     str = "localhost"
    DB_PORT:     int = 5432
    DB_NAME:     str = "onoruame"
    DB_USER:     str = "postgres"
    DB_PASSWORD: str = "postgres"

    # ── Redis / Celery ─────────────────────────────────────────
    REDIS_HOST:     str           = "localhost"
    REDIS_PORT:     int           = 6379
    REDIS_PASSWORD: Optional[str] = None

    # ── Google Maps ────────────────────────────────────────────
    GOOGLE_MAPS_API_KEY: str = ""
    TIMEOUT_API:         int = 10   # segundos — usado por geocoder

    # ── Origen ─────────────────────────────────────────────────
    ORIGEN_COORDS: str = "19.4283717,-99.1430307"
    ORIGEN_NOMBRE: str = "TSJCDMX - Niños Héroes 150"

    # ── Rutas ──────────────────────────────────────────────────
    MAX_EDIFICIOS_POR_RUTA: int = 8

    # ── API ────────────────────────────────────────────────────
    SECRET_KEY: str = "cambia-esto-en-produccion"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"   # ignora variables del .env que no estén aquí


settings = Settings()
