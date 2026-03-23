# core/cache.py
"""
Módulo de caché para Onoruame.
Proporciona caché en memoria y archivo para geocodificación.
"""
import hashlib
import json
import os
import logging
from typing import Optional, Any, Dict, Tuple

logger = logging.getLogger(__name__)


class CacheManager:
    """Gestor de caché para geocodificación (archivo JSON local)."""

    def __init__(self, cache_file: str = "geocode_cache.json"):
        self.cache_file = cache_file
        self.cache = self._cargar_cache()

    def _cargar_cache(self) -> Dict:
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Error cargando caché: {e}")
        return {}

    def guardar_cache(self):
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f)
        except IOError as e:
            logger.error(f"Error guardando caché: {e}")

    def obtener(self, key: str) -> Tuple[bool, Optional[Any]]:
        """Retorna (encontrado, valor)."""
        if key in self.cache:
            return True, self.cache[key]
        return False, None

    def guardar(self, key: str, value: Any):
        self.cache[key] = value
        # Auto-guardar cada 10 inserciones
        if len(self.cache) % 10 == 0:
            self.guardar_cache()

    def generar_key(self, texto: str) -> str:
        return hashlib.md5(texto.encode('utf-8')).hexdigest()

    def limpiar(self):
        self.cache = {}
        if os.path.exists(self.cache_file):
            os.remove(self.cache_file)


class RedisCache:
    """
    Cache Redis para datos volátiles/sesión.
    El geocoding cache vive en PostgreSQL (GeocacheRepo).
    """

    def __init__(self):
        import redis
        from .config import settings
        self.client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            password=settings.REDIS_PASSWORD,
            decode_responses=False,
            socket_connect_timeout=5,
            socket_timeout=5,
        )

    def get(self, key: str) -> Optional[Any]:
        try:
            import pickle
            data = self.client.get(key)
            return pickle.loads(data) if data else None
        except Exception as e:
            logger.warning(f"Redis GET falló para '{key}': {e}")
            return None

    def set(self, key: str, value: Any, ttl: int = 3600):
        try:
            import pickle
            self.client.setex(key, ttl, pickle.dumps(value))
        except Exception as e:
            logger.warning(f"Redis SET falló para '{key}': {e}")

    def delete(self, key: str):
        try:
            self.client.delete(key)
        except Exception as e:
            logger.warning(f"Redis DELETE falló para '{key}': {e}")

    def health_check(self) -> bool:
        try:
            return self.client.ping()
        except Exception:
            return False


# Instancia global de Redis — falla silenciosamente si no está disponible
try:
    cache = RedisCache()
except Exception as e:
    logger.warning(f"Redis no disponible: {e}")
    cache = None
