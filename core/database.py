# core/database.py
"""
Manejador de base de datos PostgreSQL para Onoruame
"""
import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager

from .config import settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    _instance = None
    _pool     = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_pool()
        return cls._instance

    def _init_pool(self):
        try:
            self._pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                database=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
            )
            logger.info("✅ Pool de conexiones PostgreSQL creado")
        except Exception as e:
            logger.error(f"❌ Error creando pool: {e}")
            self._pool = None

    @contextmanager
    def get_connection(self):
        if self._pool is None:
            self._init_pool()
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    @contextmanager
    def get_cursor(self):
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            try:
                yield cursor
            finally:
                cursor.close()

    def execute(self, query: str, params: tuple = None):
        with self.get_cursor() as cursor:
            cursor.execute(query, params or ())
            if cursor.description:
                return cursor.fetchall()
            return None

    def health_check(self) -> bool:
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Health check falló: {e}")
            return False

    def init_schema(self) -> bool:
        """Ejecuta scripts/init_db.sql para crear las tablas."""
        schema_path = os.path.join(
            os.path.dirname(__file__), '..', 'scripts', 'init_db.sql'
        )
        if not os.path.exists(schema_path):
            logger.error(f"❌ No se encontró {schema_path}")
            return False
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                sql = f.read()
            with self.get_cursor() as cursor:
                cursor.execute(sql)
            logger.info("✅ Schema aplicado correctamente")
            return True
        except Exception as e:
            logger.error(f"❌ Error ejecutando schema: {e}")
            return False


# Instancia global — importar con: from core.database import db
db = DatabaseManager()
