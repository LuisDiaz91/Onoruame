"""
Manejador de base de datos PostgreSQL para Onoruame
Proporciona pool de conexiones y métodos de acceso a BD
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
from .config import settings
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Singleton para manejar conexiones a PostgreSQL usando pool
    """
    _instance = None
    _pool = None
    
    def __new__(cls):
        """Implementación del patrón Singleton"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_pool()
        return cls._instance
    
    def _init_pool(self):
        """Inicializa el pool de conexiones usando settings"""
        try:
            self._pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                database=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD
            )
            logger.info("✅ Pool de conexiones PostgreSQL creado")
        except Exception as e:
            logger.error(f"❌ Error creando pool de conexiones: {e}")
            self._pool = None
    
    @contextmanager
    def get_connection(self):
        """
        Obtiene una conexión del pool (context manager)
        Uso:
            with db.get_connection() as conn:
                # usar conexión
        """
        if self._pool is None:
            self._init_pool()
        
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error en conexión: {e}")
            raise
        finally:
            self._pool.putconn(conn)
    
    @contextmanager
    def get_cursor(self):
        """
        Obtiene un cursor directamente (más común)
        Uso:
            with db.get_cursor() as cursor:
                cursor.execute("SELECT * FROM rutas")
                resultados = cursor.fetchall()
        """
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            try:
                yield cursor
            finally:
                cursor.close()
    
    def execute(self, query: str, params: tuple = None):
        """
        Ejecuta una consulta SQL y retorna resultados si existen
        Útil para consultas rápidas
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params or ())
            if cursor.description:
                return cursor.fetchall()
            return None
    
    def execute_many(self, query: str, params_list: list):
        """
        Ejecuta la misma consulta con múltiples parámetros
        Útil para inserts masivos
        """
        with self.get_cursor() as cursor:
            cursor.executemany(query, params_list)
    
    def health_check(self) -> bool:
        """Verifica que la conexión a BD funciona"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception as e:
            logger.error(f"Health check falló: {e}")
            return False
    
    def init_schema(self):
        """
        Inicializa el esquema de la base de datos
        Ejecuta el archivo scripts/init_db.sql
        """
        import os
        schema_path = os.path.join(
            os.path.dirname(__file__), 
            '..', 
            'scripts', 
            'init_db.sql'
        )
        
        if os.path.exists(schema_path):
            try:
                with open(schema_path, 'r') as f:
                    sql = f.read()
                
                # Ejecutar el script SQL completo
                with self.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(sql)
                
                logger.info("✅ Esquema de base de datos inicializado")
                return True
            except Exception as e:
                logger.error(f"❌ Error inicializando esquema: {e}")
                return False
        else:
            logger.warning(f"⚠️ No se encontró {schema_path}")
            return False
    
    def get_stats(self) -> dict:
        """Obtiene estadísticas del pool de conexiones"""
        if self._pool:
            return {
                'min_connections': self._pool.minconn,
                'max_connections': self._pool.maxconn,
                'used_connections': len(self._pool._used),
                'available_connections': len(self._pool._pool)
            }
        return {}


# =============================================================================
# INSTANCIA GLOBAL (SINGLETON)
# =============================================================================
# Esta instancia es la que se importa en otros módulos:
# from core.database import db
db = DatabaseManager()
