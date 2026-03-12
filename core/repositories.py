cat > core/repositories.py << 'EOF'
"""
Repositorios para acceso a base de datos
Proporciona métodos CRUD para las entidades del sistema
"""

from .database import db
import logging

logger = logging.getLogger(__name__)

class RutaRepo:
    """Repositorio para operaciones con rutas"""
    
    @staticmethod
    def list_all(estado=None):
        """Lista todas las rutas, opcionalmente filtradas por estado"""
        with db.get_cursor() as cursor:
            if estado:
                cursor.execute("""
                    SELECT r.*, COUNT(p.id) as total_paradas, 
                           SUM(p.total_personas) as total_personas
                    FROM rutas r
                    LEFT JOIN paradas p ON r.id = p.ruta_id
                    WHERE r.estado = %s
                    GROUP BY r.id
                    ORDER BY r.id DESC
                """, (estado,))
            else:
                cursor.execute("""
                    SELECT r.*, COUNT(p.id) as total_paradas, 
                           SUM(p.total_personas) as total_personas
                    FROM rutas r
                    LEFT JOIN paradas p ON r.id = p.ruta_id
                    GROUP BY r.id
                    ORDER BY r.id DESC
                """)
            return cursor.fetchall()
    
    @staticmethod
    def cambiar_estado(ruta_id, nuevo_estado):
        """Cambia el estado de una ruta"""
        with db.get_cursor() as cursor:
            cursor.execute(
                "UPDATE rutas SET estado = %s WHERE id = %s",
                (nuevo_estado, ruta_id)
            )
    
    @staticmethod
    def asignar(ruta_id, repartidor_id):
        """Asigna una ruta a un repartidor"""
        with db.get_cursor() as cursor:
            cursor.execute(
                "UPDATE rutas SET repartidor_id = %s, estado = 'asignada' WHERE id = %s",
                (repartidor_id, ruta_id)
            )
    
    @staticmethod
    def crear_desde_generador(ruta):
        """Guarda una ruta generada en la base de datos"""
        # Esta función se implementará después
        pass


class RepartidorRepo:
    """Repositorio para operaciones con repartidores"""
    
    @staticmethod
    def list_all():
        """Lista todos los repartidores"""
        with db.get_cursor() as cursor:
            cursor.execute("SELECT * FROM repartidores ORDER BY nombre")
            return cursor.fetchall()
    
    @staticmethod
    def create(nombre, telefono=None, telegram_id=None):
        """Crea un nuevo repartidor"""
        with db.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO repartidores (nombre, telefono, telegram_id)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (nombre, telefono, telegram_id))
            return cursor.fetchone()['id']


class AvanceRepo:
    """Repositorio para operaciones con avances/entregas"""
    
    @staticmethod
    def pendientes():
        """Lista avances pendientes"""
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT a.*, p.nombre as persona_nombre, r.nombre as repartidor_nombre
                FROM avances a
                LEFT JOIN personas p ON a.persona_id = p.id
                LEFT JOIN repartidores r ON a.repartidor_id = r.id
                WHERE a.estado = 'pendiente'
                ORDER BY a.creado_en DESC
            """)
            return cursor.fetchall()
    
    @staticmethod
    def marcar_procesado(avance_id):
        """Marca un avance como procesado"""
        with db.get_cursor() as cursor:
            cursor.execute(
                "UPDATE avances SET estado = 'procesado' WHERE id = %s",
                (avance_id,)
            )


class GeocacheRepo:
    """Repositorio para caché de geocodificación"""
    
    @staticmethod
    def stats():
        """Obtiene estadísticas del caché"""
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN exitoso THEN 1 ELSE 0 END) as exitosos,
                    SUM(CASE WHEN NOT exitoso THEN 1 ELSE 0 END) as fallidos
                FROM geocache
            """)
            return cursor.fetchone()
EOF
