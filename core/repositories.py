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
    def get(ruta_id):
        """Obtiene una ruta por su ID"""
        with db.get_cursor() as cursor:
            cursor.execute("SELECT * FROM rutas WHERE id = %s", (ruta_id,))
            return cursor.fetchone()
    
    @staticmethod
    def get_full(ruta_id):
        """Obtiene una ruta con todas sus paradas y personas"""
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT r.*, 
                       json_agg(json_build_object(
                           'direccion_original', p.direccion_original,
                           'personas', (
                               SELECT json_agg(json_build_object(
                                   'nombre', pe.nombre,
                                   'estado', pe.estado
                               ))
                               FROM personas pe
                               WHERE pe.parada_id = p.id
                           )
                       )) as paradas
                FROM rutas r
                LEFT JOIN paradas p ON r.id = p.ruta_id
                WHERE r.id = %s
                GROUP BY r.id
            """, (ruta_id,))
            return cursor.fetchone()
    
    @staticmethod
    def cambiar_estado(ruta_id, nuevo_estado):
        """Cambia el estado de una ruta"""
        with db.get_cursor() as cursor:
            cursor.execute(
                "UPDATE rutas SET estado = %s WHERE id = %s",
                (nuevo_estado, ruta_id)
            )
    
    @staticmethod
    def list_by_estado(estado):
        """Lista rutas por estado"""
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT r.*, COUNT(p.id) as total_paradas
                FROM rutas r
                LEFT JOIN paradas p ON r.id = p.ruta_id
                WHERE r.estado = %s
                GROUP BY r.id
                ORDER BY r.id DESC
            """, (estado,))
            return cursor.fetchall()
    
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
    def list_activos():
        """Lista repartidores activos"""
        with db.get_cursor() as cursor:
            cursor.execute("SELECT * FROM repartidores WHERE activo = true ORDER BY nombre")
            return cursor.fetchall()
    
    @staticmethod
    def create(nombre, telefono=None, telegram_id=None):
        """Crea un nuevo repartidor"""
        with db.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO repartidores (nombre, telefono, telegram_id, activo)
                VALUES (%s, %s, %s, true)
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
                LIMIT 200
            """)
            return cursor.fetchall()
    
    @staticmethod
    def list_all(limit=200):
        """Lista todos los avances"""
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT a.*, p.nombre as persona_nombre, r.nombre as repartidor_nombre
                FROM avances a
                LEFT JOIN personas p ON a.persona_id = p.id
                LEFT JOIN repartidores r ON a.repartidor_id = r.id
                ORDER BY a.creado_en DESC
                LIMIT %s
            """, (limit,))
            return cursor.fetchall()
    
    @staticmethod
    def procesados(limit=200):
        """Lista avances procesados"""
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT a.*, p.nombre as persona_nombre, r.nombre as repartidor_nombre
                FROM avances a
                LEFT JOIN personas p ON a.persona_id = p.id
                LEFT JOIN repartidores r ON a.repartidor_id = r.id
                WHERE a.estado = 'procesado'
                ORDER BY a.creado_en DESC
                LIMIT %s
            """, (limit,))
            return cursor.fetchall()
    
    @staticmethod
    def marcar_procesado(avance_id):
        """Marca un avance como procesado"""
        with db.get_cursor() as cursor:
            cursor.execute(
                "UPDATE avances SET estado = 'procesado' WHERE id = %s",
                (avance_id,)
            )
            
class PersonaRepo:
    """Repositorio para operaciones con personas"""
    
    @staticmethod
    def get_by_ruta(ruta_id):
        """Obtiene todas las personas de una ruta"""
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT pe.* 
                FROM personas pe
                JOIN paradas pa ON pe.parada_id = pa.id
                WHERE pa.ruta_id = %s
                ORDER BY pa.orden, pe.sub_orden
            """, (ruta_id,))
            return cursor.fetchall()
    
    @staticmethod
    def marcar_entregado(persona_id, foto_path=None):
        """Marca una persona como entregada"""
        with db.get_cursor() as cursor:
            cursor.execute("""
                UPDATE personas 
                SET estado = 'entregado', 
                    foto_path = COALESCE(%s, foto_path),
                    fecha_entrega = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (foto_path, persona_id))

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
