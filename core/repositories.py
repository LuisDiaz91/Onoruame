# core/repositories.py
"""
Repositorios para acceso a base de datos.
Toda operación de BD pasa por aquí — ni la API ni la GUI tocan SQL directamente.
"""
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from .database import db

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# RUTAS
# ─────────────────────────────────────────────────────────────

class RutaRepo:

    @staticmethod
    def list_all(estado: Optional[str] = None) -> List[Dict]:
        with db.get_cursor() as cur:
            if estado:
                cur.execute("""
                    SELECT r.*,
                           r.total_paradas,
                           r.total_personas,
                           rep.nombre AS repartidor_nombre
                    FROM rutas r
                    LEFT JOIN repartidores rep ON r.repartidor_id = rep.id
                    WHERE r.estado = %s
                    ORDER BY r.id DESC
                """, (estado,))
            else:
                cur.execute("""
                    SELECT r.*,
                           r.total_paradas,
                           r.total_personas,
                           rep.nombre AS repartidor_nombre
                    FROM rutas r
                    LEFT JOIN repartidores rep ON r.repartidor_id = rep.id
                    ORDER BY r.id DESC
                """)
            return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def list_by_estado(estado: str) -> List[Dict]:
        return RutaRepo.list_all(estado)

    @staticmethod
    def get(ruta_id: int) -> Optional[Dict]:
        with db.get_cursor() as cur:
            cur.execute("SELECT * FROM rutas WHERE id = %s", (ruta_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    @staticmethod
    def get_full(ruta_id: int) -> Optional[Dict]:
        """Ruta + paradas + personas en una sola consulta."""
        ruta = RutaRepo.get(ruta_id)
        if not ruta:
            return None
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT p.*,
                       COALESCE(
                           json_agg(
                               json_build_object(
                                   'id',              pe.id,
                                   'sub_orden',       pe.sub_orden,
                                   'nombre',          pe.nombre,
                                   'nombre_completo', pe.nombre_completo,
                                   'adscripcion',     pe.adscripcion,
                                   'alcaldia',        pe.alcaldia,
                                   'estado',          pe.estado,
                                   'notas',           pe.notas
                               ) ORDER BY pe.sub_orden
                           ) FILTER (WHERE pe.id IS NOT NULL),
                           '[]'
                       ) AS personas
                FROM paradas p
                LEFT JOIN personas pe ON pe.parada_id = p.id
                WHERE p.ruta_id = %s
                GROUP BY p.id
                ORDER BY p.orden
            """, (ruta_id,))
            ruta["paradas"] = [dict(r) for r in cur.fetchall()]
        return ruta

    @staticmethod
    def cambiar_estado(ruta_id: int, nuevo_estado: str):
        with db.get_cursor() as cur:
            cur.execute(
                "UPDATE rutas SET estado = %s WHERE id = %s",
                (nuevo_estado, ruta_id)
            )

    @staticmethod
    def asignar(ruta_id: int, repartidor_id: str):
        with db.get_cursor() as cur:
            cur.execute(
                "UPDATE rutas SET repartidor_id = %s, estado = 'asignada' WHERE id = %s",
                (repartidor_id, ruta_id)
            )

    @staticmethod
    def create(zona: str, origen_nombre: str, origen_coords: str,
               ruta_hash: str = "") -> int:
        with db.get_cursor() as cur:
            cur.execute("""
                INSERT INTO rutas (zona, origen_nombre, origen_coords, ruta_hash)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (zona, origen_nombre, origen_coords, ruta_hash))
            return cur.fetchone()["id"]

    @staticmethod
    def update_metricas(ruta_id: int, distancia_km: float, tiempo_min: int,
                        polyline_data: str, google_maps_url: str,
                        total_paradas: int, total_personas: int):
        with db.get_cursor() as cur:
            cur.execute("""
                UPDATE rutas SET
                    distancia_km    = %s,
                    tiempo_min      = %s,
                    polyline_data   = %s,
                    google_maps_url = %s,
                    total_paradas   = %s,
                    total_personas  = %s
                WHERE id = %s
            """, (distancia_km, tiempo_min, polyline_data,
                  google_maps_url, total_paradas, total_personas, ruta_id))

    @staticmethod
    def crear_desde_generador(ruta) -> int:
        """
        Persiste una Ruta (dataclass) completa en la BD.
        Idempotente: si la ruta ya existe (mismo hash de contenido)
        retorna el ID existente sin duplicar.
        """
        import urllib.parse
        import hashlib

        # Hash del contenido para detectar duplicados
        contenido = f"{ruta.zona}_{'_'.join(e.direccion_original for e in ruta.edificios)}"
        ruta_hash = hashlib.md5(contenido.encode()).hexdigest()

        # Verificar si ya existe (evita doble guardado GUI + Celery)
        with db.get_cursor() as cur:
            cur.execute(
                "SELECT id FROM rutas WHERE ruta_hash = %s",
                (ruta_hash,)
            )
            existente = cur.fetchone()
            if existente:
                logger.warning(f"Ruta ya existe con hash {ruta_hash[:8]}… → id={existente['id']}")
                return existente['id']

        # URL Google Maps
        dirs = [e.direccion_original for e in ruta.edificios if e.direccion_original]
        google_maps_url = ""
        if dirs:
            from core.config import settings
            base   = "https://www.google.com/maps/dir/?api=1"
            origen = urllib.parse.quote(f"{settings.ORIGEN_NOMBRE}, Ciudad de México")
            destino = urllib.parse.quote(dirs[-1])
            if len(dirs) == 1:
                google_maps_url = f"{base}&origin={origen}&destination={destino}&travelmode=driving"
            else:
                wps = "|".join(urllib.parse.quote(d) for d in dirs[:-1])
                google_maps_url = f"{base}&origin={origen}&destination={destino}&waypoints={wps}&travelmode=driving"

        from core.config import settings as s
        ruta_id = RutaRepo.create(ruta.zona, s.ORIGEN_NOMBRE, s.ORIGEN_COORDS, ruta_hash)

        total_personas = 0
        for orden, edificio in enumerate(ruta.edificios, 1):
            parada_id = ParadaRepo.create(
                ruta_id               = ruta_id,
                orden                 = orden,
                direccion_original    = edificio.direccion_original,
                direccion_normalizada = edificio.direccion_normalizada,
                alcaldia              = edificio.alcaldia,
                dependencia_principal = edificio.dependencia_principal,
                coords                = edificio.coordenadas,
            )
            for sub, persona in enumerate(edificio.personas, 1):
                PersonaRepo.create(
                    parada_id       = parada_id,
                    sub_orden       = sub,
                    nombre_completo = persona.get('nombre_completo', ''),
                    nombre          = persona.get('nombre', ''),
                    adscripcion     = persona.get('adscripcion', ''),
                    direccion       = persona.get('direccion', ''),
                    alcaldia        = persona.get('alcaldia', ''),
                    notas           = persona.get('notas', ''),
                )
                total_personas += 1

        RutaRepo.update_metricas(
            ruta_id         = ruta_id,
            distancia_km    = ruta.distancia_km,
            tiempo_min      = int(ruta.tiempo_min),
            polyline_data   = ruta.polyline_data,
            google_maps_url = google_maps_url,
            total_paradas   = ruta.total_edificios,
            total_personas  = total_personas,
        )
        logger.info(f"✅ Ruta guardada en DB: id={ruta_id} zona={ruta.zona} "
                    f"paradas={ruta.total_edificios} personas={total_personas}")
        return ruta_id

    @staticmethod
    def resumen() -> List[Dict]:
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT r.id, r.zona, r.estado, r.distancia_km, r.tiempo_min,
                       r.total_paradas, r.total_personas, r.creado_en,
                       rep.nombre AS repartidor
                FROM rutas r
                LEFT JOIN repartidores rep ON rep.id = r.repartidor_id
                ORDER BY r.id
            """)
            return [dict(r) for r in cur.fetchall()]


# ─────────────────────────────────────────────────────────────
# PARADAS
# ─────────────────────────────────────────────────────────────

class ParadaRepo:

    @staticmethod
    def create(ruta_id: int, orden: int, direccion_original: str,
               direccion_normalizada: str, alcaldia: str,
               dependencia_principal: str,
               coords: Optional[Tuple[float, float]]) -> int:
        lat = coords[0] if coords else None
        lng = coords[1] if coords else None
        with db.get_cursor() as cur:
            cur.execute("""
                INSERT INTO paradas
                    (ruta_id, orden, direccion_original, direccion_normalizada,
                     alcaldia, dependencia_principal, lat, lng)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (ruta_id, orden, direccion_original, direccion_normalizada,
                  alcaldia, dependencia_principal, lat, lng))
            return cur.fetchone()["id"]

    @staticmethod
    def cambiar_estado(parada_id: int, estado: str):
        with db.get_cursor() as cur:
            cur.execute(
                "UPDATE paradas SET estado = %s WHERE id = %s",
                (estado, parada_id)
            )


# ─────────────────────────────────────────────────────────────
# PERSONAS
# ─────────────────────────────────────────────────────────────

class PersonaRepo:

    @staticmethod
    def create(parada_id: int, sub_orden: int, nombre_completo: str,
               nombre: str, adscripcion: str, direccion: str,
               alcaldia: str, notas: str = "") -> int:
        with db.get_cursor() as cur:
            cur.execute("""
                INSERT INTO personas
                    (parada_id, sub_orden, nombre_completo, nombre,
                     adscripcion, direccion, alcaldia, notas)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (parada_id, sub_orden, nombre_completo, nombre,
                  adscripcion, direccion, alcaldia, notas))
            return cur.fetchone()["id"]

    @staticmethod
    def get_by_ruta(ruta_id: int) -> List[Dict]:
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT pe.*
                FROM personas pe
                JOIN paradas pa ON pe.parada_id = pa.id
                WHERE pa.ruta_id = %s
                ORDER BY pa.orden, pe.sub_orden
            """, (ruta_id,))
            return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def buscar(nombre: str, ruta_id: Optional[int] = None) -> List[Dict]:
        """Búsqueda fuzzy usando pg_trgm."""
        with db.get_cursor() as cur:
            if ruta_id:
                cur.execute("""
                    SELECT pe.*, pa.ruta_id, pa.id AS parada_id
                    FROM personas pe
                    JOIN paradas pa ON pa.id = pe.parada_id
                    WHERE pa.ruta_id = %s AND pe.nombre %% %s
                    ORDER BY similarity(pe.nombre, %s) DESC
                    LIMIT 5
                """, (ruta_id, nombre, nombre))
            else:
                cur.execute("""
                    SELECT pe.*, pa.ruta_id, pa.id AS parada_id
                    FROM personas pe
                    JOIN paradas pa ON pa.id = pe.parada_id
                    WHERE pe.nombre %% %s
                    ORDER BY similarity(pe.nombre, %s) DESC
                    LIMIT 10
                """, (nombre, nombre))
            return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def cambiar_estado(persona_id: int, estado: str):
        with db.get_cursor() as cur:
            cur.execute(
                "UPDATE personas SET estado = %s WHERE id = %s",
                (estado, persona_id)
            )

    @staticmethod
    def marcar_entregado(persona_id: int, foto_path: Optional[str] = None):
        with db.get_cursor() as cur:
            cur.execute("""
                UPDATE personas
                SET estado = 'entregado',
                    foto_path = COALESCE(%s, foto_path),
                    fecha_entrega = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (foto_path, persona_id))


# ─────────────────────────────────────────────────────────────
# REPARTIDORES
# ─────────────────────────────────────────────────────────────

class RepartidorRepo:

    @staticmethod
    def list_all() -> List[Dict]:
        with db.get_cursor() as cur:
            cur.execute("SELECT * FROM repartidores ORDER BY nombre")
            return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def list_activos() -> List[Dict]:
        with db.get_cursor() as cur:
            cur.execute(
                "SELECT * FROM repartidores WHERE activo = true ORDER BY nombre"
            )
            return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def get(repartidor_id: str) -> Optional[Dict]:
        with db.get_cursor() as cur:
            cur.execute("SELECT * FROM repartidores WHERE id = %s", (repartidor_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    @staticmethod
    def create(nombre: str, telefono: Optional[str] = None,
               telegram_id: Optional[str] = None) -> int:
        with db.get_cursor() as cur:
            cur.execute("""
                INSERT INTO repartidores (nombre, telefono, telegram_id, activo)
                VALUES (%s, %s, %s, true)
                ON CONFLICT (nombre) DO UPDATE
                    SET telefono    = EXCLUDED.telefono,
                        telegram_id = EXCLUDED.telegram_id
                RETURNING id
            """, (nombre, telefono, telegram_id))
            return cur.fetchone()["id"]


# ─────────────────────────────────────────────────────────────
# AVANCES
# ─────────────────────────────────────────────────────────────

class AvanceRepo:

    @staticmethod
    def pendientes() -> List[Dict]:
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT a.*, p.nombre AS persona_nombre,
                       r.nombre AS repartidor_nombre
                FROM avances a
                LEFT JOIN personas     p ON a.persona_id    = p.id
                LEFT JOIN repartidores r ON a.repartidor_id = r.id
                WHERE a.estado = 'pendiente'
                ORDER BY a.creado_en DESC
                LIMIT 200
            """)
            return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def list_all(limit: int = 200) -> List[Dict]:
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT a.*, p.nombre AS persona_nombre,
                       r.nombre AS repartidor_nombre
                FROM avances a
                LEFT JOIN personas     p ON a.persona_id    = p.id
                LEFT JOIN repartidores r ON a.repartidor_id = r.id
                ORDER BY a.creado_en DESC
                LIMIT %s
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def procesados(limit: int = 200) -> List[Dict]:
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT a.*, p.nombre AS persona_nombre,
                       r.nombre AS repartidor_nombre
                FROM avances a
                LEFT JOIN personas     p ON a.persona_id    = p.id
                LEFT JOIN repartidores r ON a.repartidor_id = r.id
                WHERE a.estado = 'procesado'
                ORDER BY a.creado_en DESC
                LIMIT %s
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def marcar_procesado(avance_id) -> None:
        with db.get_cursor() as cur:
            cur.execute(
                "UPDATE avances SET estado = 'procesado', procesado_en = NOW() WHERE id = %s",
                (str(avance_id),)
            )

    @staticmethod
    def create(ruta_id: int, repartidor_id: Optional[str],
               persona_id: Optional[int], parada_id: Optional[int],
               foto_path: str = "", notas: str = "",
               tipo: str = "entrega",
               timestamp_bot: Optional[datetime] = None) -> str:
        with db.get_cursor() as cur:
            cur.execute("""
                INSERT INTO avances
                    (ruta_id, repartidor_id, persona_id, parada_id,
                     foto_path, notas, tipo, timestamp_bot)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (ruta_id, repartidor_id, persona_id, parada_id,
                  foto_path, notas, tipo, timestamp_bot or datetime.utcnow()))
            return str(cur.fetchone()["id"])

    @staticmethod
    def by_ruta(ruta_id: int) -> List[Dict]:
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT a.*, p.nombre AS persona_nombre,
                       r.nombre AS repartidor_nombre
                FROM avances a
                LEFT JOIN personas     p ON a.persona_id    = p.id
                LEFT JOIN repartidores r ON a.repartidor_id = r.id
                WHERE a.ruta_id = %s
                ORDER BY a.creado_en DESC
            """, (ruta_id,))
            return [dict(r) for r in cur.fetchall()]


# ─────────────────────────────────────────────────────────────
# GEOCODING CACHE
# ─────────────────────────────────────────────────────────────

class GeocacheRepo:

    @staticmethod
    def stats() -> Dict:
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                              AS total,
                    COUNT(*) FILTER (WHERE exito = true)  AS exitosos,
                    COUNT(*) FILTER (WHERE exito = false) AS fallidos
                FROM geocoding_cache
            """)
            row = cur.fetchone()
            return dict(row) if row else {'total': 0, 'exitosos': 0, 'fallidos': 0}

    @staticmethod
    def cleanup(days: int = 30):
        with db.get_cursor() as cur:
            cur.execute(
                "DELETE FROM geocoding_cache WHERE usado_en < NOW() - INTERVAL '%s days'",
                (days,)
            )
