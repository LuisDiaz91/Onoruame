# worker/tasks.py
"""
Tareas Celery.

Uso desde Flask:
    from worker.tasks import generar_rutas_task
    result = generar_rutas_task.delay(registros)   # registros = list[dict]
    task_id = result.id

Consultar estado:
    GET /api/tareas/<task_id>
"""

import logging
from typing import Any, Dict, List

import pandas as pd

from .celery_app import celery
from core.route_generator import RouteGenerator
from core.repositories    import GeocacheRepo

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tarea principal: generar rutas
# ---------------------------------------------------------------------------

@celery.task(
    bind           = True,
    name           = "onoruame.generar_rutas",
    max_retries    = 2,
    default_retry_delay = 30,
    soft_time_limit = 600,   # 10 min warning
    time_limit      = 700,   # 11.6 min hard kill
)
def generar_rutas_task(self, registros: List[Dict[str, Any]]) -> Dict:
    """
    Recibe lista de dicts (filas del Excel ya parseadas) y genera rutas.

    Args:
        registros: [{'nombre': ..., 'direccion': ..., 'alcaldia': ..., ...}]

    Returns:
        {'rutas_generadas': int, 'total_paradas': int, 'total_personas': int}
    """
    try:
        logger.info(f"[{self.request.id}] Iniciando geocoding de {len(registros)} registros")

        # Actualizar estado visible desde la API
        self.update_state(state="PROGRESS", meta={"paso": "geocoding", "total": len(registros)})

        df = pd.DataFrame(registros)
        generator = RouteGenerator()

        # Fase 1: agrupar (incluye geocoding)
        edificios_por_zona = generator.agrupar_edificios(df)
        total_edificios = sum(len(v) for v in edificios_por_zona.values())
        logger.info(f"[{self.request.id}] {total_edificios} edificios únicos detectados")

        self.update_state(state="PROGRESS", meta={"paso": "optimizando_rutas", "edificios": total_edificios})

        # Fase 2: crear rutas (llama Google Directions)
        rutas = generator.crear_rutas(edificios_por_zona)

        self.update_state(state="PROGRESS", meta={"paso": "guardando_en_db", "rutas": len(rutas)})

        # Fase 3: persistir en PostgreSQL
        generator.persistir_en_db(rutas)

        resultado = {
            "rutas_generadas": len(rutas),
            "total_paradas":   sum(r.total_edificios for r in rutas),
            "total_personas":  sum(r.total_personas  for r in rutas),
            "por_zona": {
                zona: sum(1 for r in rutas if r.zona == zona)
                for zona in {r.zona for r in rutas}
            },
        }
        logger.info(f"[{self.request.id}] ✅ {resultado}")
        return resultado

    except Exception as exc:
        logger.exception(f"[{self.request.id}] Error en generar_rutas_task")
        raise self.retry(exc=exc)


# ---------------------------------------------------------------------------
# Tarea secundaria: limpiar caché de geocoding
# ---------------------------------------------------------------------------

@celery.task(name="onoruame.limpiar_geocache")
def limpiar_geocache_task(days: int = 30) -> Dict:
    GeocacheRepo.cleanup(days)
    stats = GeocacheRepo.stats()
    logger.info(f"Geocache limpiado (>{days} días). Stats: {stats}")
    return {"ok": True, "stats": stats}


# ---------------------------------------------------------------------------
# Beat schedule (tareas programadas)
# ---------------------------------------------------------------------------

celery.conf.beat_schedule = {
    "limpiar-geocache-semanal": {
        "task":     "onoruame.limpiar_geocache",
        "schedule": 60 * 60 * 24 * 7,   # cada 7 días
        "args":     (30,),
    },
}
