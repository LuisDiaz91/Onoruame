# worker/celery_app.py
from celery import Celery
from core.config import settings

def make_celery() -> Celery:
    broker_url  = f"redis://:{settings.REDIS_PASSWORD}@{settings.REDIS_HOST}:{settings.REDIS_PORT}/0" \
                  if settings.REDIS_PASSWORD else \
                  f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/0"

    backend_url = broker_url.replace("/0", "/1")   # DB separada para resultados

    app = Celery(
        "onoruame",
        broker  = broker_url,
        backend = backend_url,
    )

    app.conf.update(
        task_serializer         = "json",
        result_serializer       = "json",
        accept_content          = ["json"],
        timezone                = "America/Mexico_City",
        enable_utc              = True,
        task_track_started      = True,
        task_acks_late          = True,          # re-encolar si el worker muere
        worker_prefetch_multiplier = 1,          # una tarea a la vez (geocoding es lento)
        result_expires          = 3600,          # 1 hora
    )

    return app


celery = make_celery()
