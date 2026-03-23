# worker/__init__.py
from .celery_app import celery

__all__ = ["celery"]
