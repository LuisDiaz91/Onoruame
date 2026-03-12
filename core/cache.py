"""
Módulo de caché para Onoruame
Proporciona caché en memoria y archivo para geocodificación
"""

import hashlib
import json
import os
import logging
from typing import Optional, Any, Dict, Tuple

logger = logging.getLogger(__name__)

class CacheManager:
    """Gestor de caché para geocodificación"""
    
    def __init__(self, cache_file: str = "geocode_cache.json"):
        self.cache_file = cache_file
        self.cache = self._cargar_cache()
    
    def _cargar_cache(self) -> Dict:
        """Carga el caché desde archivo"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Error cargando caché: {e}")
        return {}
    
    def guardar_cache(self):
        """Guarda el caché en archivo"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f)
        except IOError as e:
            logger.error(f"Error guardando caché: {e}")
    
    def obtener(self, key: str) -> Tuple[bool, Optional[Any]]:
        """
        Obtiene un valor del caché
        Retorna (encontrado, valor)
        """
        if key in self.cache:
            return True, self.cache[key]
        return False, None
    
    def guardar(self, key: str, value: Any):
        """Guarda un valor en el caché"""
        self.cache[key] = value
        # Auto-guardar después de cada inserción (opcional)
        if len(self.cache) % 10 == 0:  # Guardar cada 10 inserciones
            self.guardar_cache()
    
    def generar_key(self, texto: str) -> str:
        """Genera una key hash para el texto"""
        return hashlib.md5(texto.encode('utf-8')).hexdigest()
    
    def limpiar(self):
        """Limpia todo el caché"""
        self.cache = {}
        if os.path.exists(self.cache_file):
            os.remove(self.cache_file)
