"""
<<<<<<< HEAD
Módulo de caché para Onoruame
Proporciona caché en memoria y archivo para geocodificación
"""

import hashlib
import json
import os
import logging
from typing import Optional, Any, Dict, Tuple
=======
Módulo de caché simple para Onoruame
"""
>>>>>>> b4464ec (Auto-sync: 2026-03-20 16:59:19)

import hashlib
import json
import os
from typing import Optional, Any, Dict, Tuple

class CacheManager:
    """Gestor de caché para geocodificación"""
    
    def __init__(self, cache_file: str = "geocode_cache.json"):
        self.cache_file = cache_file
        self.cache = self._cargar_cache()
    
    def _cargar_cache(self) -> Dict:
<<<<<<< HEAD
        """Carga el caché desde archivo"""
=======
>>>>>>> b4464ec (Auto-sync: 2026-03-20 16:59:19)
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
<<<<<<< HEAD
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
=======
            except:
                return {}
        return {}
    
    def guardar_cache(self):
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f)
        except:
            pass
    
    def obtener(self, key: str) -> Tuple[bool, Optional[Any]]:
>>>>>>> b4464ec (Auto-sync: 2026-03-20 16:59:19)
        if key in self.cache:
            return True, self.cache[key]
        return False, None
    
    def guardar(self, key: str, value: Any):
<<<<<<< HEAD
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
=======
        self.cache[key] = value
        self.guardar_cache()
    
    def generar_key(self, texto: str) -> str:
        return hashlib.md5(texto.encode('utf-8')).hexdigest()
>>>>>>> b4464ec (Auto-sync: 2026-03-20 16:59:19)
