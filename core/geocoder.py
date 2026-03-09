"""
Geocodificador optimizado para CDMX
Correcciones:
- Normalización antes de hash (caché más efectivo)
- Manejo de errores de API
- Bug de inicialización corregido
"""

import hashlib
import logging
import time
from typing import Optional, Tuple, Dict, Any
import requests
import pandas as pd
import re

from .config import settings
from .cache import CacheManager

logger = logging.getLogger(__name__)

class Geocoder:
    """Geocodificador optimizado para CDMX con caché y manejo de errores"""
    
    # Patrones de normalización
    NORMALIZACIONES = {
        r'\bAv\.?\b': 'Avenida',
        r'\bBlvd\.?\b': 'Boulevard',
        r'\bCol\.?\b': 'Colonia',
        r'\bDel\.?\b': 'Delegación',
        r'\bAlc\.?\b': 'Alcaldía',
        r'\bEdif\.?\b': 'Edificio',
        r'\bP\.?\s*iso\b': 'Piso',
        r'\bInt\.?\b': 'Interior',
        r'\bS\/?N\b': 'S/N',
        r'\bCto\.?\b': 'Circuito',
        r'\bPte\.?\b': 'Poniente',
        r'\bS\.?\b': 'Sur',
        r'\bN\.?\b': 'Norte',
        r'\bE\.?\b': 'Oriente',
        r'\bO\.?\b': 'Poniente'
    }
    
    def __init__(self):
        """Inicializa geocodificador con API key desde settings"""
        self.api_key = settings.GOOGLE_MAPS_API_KEY
        self.cache = CacheManager(settings.CACHE_FILE)
        self.stats = {'exactas': 0, 'aproximadas': 0, 'fallos': 0}
        self.max_retries = 3
        self.retry_delay = 2  # segundos
        
        if not self.api_key:
            logger.error("❌ GOOGLE_MAPS_API_KEY no configurada")
    
    def _get_cache_key(self, direccion: str, alcaldia: str = "") -> str:
        """
        Genera key de caché NORMALIZANDO primero.
        Esto evita duplicados por diferencias de mayúsculas/minúsculas.
        """
        texto = f"{direccion}_{alcaldia}".strip().lower()
        texto = re.sub(r'\s+', ' ', texto)  # Normalizar espacios
        return hashlib.md5(texto.encode('utf-8')).hexdigest()
    
    def geocodificar(self, direccion: str, alcaldia: str = "") -> Optional[Tuple[float, float]]:
        """
        Geocodifica una dirección con múltiples estrategias y caché.
        """
        if not direccion or (isinstance(direccion, float) and pd.isna(direccion)):
            return None
        
        key = self._get_cache_key(direccion, alcaldia)
        
        # Intentar caché primero
        encontrado, cached = self.cache.obtener(key)
        if encontrado:
            if cached:
                logger.debug(f"Cache hit: {direccion[:30]}...")
                return tuple(cached) if cached else None
            else:
                # Cache guardó None (fallo previo)
                return None
        
        # Estrategia 1: Dirección completa
        coords = self._geocode_api(direccion)
        if coords:
            self.stats['exactas'] += 1
            self.cache.guardar(key, coords)
            logger.debug(f"Geocoding exacto: {direccion[:30]}...")
            return coords
        
        # Estrategia 2: Con alcaldía
        if alcaldia:
            coords = self._geocode_api(f"{direccion}, Alcaldía {alcaldia}, Ciudad de México")
            if coords:
                self.stats['aproximadas'] += 1
                self.cache.guardar(key, coords)
                logger.debug(f"Geocoding con alcaldía: {direccion[:30]}...")
                return coords
        
        # Estrategia 3: Calle principal
        calle = self._extraer_calle(direccion)
        if calle and calle != direccion:
            coords = self._geocode_api(f"{calle}, Ciudad de México")
            if coords:
                self.stats['aproximadas'] += 1
                self.cache.guardar(key, coords)
                logger.debug(f"Geocoding solo calle: {calle[:30]}...")
                return coords
        
        # Fallo total
        self.stats['fallos'] += 1
        self.cache.guardar(key, None)  # Guardamos None para no reintentar
        logger.warning(f"❌ No se pudo geocodificar: {direccion[:50]}...")
        return None
    
    def _geocode_api(self, direccion: str) -> Optional[Tuple[float, float]]:
        """
        Llama a Google Maps API con manejo de errores y reintentos.
        """
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'address': direccion,
            'key': self.api_key,
            'region': 'mx',
            'components': 'country:MX'
        }
        
        for intento in range(self.max_retries):
            try:
                response = requests.get(
                    url, 
                    params=params, 
                    timeout=settings.TIMEOUT_API
                )
                data = response.json()
                status = data.get('status')
                
                if status == 'OK' and data.get('results'):
                    loc = data['results'][0]['geometry']['location']
                    time.sleep(0.1)  # Rate limiting
                    return (loc['lat'], loc['lng'])
                
                elif status == 'OVER_QUERY_LIMIT':
                    logger.warning(f"Límite de API excedido, esperando {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                    continue  # Reintentar
                
                elif status == 'REQUEST_DENIED':
                    logger.error("API key inválida o sin permisos")
                    break  # No reintentar
                
                elif status == 'ZERO_RESULTS':
                    logger.debug(f"Sin resultados para: {direccion[:50]}...")
                    break
                
                else:
                    logger.warning(f"Estado inesperado: {status}")
                    break
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout en intento {intento + 1}")
                if intento < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
            except Exception as e:
                logger.error(f"Error en geocode API: {e}")
                break
        
        return None
    
    def _extraer_calle(self, direccion: str) -> str:
        """Extrae solo el nombre de la calle"""
        d = str(direccion)
        
        # Eliminar números y detalles
        d = re.sub(r'#\s*\d+.*$', '', d)
        d = re.sub(r'No\.?\s*\d+.*$', '', d)
        d = re.sub(r'Núm\.?\s*\d+.*$', '', d)
        d = re.sub(r'\bPiso\s*\d+.*$', '', d, flags=re.IGNORECASE)
        d = re.sub(r'\bInt\.?\s*\w+.*$', '', d, flags=re.IGNORECASE)
        
        return d.strip()
    
    def normalizar_direccion(self, direccion: str) -> str:
        """Normaliza dirección para agrupamiento"""
        if not direccion:
            return ""
        
        d = str(direccion).strip()
        
        # Eliminar códigos postales
        d = re.sub(r'C\.?P\.?\s*\d{5}', '', d, flags=re.IGNORECASE)
        d = re.sub(r'CÓDIGO POSTAL\s*\d{5}', '', d, flags=re.IGNORECASE)
        
        # Eliminar referencias a CDMX
        d = re.sub(r'Ciudad de México|CDMX|Ciudad de Méx\.?', '', d, flags=re.IGNORECASE)
        
        # Normalizar abreviaturas
        for patron, reemplazo in self.NORMALIZACIONES.items():
            d = re.sub(patron, reemplazo, d, flags=re.IGNORECASE)
        
        # Normalizar números para agrupamiento
        match = re.search(r'(.+?)(?:#\s*(\d+)|No\.?\s*(\d+)|Núm\.?\s*(\d+)|\s+(\d+)\b)', d)
        if match:
            calle = match.group(1).strip()
            numero = next((g for g in match.groups()[1:] if g), '')
            if numero and len(numero) >= 3:
                numero_grupo = numero[:-2] + "00"
                return f"{calle} #{numero_grupo}"
        
        return re.sub(r'\s+', ' ', d).strip()
    
    def log_stats(self):
        """Registra estadísticas de geocodificación"""
        logger.info(f"📊 Geocoding stats: "
                    f"Exactas={self.stats['exactas']}, "
                    f"Aprox={self.stats['aproximadas']}, "
                    f"Fallos={self.stats['fallos']}")
