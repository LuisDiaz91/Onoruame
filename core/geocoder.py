import requests
import hashlib
import logging
from typing import Optional, Tuple
from .config import settings
from .cache import RedisCache

logger = logging.getLogger(__name__)

class Geocoder:
    def __init__(self):
        self.api_key = settings.GOOGLE_MAPS_API_KEY
        self.cache = RedisCache()
    
    def geocodificar(self, direccion: str) -> Optional[Tuple[float, float]]:
        if not direccion:
            return None
        
        # Intentar caché
        cache_key = f"geocode:{hashlib.md5(direccion.encode()).hexdigest()}"
        cached = self.cache.get(cache_key)
        if cached:
            return tuple(cached)
        
        # Llamar a Google Maps
        try:
            url = "https://maps.googleapis.com/maps/api/geocode/json"
            params = {
                'address': direccion,
                'key': self.api_key,
                'region': 'mx'
            }
            
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data['status'] == 'OK' and data['results']:
                loc = data['results'][0]['geometry']['location']
                coords = (loc['lat'], loc['lng'])
                
                # Guardar en caché por 24 horas
                self.cache.set(cache_key, coords, ttl=86400)
                return coords
                
        except Exception as e:
            logger.error(f"Error geocodificando: {e}")
        
        return None
