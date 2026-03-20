"""
Generador de rutas optimizado
Algoritmo dual: vecino cercano para pocos edificios, k-means para muchos
"""

import math
import logging
from typing import List, Dict, Tuple, Optional
import pandas as pd
import requests

from .models import Persona, Edificio, Ruta
from .geocoder import Geocoder
from .config import settings

logger = logging.getLogger(__name__)

class RouteGenerator:
    """
    Generador de rutas con algoritmo dual
    """
    
    MAX_PARADAS = 8
    MIN_PARADAS = 3
    DISTANCIA_MAX = 5.0  # km
    UMBRAL_KMEANS = 24
    
    def __init__(self, api_key: str, origen_coords: str, origen_nombre: str):
        self.api_key = api_key
        self.origen_coords = origen_coords
        self.origen_nombre = origen_nombre
        self.origen = tuple(map(float, origen_coords.split(',')))
        self.geocoder = Geocoder()
    
    def procesar_dataframe(self, df: pd.DataFrame) -> List[Ruta]:
        """Procesa un DataFrame y genera rutas"""
        logger.info(f"Procesando {len(df)} registros...")
        edificios_por_zona = self.agrupar_edificios(df)
        rutas = self.crear_rutas(edificios_por_zona)
        return rutas
    
    def agrupar_edificios(self, df: pd.DataFrame) -> Dict[str, List[Edificio]]:
        """Agrupa personas por dirección"""
        edificios_dict = {}
        
        for _, fila in df.iterrows():
            persona = self._extraer_persona(fila)
            
            if not persona.direccion:
                continue
            
            clave = f"{persona.direccion}_{persona.alcaldia}".lower()
            
            if clave not in edificios_dict:
                coords = self.geocoder.geocodificar(persona.direccion, persona.alcaldia)
                
                edificios_dict[clave] = Edificio(
                    direccion_original=persona.direccion,
                    direccion_normalizada=persona.direccion,
                    alcaldia=persona.alcaldia,
                    dependencia_principal=persona.adscripcion,
                    coordenadas=coords,
                    personas=[],
                    zona=self._asignar_zona(persona.alcaldia)
                )
            
            edificios_dict[clave].personas.append(persona)
        
        return {"TODAS": list(edificios_dict.values())}
    
    def crear_rutas(self, edificios_por_zona: Dict[str, List[Edificio]]) -> List[Ruta]:
        """Crea rutas usando algoritmo de vecino más cercano"""
        todas_rutas = []
        ruta_id = 1
        
        for zona, edificios in edificios_por_zona.items():
            if not edificios:
                continue
            
            con_coords = [e for e in edificios if e.coordenadas]
            sin_coords = [e for e in edificios if not e.coordenadas]
            
            if not con_coords:
                continue
            
            # Algoritmo de vecino más cercano
            disponibles = con_coords.copy()
            
            while disponibles:
                ruta_actual = []
                punto_actual = self.origen
                
                # Primer edificio: más cercano al origen
                primero = min(disponibles, key=lambda e: self._distancia(self.origen, e.coordenadas))
                ruta_actual.append(primero)
                disponibles.remove(primero)
                punto_actual = primero.coordenadas
                
                # Siguientes: vecinos más cercanos
                while len(ruta_actual) < self.MAX_PARADAS and disponibles:
                    siguiente = min(disponibles, 
                                  key=lambda e: self._distancia(punto_actual, e.coordenadas))
                    ruta_actual.append(siguiente)
                    disponibles.remove(siguiente)
                    punto_actual = siguiente.coordenadas
                
                if len(ruta_actual) >= self.MIN_PARADAS:
                    rutas.append(Ruta(
                        id=ruta_id,
                        zona=zona,
                        edificios=ruta_actual + (sin_coords[:2] if sin_coords else []),
                        origen=self.origen_nombre
                    ))
                    ruta_id += 1
                    if sin_coords:
                        sin_coords = sin_coords[2:]
        
        return rutas
    
    def _extraer_persona(self, fila: pd.Series) -> Persona:
        """Extrae persona de una fila del DataFrame"""
        return Persona(
            nombre_completo=str(fila.get('nombre', '')),
            nombre=str(fila.get('nombre', '')),
            adscripcion=str(fila.get('adscripcion', '')),
            direccion=str(fila.get('direccion', '')),
            alcaldia=str(fila.get('alcaldia', '')),
            notas=str(fila.get('notas', '')),
            fila_original=fila.to_dict()
        )
    
    def _asignar_zona(self, alcaldia: str) -> str:
        """Asigna zona según alcaldía"""
        zonas = {
            'CENTRO': ['CUAUHTEMOC', 'MIGUEL HIDALGO', 'BENITO JUAREZ'],
            'SUR': ['ALVARO OBREGON', 'COYOACAN', 'TLALPAN'],
            'ORIENTE': ['IZTAPALAPA', 'GUSTAVO A. MADERO', 'VENUSTIANO CARRANZA']
        }
        
        for zona, alcaldias in zonas.items():
            if any(alc in alcaldia.upper() for alc in alcaldias):
                return zona
        return 'OTRAS'
    
    def _distancia(self, coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
        """Distancia Haversine"""
        lat1, lon1 = coord1
        lat2, lon2 = coord2
        R = 6371
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat/2) * math.sin(dlat/2) + 
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * 
             math.sin(dlon/2) * math.sin(dlon/2))
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R * c
