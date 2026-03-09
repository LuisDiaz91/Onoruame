"""
GENERADOR DE RUTAS - VERSIÓN DUAL (Claude + Tus Modelos)
Características:
- Algoritmo dual: vecino cercano (pocos) / k-means (muchos)
- Guarda en PostgreSQL vía repositories
- Usa tus dataclasses (Persona, Edificio, Ruta)
- Optimizado para 50+ repartidores
- CORRECIONES: Geocoder sin api_key, to_dict('records'), validación Directions API
"""

import math
import logging
from typing import List, Dict, Tuple, Optional
import pandas as pd
import requests

from .models import Persona, Edificio, Ruta
from .geocoder import Geocoder
from .repositories import RutaRepository
from .config import CONFIG

logger = logging.getLogger(__name__)

class RouteGenerator:
    """
    GENERADOR DE RUTAS CON ALGORITMO DUAL:
    - Si hay ≤ 24 edificios: Vecino más cercano (preciso)
    - Si hay > 24 edificios: K-means + Vecino (escalable)
    """
    
    # Colores para mapas (de tu código original)
    COLORES_ZONA = {
        'CENTRO': '#FF6B6B', 'SUR': '#4ECDC4', 'ORIENTE': '#45B7D1',
        'SUR_ORIENTE': '#96CEB4', 'OTRAS': '#FECA57', 'MIXTA': '#9B59B6'
    }
    
    # Zonas de CDMX (completas)
    ZONAS_ALCALDIAS = {
        'CENTRO':   ['CUAUHTEMOC', 'MIGUEL HIDALGO', 'BENITO JUAREZ', 'VENUSTIANO CARRANZA'],
        'SUR':      ['ALVARO OBREGON', 'COYOACAN', 'TLALPAN', 'MAGDALENA CONTRERAS',
                     'XOCHIMILCO', 'MILPA ALTA', 'TLAHUAC'],
        'ORIENTE':  ['IZTAPALAPA', 'IZTACALCO'],
        'NORTE':    ['GUSTAVO A. MADERO', 'AZCAPOTZALCO'],
        'PONIENTE': ['CUAJIMALPA'],
    }
    
    # Parámetros de optimización
    MAX_PARADAS = 8
    MIN_PARADAS = 3
    DISTANCIA_MAX = 5.0  # km - para cortar rutas muy dispersas
    UMBRAL_KMEANS = 24    # >24 edificios → usar k-means
    
    def __init__(self, api_key: str, origen_coords: str, origen_nombre: str):
        self.api_key = api_key
        self.origen_coords = origen_coords
        self.origen_nombre = origen_nombre
        self.origen = tuple(map(float, origen_coords.split(',')))
        # CORRECCIÓN: Geocoder sin pasar api_key (la toma de settings)
        self.geocoder = Geocoder()
    
    # ----------------------------------------------------------------------
    # API PÚBLICA
    # ----------------------------------------------------------------------
    
    def procesar_dataframe(self, df: pd.DataFrame) -> List[Ruta]:
        """
        Punto de entrada principal.
        Recibe DataFrame → retorna rutas (ya guardadas en BD)
        """
        logger.info(f"Procesando {len(df)} registros...")
        
        # 1. Agrupar por edificios
        edificios_por_zona = self.agrupar_edificios(df)
        
        # 2. Crear rutas (algoritmo dual)
        rutas = self.crear_rutas(edificios_por_zona)
        
        # 3. Guardar en PostgreSQL
        for ruta in rutas:
            RutaRepository.crear_desde_generador(ruta)
            logger.info(f"✅ Ruta {ruta.id} guardada en BD")
        
        logger.info(f"🎉 Total: {len(rutas)} rutas generadas")
        return rutas
    
    def agrupar_edificios(self, df: pd.DataFrame) -> Dict[str, List[Edificio]]:
        """
        Agrupa personas por dirección única.
        Cada dirección = un edificio (parada)
        CORRECCIÓN: usa to_dict('records') para mejor performance
        """
        logger.info("Agrupando personas por edificio...")
        edificios_dict: Dict[str, Edificio] = {}
        
        # MEJORA: to_dict('records') es MÁS RÁPIDO que iterrows()
        for fila_dict in df.to_dict('records'):
            # Convertir a Series para compatibilidad con _extraer_persona
            fila = pd.Series(fila_dict)
            persona = self._extraer_persona(fila)
            
            if not persona.direccion or persona.direccion in ['', 'nan']:
                continue
            
            # Normalizar dirección
            dir_norm = self.geocoder.normalizar_direccion(persona.direccion)
            clave = f"{dir_norm}_{persona.alcaldia}".lower()
            
            if clave not in edificios_dict:
                coords = self.geocoder.geocodificar(persona.direccion, persona.alcaldia)
                
                edificios_dict[clave] = Edificio(
                    direccion_original=persona.direccion,
                    direccion_normalizada=dir_norm,
                    alcaldia=persona.alcaldia,
                    dependencia_principal=persona.adscripcion,
                    coordenadas=coords,
                    personas=[],
                    zona=self._asignar_zona(persona.alcaldia)
                )
            
            edificios_dict[clave].personas.append(persona)
        
        # Agrupar por zona
        por_zona: Dict[str, List[Edificio]] = {}
        for edificio in edificios_dict.values():
            por_zona.setdefault(edificio.zona, []).append(edificio)
        
        total_personas = sum(len(e.personas) for e in edificios_dict.values())
        logger.info(f"Edificios: {len(edificios_dict)} | Personas: {total_personas}")
        self.geocoder.log_stats()
        
        return por_zona
    
    def crear_rutas(self, edificios_por_zona: Dict[str, List[Edificio]]) -> List[Ruta]:
        """
        ALGORITMO DUAL:
        - ≤24 edificios: Vecino más cercano
        - >24 edificios: K-means + Vecino
        """
        todas_rutas = []
        ruta_id = 1
        
        for zona, edificios in edificios_por_zona.items():
            if not edificios:
                continue
            
            total = len(edificios)
            logger.info(f"Zona {zona}: {total} edificios")
            
            # Separar con/sin coordenadas
            con_coords = [e for e in edificios if e.coordenadas]
            sin_coords = [e for e in edificios if not e.coordenadas]
            
            # DECISIÓN: ¿Vecino o K-means?
            if total <= self.UMBRAL_KMEANS or len(con_coords) < self.MIN_PARADAS * 2:
                # CASO A: Vecino más cercano (preciso)
                logger.info(f"  Usando Vecino más cercano")
                rutas_zona = self._vecino_mas_cercano(con_coords, zona, ruta_id)
            else:
                # CASO B: K-means + Vecino (escalable)
                k = self._calcular_k(total)
                logger.info(f"  Usando K-means ({k} clusters)")
                
                clusters = self._kmeans_geo(con_coords, k)
                rutas_zona = []
                
                for cluster in clusters:
                    if not cluster:
                        continue
                    sub = self._vecino_mas_cercano(cluster, zona, ruta_id)
                    rutas_zona.extend(sub)
                    if sub:
                        ruta_id = max(r.id for r in sub) + 1
            
            if rutas_zona:
                ruta_id = max(r.id for r in rutas_zona) + 1
            
            # Distribuir edificios sin coordenadas
            if sin_coords:
                if rutas_zona:
                    self._distribuir_sin_coords(rutas_zona, sin_coords)
                else:
                    emergencia = self._rutas_emergencia(sin_coords, zona, ruta_id)
                    todas_rutas.extend(emergencia)
                    if emergencia:
                        ruta_id = max(r.id for r in emergencia) + 1
            
            todas_rutas.extend(rutas_zona)
        
        # Fusionar rutas pequeñas
        rutas_finales = self._fusionar_pequenas(todas_rutas)
        
        # Optimizar con Google Maps (solo si vale la pena)
        for ruta in rutas_finales:
            # CORRECCIÓN: solo llamar si hay más de 3 edificios (ahorra $$)
            if len([e for e in ruta.edificios if e.coordenadas]) >= 3:
                self._optimizar_con_google(ruta)
        
        # Renumerar IDs
        for i, r in enumerate(rutas_finales, 1):
            r.id = i
        
        logger.info(f"✅ {len(rutas_finales)} rutas generadas")
        return rutas_finales
    
    # ----------------------------------------------------------------------
    # ALGORITMOS PRINCIPALES
    # ----------------------------------------------------------------------
    
    def _vecino_mas_cercano(self, edificios: List[Edificio], zona: str, inicio_id: int) -> List[Ruta]:
        """Vecino más cercano con mínimo de paradas"""
        if not edificios:
            return []
        
        disponibles = edificios.copy()
        rutas = []
        ruta_id = inicio_id
        
        while disponibles:
            ruta_actual = []
            punto_actual = self.origen
            
            # Primer edificio: el más cercano al origen
            primero = min(disponibles, key=lambda e: self._haversine(self.origen, e.coordenadas))
            ruta_actual.append(primero)
            disponibles.remove(primero)
            punto_actual = primero.coordenadas
            
            # Siguientes: vecino más cercano al último
            while len(ruta_actual) < self.MAX_PARADAS and disponibles:
                siguiente = min(disponibles, key=lambda e: self._haversine(punto_actual, e.coordenadas))
                distancia = self._haversine(punto_actual, siguiente.coordenadas)
                
                # Si está muy lejos y ya tenemos mínimo, terminar
                if distancia > self.DISTANCIA_MAX and len(ruta_actual) >= self.MIN_PARADAS:
                    break
                
                ruta_actual.append(siguiente)
                disponibles.remove(siguiente)
                punto_actual = siguiente.coordenadas
            
            if len(ruta_actual) >= self.MIN_PARADAS:
                rutas.append(Ruta(
                    id=ruta_id,
                    zona=zona,
                    edificios=ruta_actual,
                    origen=self.origen_nombre
                ))
                ruta_id += 1
            else:
                # No cumple mínimo, regresar y terminar
                disponibles.extend(ruta_actual)
                break
        
        # Reabsorber sobrantes
        if disponibles and rutas:
            for edificio in disponibles:
                for r in rutas:
                    if len(r.edificios) < self.MAX_PARADAS:
                        r.edificios.append(edificio)
                        break
        
        return rutas
    
    def _kmeans_geo(self, edificios: List[Edificio], k: int, max_iter: int = 15) -> List[List[Edificio]]:
        """K-means con distancia Haversine (para CDMX)"""
        if k >= len(edificios):
            return [[e] for e in edificios]
        
        # Inicialización K-means++
        centroides: List[Tuple[float, float]] = []
        restantes = edificios.copy()
        
        # Primer centroide: el más lejano al origen
        primero = max(restantes, key=lambda e: self._haversine(self.origen, e.coordenadas))
        centroides.append(primero.coordenadas)
        restantes.remove(primero)
        
        # Siguientes: los más lejanos a los ya elegidos
        while len(centroides) < k and restantes:
            mas_lejano = max(
                restantes,
                key=lambda e: min(self._haversine(e.coordenadas, c) for c in centroides)
            )
            centroides.append(mas_lejano.coordenadas)
            restantes.remove(mas_lejano)
        
        # Iteraciones
        grupos: List[List[Edificio]] = [[] for _ in range(k)]
        
        for _ in range(max_iter):
            nuevos_grupos: List[List[Edificio]] = [[] for _ in range(k)]
            
            for edificio in edificios:
                distancias = [self._haversine(edificio.coordenadas, c) for c in centroides]
                nuevos_grupos[distancias.index(min(distancias))].append(edificio)
            
            # Recalcular centroides
            nuevos_centroides = []
            for i, grupo in enumerate(nuevos_grupos):
                if grupo:
                    lat = sum(e.coordenadas[0] for e in grupo) / len(grupo)
                    lng = sum(e.coordenadas[1] for e in grupo) / len(grupo)
                    nuevos_centroides.append((lat, lng))
                else:
                    nuevos_centroides.append(centroides[i])
            
            # Verificar convergencia
            convergio = all(
                self._haversine(v, n) < 0.01
                for v, n in zip(centroides, nuevos_centroides)
            )
            
            centroides = nuevos_centroides
            grupos = nuevos_grupos
            
            if convergio:
                break
        
        return [g for g in grupos if g]
    
    # ----------------------------------------------------------------------
    # UTILIDADES
    # ----------------------------------------------------------------------
    
    def _calcular_k(self, total: int) -> int:
        """Número óptimo de clusters"""
        return max(2, min(math.ceil(total / self.MAX_PARADAS), 50))
    
    def _distribuir_sin_coords(self, rutas: List[Ruta], sin_coords: List[Edificio]):
        """Distribuye edificios sin coordenadas equitativamente"""
        rutas_ord = sorted(rutas, key=lambda r: len(r.edificios))
        for i, edificio in enumerate(sin_coords):
            for ruta in rutas_ord:
                if len(ruta.edificios) < self.MAX_PARADAS:
                    ruta.edificios.append(edificio)
                    break
    
    def _rutas_emergencia(self, edificios: List[Edificio], zona: str, inicio_id: int) -> List[Ruta]:
        """Rutas de emergencia (sin coordenadas)"""
        rutas = []
        for i in range(0, len(edificios), self.MAX_PARADAS):
            grupo = edificios[i:i + self.MAX_PARADAS]
            if rutas and len(grupo) < self.MIN_PARADAS:
                if len(rutas[-1].edificios) + len(grupo) <= self.MAX_PARADAS:
                    rutas[-1].edificios.extend(grupo)
                    continue
            rutas.append(Ruta(
                id=inicio_id + len(rutas),
                zona=zona,
                edificios=grupo,
                origen=self.origen_nombre
            ))
        return rutas
    
    def _fusionar_pequenas(self, rutas: List[Ruta]) -> List[Ruta]:
        """Fusiona rutas con menos del mínimo de paradas"""
        por_zona: Dict[str, List[Ruta]] = {}
        for r in rutas:
            por_zona.setdefault(r.zona, []).append(r)
        
        resultado = []
        for zona, zrutas in por_zona.items():
            normales = [r for r in zrutas if len(r.edificios) >= self.MIN_PARADAS]
            pequenas = [r for r in zrutas if len(r.edificios) < self.MIN_PARADAS]
            
            for pq in pequenas:
                fusionada = False
                for nm in normales:
                    if len(nm.edificios) + len(pq.edificios) <= self.MAX_PARADAS:
                        nm.edificios.extend(pq.edificios)
                        fusionada = True
                        break
                if not fusionada:
                    normales.append(pq)
            
            resultado.extend(normales)
        
        return resultado
    
    def _optimizar_con_google(self, ruta: Ruta):
        """
        Optimiza orden con Google Directions API.
        CORRECCIÓN: solo se llama si hay más de 3 edificios (ahorra $$)
        """
        try:
            con_coords = [e for e in ruta.edificios if e.coordenadas]
            if len(con_coords) < 2:
                return
            
            waypoints = "|".join(f"{lat},{lng}" for lat, lng in
                                 [e.coordenadas for e in con_coords])
            
            response = requests.get(
                "https://maps.googleapis.com/maps/api/directions/json",
                params={
                    'origin': self.origen_coords,
                    'destination': self.origen_coords,
                    'waypoints': f"optimize:true|{waypoints}",
                    'key': self.api_key,
                    'language': 'es',
                    'units': 'metric'
                },
                timeout=10
            )
            data = response.json()
            
            if data.get('status') == 'OK' and data['routes']:
                route = data['routes'][0]
                orden = route['waypoint_order']
                
                opt = [con_coords[i] for i in orden]
                sin = [e for e in ruta.edificios if not e.coordenadas]
                ruta.edificios = opt + sin
                
                ruta.distancia_km = sum(l['distance']['value'] for l in route['legs']) / 1000
                ruta.tiempo_min = sum(l['duration']['value'] for l in route['legs']) / 60
                ruta.polyline = route['overview_polyline']['points']
                
                # Generar URL de Google Maps
                ruta.google_maps_url = self._generar_url_maps(ruta)
                
        except Exception as e:
            logger.error(f"Error optimizando ruta {ruta.id}: {e}")
    
    def _generar_url_maps(self, ruta: Ruta) -> str:
        """Genera URL de Google Maps (de tu código original)"""
        try:
            import urllib.parse
            direcciones = [e.direccion_original for e in ruta.edificios if e.direccion_original]
            if len(direcciones) < 2:
                return ""
            
            base = "https://www.google.com/maps/dir/?api=1"
            origen = urllib.parse.quote(f"{ruta.origen}, Ciudad de México")
            destino = urllib.parse.quote(direcciones[-1])
            
            if len(direcciones) > 2:
                waypoints = "|".join(urllib.parse.quote(d) for d in direcciones[1:-1])
                return f"{base}&origin={origen}&destination={destino}&waypoints={waypoints}&travelmode=driving"
            else:
                return f"{base}&origin={origen}&destination={destino}&travelmode=driving"
        except Exception as e:
            logger.error(f"Error generando URL Maps: {e}")
            return ""
    
    # ----------------------------------------------------------------------
    # HELPER METHODS (de tu código original)
    # ----------------------------------------------------------------------
    
    def _extraer_persona(self, fila: pd.Series) -> Persona:
        """Extrae persona de una fila (de tu código)"""
        nombre_completo = str(fila.get('nombre', '')).strip()
        return Persona(
            nombre_completo=nombre_completo,
            nombre=self._limpiar_titulo(nombre_completo),
            adscripcion=str(fila.get('adscripcion', '')).strip(),
            direccion=str(fila.get('direccion', '')).strip(),
            alcaldia=str(fila.get('alcaldia', '')).strip(),
            notas=str(fila.get('notas', '')).strip() if 'notas' in fila else '',
            fila_original=fila.to_dict()
        )
    
    def _limpiar_titulo(self, nombre: str) -> str:
        """Limpia títulos académicos"""
        if not nombre or pd.isna(nombre):
            return "Sin nombre"
        
        titulos = ['mtra.', 'mtro.', 'lic.', 'ing.', 'dr.', 'dra.',
                   'maestro', 'maestra', 'ingeniero', 'ingeniera',
                   'doctor', 'doctora', 'licenciado', 'licenciada']
        
        nombre_str = str(nombre).strip().lower()
        for titulo in titulos:
            if nombre_str.startswith(titulo):
                nombre_str = nombre_str[len(titulo):].lstrip('. ')
                break
        
        return ' '.join(p.capitalize() for p in nombre_str.split())
    
    def _asignar_zona(self, alcaldia: str) -> str:
        """Asigna zona según alcaldía"""
        if not alcaldia:
            return 'OTRAS'
        alcaldia_upper = alcaldia.upper()
        for zona, alcaldias in self.ZONAS_ALCALDIAS.items():
            if any(alc in alcaldia_upper for alc in alcaldias):
                return zona
        return 'OTRAS'
    
    @staticmethod
    def _haversine(coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
        """Distancia Haversine en km"""
        try:
            lat1, lon1 = coord1
            lat2, lon2 = coord2
            R = 6371
            dlat = math.radians(lat2 - lat1)
            dlon = math.radians(lon2 - lon1)
            a = (math.sin(dlat/2)**2 +
                 math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
                 math.sin(dlon/2)**2)
            return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        except Exception:
            return 9999.0
