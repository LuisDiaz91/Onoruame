# core/route_generator.py
"""
GENERADOR DE RUTAS - VERSIÓN DUAL
Algoritmo dual: vecino cercano (pocos) / k-means (muchos)
Guarda en PostgreSQL vía repositories.

Cambios respecto a versión anterior:
  - Constructor sin parámetros → lee todo de settings
  - Usa RutaRepo.crear_desde_generador (no RutaRepository)
  - ruta.polyline_data (no ruta.polyline)
  - persistir_en_db separado para que Celery lo llame explícitamente
"""

import math
import logging
from typing import List, Dict, Tuple, Optional

import pandas as pd
import requests

from .models      import Persona, Edificio, Ruta
from .geocoder    import Geocoder
from .config      import settings

logger = logging.getLogger(__name__)


class RouteGenerator:
    """
    GENERADOR DE RUTAS CON ALGORITMO DUAL:
      - ≤ UMBRAL_KMEANS edificios → Vecino más cercano (preciso)
      - >  UMBRAL_KMEANS edificios → K-means geográfico + Vecino (escalable)

    Uso desde GUI:
        gen = RouteGenerator()
        zonas = gen.agrupar_edificios(df)
        rutas = gen.crear_rutas(zonas)
        gen.persistir_en_db(rutas)

    Uso desde Celery:
        gen = RouteGenerator()
        rutas = gen.procesar_dataframe(df)   # hace todo en uno
    """

    COLORES_ZONA = {
        'CENTRO':   '#FF6B6B',
        'SUR':      '#4ECDC4',
        'ORIENTE':  '#45B7D1',
        'NORTE':    '#F7DC6F',
        'PONIENTE': '#BB8FCE',
        'OTRAS':    '#FECA57',
    }

    ZONAS_ALCALDIAS = {
        'CENTRO':   ['CUAUHTEMOC', 'MIGUEL HIDALGO', 'BENITO JUAREZ', 'VENUSTIANO CARRANZA'],
        'SUR':      ['ALVARO OBREGON', 'COYOACAN', 'TLALPAN', 'MAGDALENA CONTRERAS',
                     'XOCHIMILCO', 'MILPA ALTA', 'TLAHUAC'],
        'ORIENTE':  ['IZTAPALAPA', 'IZTACALCO'],
        'NORTE':    ['GUSTAVO A. MADERO', 'AZCAPOTZALCO'],
        'PONIENTE': ['CUAJIMALPA'],
    }

    MAX_PARADAS   = settings.MAX_EDIFICIOS_POR_RUTA
    MIN_PARADAS   = 3
    DISTANCIA_MAX = 5.0    # km — corta el vecino más cercano si está muy lejos
    UMBRAL_KMEANS = MAX_PARADAS * 3   # dinámico: si caben en ≤3 rutas → vecino directo

    def __init__(self):
        """
        Sin parámetros — todo viene de settings / .env
        Así Celery, la GUI y los tests lo instancian igual.
        """
        self.geocoder      = Geocoder()
        self.origen        = tuple(
            map(float, settings.ORIGEN_COORDS.replace(' ', '').split(','))
        )
        self.origen_coords = settings.ORIGEN_COORDS
        self.origen_nombre = settings.ORIGEN_NOMBRE

    # ──────────────────────────────────────────────────────────
    # API pública
    # ──────────────────────────────────────────────────────────

    def procesar_dataframe(self, df: pd.DataFrame) -> List[Ruta]:
        """Punto de entrada para Celery — hace todo en un paso."""
        logger.info(f"Procesando {len(df)} registros…")
        zonas = self.agrupar_edificios(df)
        rutas = self.crear_rutas(zonas)
        self.persistir_en_db(rutas)
        return rutas

    def agrupar_edificios(self, df: pd.DataFrame) -> Dict[str, List[Edificio]]:
        """Agrupa filas del DataFrame por dirección única."""
        logger.info("Agrupando personas por edificio…")
        edificios_dict: Dict[str, Edificio] = {}

        for fila_dict in df.to_dict('records'):
            fila    = pd.Series(fila_dict)
            persona = self._extraer_persona(fila)

            if not persona.direccion or persona.direccion in ('', 'nan'):
                continue

            dir_norm = self.geocoder.normalizar_direccion(persona.direccion)
            clave    = f"{dir_norm}_{persona.alcaldia}".lower()

            if clave not in edificios_dict:
                coords = self.geocoder.geocodificar(persona.direccion, persona.alcaldia)
                edificios_dict[clave] = Edificio(
                    direccion_original    = persona.direccion,
                    direccion_normalizada = dir_norm,
                    alcaldia              = persona.alcaldia,
                    dependencia_principal = persona.adscripcion,
                    coordenadas           = coords,
                    personas              = [],
                    zona                  = self._asignar_zona(persona.alcaldia),
                )

            # Guardar como dict para que sea JSON-serializable (Celery)
            edificios_dict[clave].personas.append({
                'nombre_completo': persona.nombre_completo,
                'nombre':          persona.nombre,
                'adscripcion':     persona.adscripcion,
                'direccion':       persona.direccion,
                'alcaldia':        persona.alcaldia,
                'notas':           persona.notas,
            })

        por_zona: Dict[str, List[Edificio]] = {}
        for edificio in edificios_dict.values():
            por_zona.setdefault(edificio.zona, []).append(edificio)

        total_p = sum(len(e.personas) for e in edificios_dict.values())
        logger.info(f"Edificios únicos: {len(edificios_dict)} | Personas: {total_p}")
        self.geocoder.log_stats()
        return por_zona

    def crear_rutas(self, edificios_por_zona: Dict[str, List[Edificio]]) -> List[Ruta]:
        """Algoritmo dual: vecino cercano o k-means según volumen."""
        todas:   List[Ruta] = []
        ruta_id: int        = 1

        for zona, edificios in edificios_por_zona.items():
            if not edificios:
                continue

            total      = len(edificios)
            con_coords = [e for e in edificios if e.coordenadas]
            sin_coords = [e for e in edificios if not e.coordenadas]

            if total <= self.UMBRAL_KMEANS or len(con_coords) < self.MIN_PARADAS * 2:
                logger.info(f"Zona {zona}: {total} edificios → vecino más cercano")
                rutas_zona = self._vecino_mas_cercano(con_coords, zona, ruta_id)
            else:
                k = self._calcular_k(total)
                logger.info(f"Zona {zona}: {total} edificios → k-means ({k} clusters)")
                rutas_zona = []
                for cluster in self._kmeans_geo(con_coords, k):
                    if not cluster:
                        continue
                    sub = self._vecino_mas_cercano(cluster, zona, ruta_id)
                    rutas_zona.extend(sub)
                    if sub:
                        ruta_id = max(r.id for r in sub) + 1

            if rutas_zona:
                ruta_id = max(r.id for r in rutas_zona) + 1

            if sin_coords:
                if rutas_zona:
                    self._distribuir_sin_coords(rutas_zona, sin_coords)
                else:
                    emergencia = self._rutas_emergencia(sin_coords, zona, ruta_id)
                    todas.extend(emergencia)
                    if emergencia:
                        ruta_id = max(r.id for r in emergencia) + 1

            todas.extend(rutas_zona)

        rutas_finales = self._fusionar_pequenas(todas)

        for ruta in rutas_finales:
            if len([e for e in ruta.edificios if e.coordenadas]) >= 2:
                self._optimizar_con_google(ruta)

        for i, r in enumerate(rutas_finales, 1):
            r.id = i

        logger.info(
            f"✅ {len(rutas_finales)} rutas | "
            f"{sum(r.total_edificios for r in rutas_finales)} paradas | "
            f"{sum(r.total_personas  for r in rutas_finales)} personas"
        )
        return rutas_finales

    def persistir_en_db(self, rutas: List[Ruta]):
        """
        Guarda rutas en PostgreSQL vía RutaRepo.
        Separado de crear_rutas para que la GUI pueda llamarlo
        independientemente de Celery.
        """
        # Importación local para evitar ciclo: route_generator → repositories → database
        from .repositories import RutaRepo

        for ruta in rutas:
            try:
                db_id    = RutaRepo.crear_desde_generador(ruta)
                ruta.db_id = db_id
                logger.info(f"  DB ← ruta {db_id} ({ruta.zona}) "
                            f"{ruta.total_edificios} paradas")
            except Exception as e:
                logger.error(f"Error persistiendo ruta {ruta.id}: {e}")
                raise

    # ──────────────────────────────────────────────────────────
    # Algoritmos internos
    # ──────────────────────────────────────────────────────────

    def _vecino_mas_cercano(
        self, edificios: List[Edificio], zona: str, inicio_id: int
    ) -> List[Ruta]:
        if not edificios:
            return []

        disponibles = edificios.copy()
        rutas: List[Ruta] = []
        ruta_id = inicio_id

        while disponibles:
            actual: List[Edificio] = []
            punto  = self.origen

            primero = min(disponibles,
                         key=lambda e: self._haversine(self.origen, e.coordenadas))
            actual.append(primero)
            disponibles.remove(primero)
            punto = primero.coordenadas

            while len(actual) < self.MAX_PARADAS and disponibles:
                siguiente = min(disponibles,
                               key=lambda e: self._haversine(punto, e.coordenadas))
                dist = self._haversine(punto, siguiente.coordenadas)

                if dist > self.DISTANCIA_MAX and len(actual) >= self.MIN_PARADAS:
                    break

                actual.append(siguiente)
                disponibles.remove(siguiente)
                punto = siguiente.coordenadas

            if len(actual) >= self.MIN_PARADAS:
                rutas.append(Ruta(id=ruta_id, zona=zona,
                                  edificios=actual, origen=self.origen_nombre))
                ruta_id += 1
            else:
                disponibles.extend(actual)
                break

        # Reabsorber sobrantes
        if disponibles and rutas:
            for edificio in disponibles:
                for r in rutas:
                    if len(r.edificios) < self.MAX_PARADAS:
                        r.edificios.append(edificio)
                        break

        return rutas

    def _kmeans_geo(
        self, edificios: List[Edificio], k: int, max_iter: int = 15
    ) -> List[List[Edificio]]:
        """K-means++ con distancia Haversine."""
        if k >= len(edificios):
            return [[e] for e in edificios]

        # Inicialización K-means++
        centroides: List[Tuple[float, float]] = []
        restantes = edificios.copy()

        primero = max(restantes,
                     key=lambda e: self._haversine(self.origen, e.coordenadas))
        centroides.append(primero.coordenadas)
        restantes.remove(primero)

        while len(centroides) < k and restantes:
            mas_lejano = max(
                restantes,
                key=lambda e: min(self._haversine(e.coordenadas, c) for c in centroides)
            )
            centroides.append(mas_lejano.coordenadas)
            restantes.remove(mas_lejano)

        grupos: List[List[Edificio]] = [[] for _ in range(k)]

        for _ in range(max_iter):
            nuevos_grupos: List[List[Edificio]] = [[] for _ in range(k)]

            for edificio in edificios:
                dists = [self._haversine(edificio.coordenadas, c) for c in centroides]
                nuevos_grupos[dists.index(min(dists))].append(edificio)

            nuevos_centroides = []
            for i, grupo in enumerate(nuevos_grupos):
                if grupo:
                    lat = sum(e.coordenadas[0] for e in grupo) / len(grupo)
                    lng = sum(e.coordenadas[1] for e in grupo) / len(grupo)
                    nuevos_centroides.append((lat, lng))
                else:
                    nuevos_centroides.append(centroides[i])

            convergio = all(
                self._haversine(v, n) < 0.01
                for v, n in zip(centroides, nuevos_centroides)
            )
            centroides = nuevos_centroides
            grupos     = nuevos_grupos

            if convergio:
                logger.debug(f"K-means convergió")
                break

        return [g for g in grupos if g]

    def _calcular_k(self, total: int) -> int:
        return max(2, min(math.ceil(total / self.MAX_PARADAS), 50))

    def _distribuir_sin_coords(self, rutas: List[Ruta], sin_coords: List[Edificio]):
        rutas_ord = sorted(rutas, key=lambda r: len(r.edificios))
        for edificio in sin_coords:
            for ruta in rutas_ord:
                if len(ruta.edificios) < self.MAX_PARADAS:
                    ruta.edificios.append(edificio)
                    break

    def _rutas_emergencia(
        self, edificios: List[Edificio], zona: str, inicio_id: int
    ) -> List[Ruta]:
        rutas: List[Ruta] = []
        for i in range(0, len(edificios), self.MAX_PARADAS):
            grupo = edificios[i:i + self.MAX_PARADAS]
            if rutas and len(grupo) < self.MIN_PARADAS:
                if len(rutas[-1].edificios) + len(grupo) <= self.MAX_PARADAS:
                    rutas[-1].edificios.extend(grupo)
                    continue
            rutas.append(Ruta(id=inicio_id + len(rutas), zona=zona,
                              edificios=grupo, origen=self.origen_nombre))
        return rutas

    def _fusionar_pequenas(self, rutas: List[Ruta]) -> List[Ruta]:
        por_zona: Dict[str, List[Ruta]] = {}
        for r in rutas:
            por_zona.setdefault(r.zona, []).append(r)

        resultado: List[Ruta] = []
        for zona, zrutas in por_zona.items():
            normales = [r for r in zrutas if len(r.edificios) >= self.MIN_PARADAS]
            pequenas = [r for r in zrutas if len(r.edificios) <  self.MIN_PARADAS]

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
        try:
            con_coords = [e for e in ruta.edificios if e.coordenadas]
            if len(con_coords) < 2:
                return

            waypoints = "|".join(
                f"{lat},{lng}" for lat, lng in [e.coordenadas for e in con_coords]
            )
            resp = requests.get(
                "https://maps.googleapis.com/maps/api/directions/json",
                params={
                    'origin':      self.origen_coords,
                    'destination': self.origen_coords,
                    'waypoints':   f"optimize:true|{waypoints}",
                    'key':         settings.GOOGLE_MAPS_API_KEY,
                    'language':    'es',
                    'units':       'metric',
                },
                timeout=10,
            )
            data = resp.json()

            if data.get('status') == 'OK' and data['routes']:
                route = data['routes'][0]
                orden = route['waypoint_order']

                opt = [con_coords[i] for i in orden]
                sin = [e for e in ruta.edificios if not e.coordenadas]
                ruta.edificios    = opt + sin
                ruta.distancia_km = sum(l['distance']['value'] for l in route['legs']) / 1000
                ruta.tiempo_min   = sum(l['duration']['value'] for l in route['legs']) / 60
                ruta.polyline_data = route['overview_polyline']['points']   # ← corregido

        except Exception as e:
            logger.error(f"Error optimizando ruta {ruta.id}: {e}")

    # ──────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────

    def _extraer_persona(self, fila: pd.Series) -> Persona:
        nombre_completo = str(fila.get('nombre', '')).strip()
        return Persona(
            nombre_completo = nombre_completo,
            nombre          = self._limpiar_titulo(nombre_completo),
            adscripcion     = str(fila.get('adscripcion', '')).strip(),
            direccion       = str(fila.get('direccion',   '')).strip(),
            alcaldia        = str(fila.get('alcaldia',    '')).strip(),
            notas           = str(fila.get('notas', '')).strip() if 'notas' in fila else '',
            fila_original   = fila.to_dict(),
        )

    def _limpiar_titulo(self, nombre: str) -> str:
        if not nombre or (isinstance(nombre, float) and math.isnan(nombre)):
            return 'Sin nombre'
        titulos = [
            'mtra.', 'mtro.', 'lic.', 'ing.', 'dr.', 'dra.',
            'magdo.', 'mgda.', 'comisario', 'maestro', 'maestra',
            'ingeniero', 'ingeniera', 'doctor', 'doctora',
            'licenciado', 'licenciada',
        ]
        n = str(nombre).strip().lower()
        for t in titulos:
            if n.startswith(t):
                n = n[len(t):].lstrip('. ')
                break
        return ' '.join(p.capitalize() for p in n.split())

    def _asignar_zona(self, alcaldia: str) -> str:
        if not alcaldia:
            return 'OTRAS'
        up = alcaldia.upper()
        for zona, alcaldias in self.ZONAS_ALCALDIAS.items():
            if any(a in up for a in alcaldias):
                return zona
        return 'OTRAS'

    @staticmethod
    def _haversine(c1: Tuple[float, float], c2: Tuple[float, float]) -> float:
        try:
            lat1, lon1 = c1
            lat2, lon2 = c2
            R    = 6371
            dlat = math.radians(lat2 - lat1)
            dlon = math.radians(lon2 - lon1)
            a    = (math.sin(dlat/2)**2 +
                    math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
                    math.sin(dlon/2)**2)
            return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        except Exception:
            return 9999.0
