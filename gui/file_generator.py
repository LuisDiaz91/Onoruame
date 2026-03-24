# gui/file_generator.py
"""
Generador de mapas interactivos para Onoruame.
"""
import os
import logging

import folium
import polyline as polyline_lib

from core.models  import Ruta, Edificio
from core.config  import settings

logger = logging.getLogger(__name__)


class FileGenerator:
    """Genera mapas HTML con Folium para cada ruta."""

    COLORES_ZONA = {
        'CENTRO':   '#FF6B6B',
        'SUR':      '#4ECDC4',
        'ORIENTE':  '#45B7D1',
        'NORTE':    '#F7DC6F',
        'PONIENTE': '#BB8FCE',
        'OTRAS':    '#FECA57',
    }

    def __init__(self, carpeta: str = 'mapas_pro'):
        self.carpeta = carpeta
        os.makedirs(carpeta, exist_ok=True)

    # ──────────────────────────────────────────────────────────
    # API pública
    # ──────────────────────────────────────────────────────────

    def generar_mapa(self, ruta: Ruta) -> str:
        """
        Genera mapa HTML para una ruta y lo guarda en self.carpeta.
        Retorna la ruta del archivo generado.
        """
        origen = tuple(
            map(float, settings.ORIGEN_COORDS.replace(' ', '').split(','))
        )
        color = self.COLORES_ZONA.get(ruta.zona, '#888888')

        m = folium.Map(location=origen, zoom_start=13, tiles='CartoDB positron')

        # Marcador de origen
        folium.Marker(
            origen,
            popup=f"<b>🏛️ {settings.ORIGEN_NOMBRE}</b>",
            tooltip="Origen",
            icon=folium.Icon(color='green', icon='home', prefix='fa'),
        ).add_to(m)

        # Polyline de la ruta optimizada
        if ruta.polyline_data:                          # ← corregido (era ruta.polyline)
            try:
                puntos = polyline_lib.decode(ruta.polyline_data)
                folium.PolyLine(
                    puntos,
                    color=color,
                    weight=5,
                    opacity=0.75,
                    tooltip=f"Ruta {ruta.id} — {ruta.zona}",
                ).add_to(m)
            except Exception as e:
                logger.warning(f"No se pudo dibujar polyline de ruta {ruta.id}: {e}")

        # Marcadores de paradas numerados
        edificios_con_coords = [e for e in ruta.edificios if e.coordenadas]
        for i, edificio in enumerate(ruta.edificios, 1):
            if not edificio.coordenadas:
                continue

            folium.Marker(
                edificio.coordenadas,
                popup=self._popup_edificio(edificio, i, ruta.zona, color),
                tooltip=f"#{i} — {edificio.total_personas} persona(s)",
                icon=folium.DivIcon(
                    html=f"""
                        <div style="
                            background:{color}; color:#fff;
                            border-radius:50%; width:28px; height:28px;
                            display:flex; align-items:center; justify-content:center;
                            font-weight:bold; font-size:13px;
                            box-shadow:0 2px 6px rgba(0,0,0,.4);
                        ">{i}</div>
                    """,
                    icon_size=(28, 28),
                    icon_anchor=(14, 14),
                ),
            ).add_to(m)

        # Panel informativo fijo
        self._panel_info(m, ruta, color)

        filename = os.path.join(self.carpeta, f"Ruta_{ruta.id}_{ruta.zona}.html")
        m.save(filename)
        logger.info(f"🗺️  Mapa generado: {filename}")
        return filename

    def generar_todos(self, rutas: list) -> list:
        """Genera mapas para una lista de rutas. Retorna lista de archivos."""
        archivos = []
        for ruta in rutas:
            try:
                archivos.append(self.generar_mapa(ruta))
            except Exception as e:
                logger.error(f"Error generando mapa ruta {ruta.id}: {e}")
        return archivos

    # ──────────────────────────────────────────────────────────
    # Helpers privados
    # ──────────────────────────────────────────────────────────

    def _popup_edificio(
        self, edificio: Edificio, idx: int, zona: str, color: str
    ) -> folium.Popup:
        personas_html = ""
        for p in edificio.personas[:5]:
            nombre = p.get('nombre', '') if isinstance(p, dict) else getattr(p, 'nombre', '')
            personas_html += f"<small>• {nombre}</small><br>"
        if edificio.total_personas > 5:
            personas_html += f"<small>• … y {edificio.total_personas - 5} más</small>"

        html = f"""
        <div style="font-family:Arial; width:320px;">
            <h4 style="color:{color}; margin:0 0 8px; font-size:14px;">
                🏢 Parada #{idx} — {zona}
            </h4>
            <b>📍 {edificio.direccion_original[:90]}</b><br>
            <small style="color:#666;">
                {edificio.alcaldia} &nbsp;|&nbsp; {edificio.total_personas} persona(s)
            </small>
            <hr style="margin:8px 0; border-color:#eee;">
            {personas_html}
        </div>
        """
        return folium.Popup(html, max_width=340)

    def _panel_info(self, mapa: folium.Map, ruta: Ruta, color: str):
        dist  = f"{ruta.distancia_km:.1f} km" if ruta.distancia_km else "—"
        tiempo = f"{int(ruta.tiempo_min)} min" if ruta.tiempo_min else "—"

        panel = f"""
        <div style="
            position:fixed; top:12px; left:52px; z-index:1000;
            background:#fff; padding:14px 18px;
            border-radius:10px; box-shadow:0 2px 16px rgba(0,0,0,.18);
            border-left:4px solid {color}; font-family:Arial; min-width:200px;
        ">
            <div style="font-weight:bold; font-size:15px; color:#2c3e50; margin-bottom:8px;">
                Ruta {ruta.id} &mdash; {ruta.zona}
            </div>
            <div style="font-size:12px; line-height:1.8; color:#444;">
                🏢 <b>{ruta.total_edificios}</b> paradas<br>
                👥 <b>{ruta.total_personas}</b> personas<br>
                📏 <b>{dist}</b><br>
                ⏱️ <b>{tiempo}</b>
            </div>
        </div>
        """
        mapa.get_root().html.add_child(folium.Element(panel))
