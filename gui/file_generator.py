# gui/file_generator.py
"""
Generador de mapas y archivos para Onoruame
Adaptado del FileGenerator original
"""

import os
import logging
import folium
import polyline
from typing import List
from core.models import Ruta
from core.config import settings

logger = logging.getLogger(__name__)

class FileGenerator:
    """Generador de mapas interactivos"""
    
    COLORES_ZONA = {
        'CENTRO': '#FF6B6B',
        'SUR': '#4ECDC4',
        'ORIENTE': '#45B7D1',
        'SUR_ORIENTE': '#96CEB4',
        'OTRAS': '#FECA57',
        'MIXTA': '#9B59B6'
    }
    
    def __init__(self):
        os.makedirs('mapas_pro', exist_ok=True)
    
    def generar_mapa(self, ruta: Ruta) -> str:
        """Genera mapa interactivo para una ruta"""
        origen = tuple(map(float, settings.ORIGEN_COORDS.split(',')))
        color = self.COLORES_ZONA.get(ruta.zona, 'gray')
        
        m = folium.Map(location=origen, zoom_start=13, tiles='CartoDB positron')
        
        # Marcador de origen
        folium.Marker(
            origen,
            popup=f"<b>🏛️ {ruta.origen}</b>",
            icon=folium.Icon(color='green', icon='balance-scale', prefix='fa')
        ).add_to(m)
        
        # Dibujar ruta optimizada
        if ruta.polyline:
            folium.PolyLine(
                polyline.decode(ruta.polyline),
                color=color,
                weight=5,
                opacity=0.7,
                popup=f"Ruta {ruta.id} - {ruta.zona}"
            ).add_to(m)
        
        # Marcadores de edificios
        for i, edificio in enumerate(ruta.edificios, 1):
            if not edificio.coordenadas:
                continue
            
            popup = self._crear_popup(edificio, i, ruta.zona)
            
            folium.Marker(
                edificio.coordenadas,
                popup=popup,
                tooltip=f"Edificio #{i}: {edificio.total_personas} personas",
                icon=folium.Icon(color='red', icon='building', prefix='fa')
            ).add_to(m)
        
        # Panel informativo
        self._agregar_panel(m, ruta, color)
        
        filename = f"mapas_pro/Ruta_{ruta.id}_{ruta.zona}.html"
        m.save(filename)
        logger.info(f"🗺️ Mapa generado: {filename}")
        
        return filename
    
    def _crear_popup(self, edificio, idx, zona):
        """Crea el popup HTML para un edificio"""
        return f"""
        <div style="font-family: Arial; width: 350px;">
            <h4 style="color: {self.COLORES_ZONA.get(zona, 'gray')}; margin: 0 0 10px;">
                🏢 Edificio #{idx} - {zona}
            </h4>
            <b>📍 {edificio.direccion_original[:100]}</b><br>
            <small>👥 {edificio.total_personas} personas</small>
        </div>
        """
    
    def _agregar_panel(self, mapa, ruta, color):
        """Agrega panel informativo al mapa"""
        panel = f"""
        <div style="position:fixed; top:10px; left:50px; z-index:1000; background:white; 
                    padding:15px; border-radius:10px; box-shadow:0 0 15px rgba(0,0,0,0.2);
                    border:2px solid {color}; font-family:Arial; max-width:400px;">
            <h4 style="margin:0 0 10px; color:#2c3e50; border-bottom:2px solid {color}; padding-bottom:5px;">
                Ruta {ruta.id} - {ruta.zona}
            </h4>
            <small>
                <b>🏢 Edificios:</b> {ruta.total_edificios}<br>
                <b>👥 Personas:</b> {ruta.total_personas}<br>
                <b>📏 Distancia:</b> {ruta.distancia_km:.1f} km<br>
                <b>⏱️ Tiempo:</b> {ruta.tiempo_min:.0f} min<br>
            </small>
        </div>
        """
        mapa.get_root().html.add_child(folium.Element(panel))
