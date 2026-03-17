"""
Procesador de archivos Excel para Onoruame
Detecta columnas automáticamente y extrae datos
"""

import pandas as pd
import re
import logging
from typing import List, Dict, Tuple

logger = logging.getLogger(__name__)

class ExcelProcessor:
    """Procesador de Excel con detección dinámica de columnas"""

    COLUMNAS_BUSQUEDA = {
        'nombre': ['NOMBRE', 'NAME', 'NOMBRE COMPLETO', 'NOMBRES'],
        'direccion': ['DIRECCIÓN', 'DIRECCION', 'DOMICILIO', 'ADDRESS', 'UBICACIÓN'],
        'adscripcion': ['ADSCRIPCIÓN', 'ADSCRIPCION', 'CARGO', 'PUESTO', 'DEPENDENCIA'],
        'alcaldia': ['ALCALDÍA', 'ALCALDIA', 'MUNICIPIO', 'DELEGACIÓN', 'DELEGACION'],
        'notas': ['NOTAS', 'OBSERVACIONES', 'COMENTARIOS', 'NUMERO', 'FOLIO']
    }

    def __init__(self, archivo: str):
        self.archivo = archivo
        # Intentar leer el archivo
        if archivo.endswith('.csv'):
            self.df_raw = pd.read_csv(archivo, dtype=str)
        else:
            self.df_raw = pd.read_excel(archivo, dtype=str)
        self.columnas_detectadas = {}

    def procesar(self) -> pd.DataFrame:
        """Procesa el archivo y retorna DataFrame estandarizado"""
        logger.info(f"Procesando archivo: {self.archivo}")
        
        # Detectar columnas basado en nombres
        self._detectar_columnas()
        
        # Extraer datos
        datos = self._extraer_datos()
        
        if not datos:
            return pd.DataFrame()
        
        df = pd.DataFrame(datos)
        logger.info(f"Registros extraídos: {len(df)}")
        return df

    def _detectar_columnas(self):
        """Detecta qué columnas corresponden a cada campo"""
        for col in self.df_raw.columns:
            col_str = str(col).upper().strip()
            for campo, patrones in self.COLUMNAS_BUSQUEDA.items():
                if any(p in col_str for p in patrones):
                    self.columnas_detectadas[campo] = col
                    logger.info(f"Columna '{col}' detectada como '{campo}'")
                    break

    def _extraer_datos(self) -> List[Dict]:
        """Extrae los datos usando las columnas detectadas"""
        datos = []
        
        for _, fila in self.df_raw.iterrows():
            if fila.isnull().all():
                continue
            
            # Si no se detectaron columnas, usar las primeras 4
            if not self.columnas_detectadas:
                self.columnas_detectadas = {
                    'nombre': self.df_raw.columns[0] if len(self.df_raw.columns) > 0 else None,
                    'direccion': self.df_raw.columns[1] if len(self.df_raw.columns) > 1 else None,
                    'adscripcion': self.df_raw.columns[2] if len(self.df_raw.columns) > 2 else None,
                    'alcaldia': self.df_raw.columns[3] if len(self.df_raw.columns) > 3 else None
                }

            # Extraer cada campo
            nombre = self._get_valor(fila, 'nombre')
            if not nombre:  # Si no hay nombre, saltar fila
                continue

            direccion = self._get_valor(fila, 'direccion') or ''
            adscripcion = self._get_valor(fila, 'adscripcion') or ''
            alcaldia = self._get_valor(fila, 'alcaldia') or ''
            notas = self._get_valor(fila, 'notas') or ''

            datos.append({
                'nombre': nombre,
                'direccion': self._limpiar_direccion(direccion),
                'adscripcion': adscripcion,
                'alcaldia': alcaldia,
                'notas': notas
            })

        return datos

    def _get_valor(self, fila, campo):
        """Obtiene el valor de una fila para un campo"""
        col = self.columnas_detectadas.get(campo)
        if col and col in fila.index:
            val = fila[col]
            if pd.notna(val):
                return str(val).strip()
        return ''

    def _limpiar_direccion(self, direccion: str) -> str:
        """Limpia la dirección quitando saltos de línea y espacios extra"""
        if not direccion:
            return ""
        # Reemplazar saltos de línea por espacios
        direccion = re.sub(r'[\n\r]', ' ', direccion)
        # Eliminar espacios múltiples
        return re.sub(r'\s+', ' ', direccion).strip()
EOF
