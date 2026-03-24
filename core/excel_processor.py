# core/excel_processor.py
"""
Procesador de Excel para el formato PJCDMX.

Formato esperado:
    NUMERO | NOMBRE | ADSCRIPCIÓN | DIRECCIÓN | ALCALDIA

Características del formato real:
  - Columnas fijas (sin secciones múltiples)
  - Títulos académicos en el nombre (Mtra., Lic., Dr., Magdo., etc.)
  - Direcciones con saltos de línea dentro de la celda
  - CP embebido en la dirección ("C.P. 03100")
  - Notas extra en dirección ("PARA ENTREGA DE INVITACIONES EN: ...")
  - Alcaldía a veces con "Alc." o "Alcaldía." abreviado
  - Números de fila no consecutivos (10, 13, 21, 25...)
"""

import re
import logging
import math
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Mapeo flexible de nombres de columna
# ─────────────────────────────────────────────────────────────

COLUMNAS = {
    'numero':     ['NUMERO', 'NÚMERO', 'NUM', 'NÚM', 'NO', 'N°', '#'],
    'nombre':     ['NOMBRE', 'NOMBRE COMPLETO', 'NAME'],
    'adscripcion':['ADSCRIPCIÓN', 'ADSCRIPCION', 'CARGO', 'PUESTO',
                   'DEPENDENCIA', 'INSTITUCIÓN', 'INSTITUCION'],
    'direccion':  ['DIRECCIÓN', 'DIRECCION', 'DOMICILIO', 'ADDRESS',
                   'UBICACIÓN', 'UBICACION'],
    'alcaldia':   ['ALCALDIA', 'ALCALDÍA', 'DELEGACIÓN', 'DELEGACION',
                   'MUNICIPIO'],
    'notas':      ['NOTAS', 'OBSERVACIONES', 'COMENTARIOS'],
}

# ─────────────────────────────────────────────────────────────
# Procesador principal
# ─────────────────────────────────────────────────────────────

class ExcelProcessor:
    """
    Carga y normaliza el Excel de entregas PJCDMX.

    Uso:
        proc = ExcelProcessor("Alcaldias.xlsx")
        df   = proc.procesar()
        # df tiene columnas: numero, nombre, adscripcion, direccion, alcaldia, notas
    """

    def __init__(self, archivo: str):
        self.archivo = archivo

    def procesar(self) -> pd.DataFrame:
        """
        Retorna DataFrame limpio con columnas estándar.
        Siempre retorna algo (DataFrame vacío si falla todo).
        """
        logger.info(f"Procesando Excel: {self.archivo}")

        # Intentar detectar la fila de encabezado automáticamente
        df_raw = pd.read_excel(self.archivo, header=None, dtype=str)
        header_row = self._detectar_encabezado(df_raw)

        logger.info(f"Encabezado detectado en fila {header_row}")

        df = pd.read_excel(self.archivo, header=header_row, dtype=str)
        df.columns = [self._limpiar_str(c) for c in df.columns]

        # Mapear columnas a nombres estándar
        mapa = self._mapear_columnas(df.columns.tolist())
        logger.info(f"Columnas mapeadas: {mapa}")

        # Renombrar a nombres estándar
        df = df.rename(columns={v: k for k, v in mapa.items() if v})

        # Asegurar que existen todas las columnas necesarias
        for col in ['numero', 'nombre', 'adscripcion', 'direccion', 'alcaldia', 'notas']:
            if col not in df.columns:
                df[col] = ''

        # Limpiar fila por fila
        registros = []
        for _, fila in df.iterrows():
            nombre = self._limpiar_str(fila.get('nombre', ''))
            if not nombre:
                continue

            direccion_raw = self._limpiar_str(fila.get('direccion', ''))
            direccion     = self._limpiar_direccion(direccion_raw)

            if not direccion:
                logger.warning(f"Sin dirección: {nombre[:40]}")
                continue

            registros.append({
                'numero':      self._limpiar_str(fila.get('numero', '')),
                'nombre':      nombre,
                'adscripcion': self._limpiar_str(fila.get('adscripcion', '')),
                'direccion':   direccion,
                'alcaldia':    self._normalizar_alcaldia(
                                   self._limpiar_str(fila.get('alcaldia', ''))
                               ),
                'notas':       self._limpiar_str(fila.get('notas', '')),
            })

        resultado = pd.DataFrame(registros)
        logger.info(f"Registros válidos: {len(resultado)} / {len(df)}")
        return resultado

    # ──────────────────────────────────────────────────────────
    # Detección de encabezado
    # ──────────────────────────────────────────────────────────

    def _detectar_encabezado(self, df_raw: pd.DataFrame) -> int:
        """
        Busca la fila que contiene 'NOMBRE' — esa es el encabezado.
        Soporta Excels con filas vacías arriba o títulos de portada.
        """
        for idx, fila in df_raw.iterrows():
            for celda in fila:
                if isinstance(celda, str) and 'NOMBRE' in celda.upper():
                    return idx
        # Si no encuentra, asumir primera fila
        logger.warning("No se detectó fila de encabezado, usando fila 0")
        return 0

    # ──────────────────────────────────────────────────────────
    # Mapeo de columnas
    # ──────────────────────────────────────────────────────────

    def _mapear_columnas(self, columnas: List[str]) -> Dict[str, Optional[str]]:
        """
        Mapea nombres reales de columnas a nombres estándar.
        Retorna {'nombre_estandar': 'nombre_real_en_df'}.
        """
        mapa: Dict[str, Optional[str]] = {k: None for k in COLUMNAS}

        for col_real in columnas:
            col_upper = col_real.upper().strip()
            for estandar, patrones in COLUMNAS.items():
                if mapa[estandar]:
                    continue  # ya mapeado
                if any(p in col_upper for p in patrones):
                    mapa[estandar] = col_real
                    break

        return mapa

    # ──────────────────────────────────────────────────────────
    # Limpieza de dirección
    # ──────────────────────────────────────────────────────────

    def _limpiar_direccion(self, direccion: str) -> str:
        """
        Normaliza la dirección para geocodificación:
          1. Quita saltos de línea y tags HTML
          2. Extrae solo la primera dirección si hay "PARA ENTREGA EN:"
          3. Quita CP, "Ciudad de México", "CDMX"
          4. Colapsa espacios
        """
        if not direccion:
            return ''

        d = direccion

        # 1. Saltos de línea y HTML
        d = re.sub(r'<br\s*/?>', ' ', d, flags=re.IGNORECASE)
        d = re.sub(r'[\n\r\t]', ' ', d)

        # 2. Si hay indicación de entrega alternativa, quedarse con la primera parte
        #    Ej: "Av Zapata 340 ... PARA ENTREGA DE INVITACIONES EN: Av Zapata 340 ..."
        patrones_split = [
            r'PARA ENTREGA DE INVITACIONES EN[:\s]',
            r'PARA ENTREGA EN[:\s]',
            r'NOTA[:\s]',
        ]
        for pat in patrones_split:
            match = re.search(pat, d, flags=re.IGNORECASE)
            if match:
                d = d[:match.start()].strip()
                break

        # 3. Quitar CP
        d = re.sub(r'C\.?\s*P\.?\s*\d{5}', '', d, flags=re.IGNORECASE)
        d = re.sub(r'CÓDIGO POSTAL\s*\d{5}', '', d, flags=re.IGNORECASE)

        # 4. Quitar menciones de ciudad/país redundantes
        d = re.sub(r',?\s*Ciudad de México\.?', '', d, flags=re.IGNORECASE)
        d = re.sub(r',?\s*CDMX\.?', '', d, flags=re.IGNORECASE)
        d = re.sub(r',?\s*México\.?\s*$', '', d, flags=re.IGNORECASE)

        # 5. Normalizar "Alcaldía X" al final (ya viene en columna separada)
        d = re.sub(r',?\s*Alc(?:aldía)?\.?\s+\w[\w\s]+$', '', d, flags=re.IGNORECASE)

        # 6. Colapsar espacios y quitar comas/puntos finales sueltos
        d = re.sub(r'\s+', ' ', d).strip().rstrip('.,')

        return d

    # ──────────────────────────────────────────────────────────
    # Normalización de alcaldía
    # ──────────────────────────────────────────────────────────

    _ALCALDIAS_CDMX = {
        'BENITO JUAREZ':           'Benito Juárez',
        'BENITO JUÁREZ':           'Benito Juárez',
        'CUAUHTEMOC':              'Cuauhtémoc',
        'CUAUHTÉMOC':              'Cuauhtémoc',
        'MIGUEL HIDALGO':          'Miguel Hidalgo',
        'VENUSTIANO CARRANZA':     'Venustiano Carranza',
        'IZTAPALAPA':              'Iztapalapa',
        'IZTACALCO':               'Iztacalco',
        'GUSTAVO A MADERO':        'Gustavo A. Madero',
        'GUSTAVO A. MADERO':       'Gustavo A. Madero',
        'AZCAPOTZALCO':            'Azcapotzalco',
        'ALVARO OBREGON':          'Álvaro Obregón',
        'ÁLVARO OBREGÓN':          'Álvaro Obregón',
        'COYOACAN':                'Coyoacán',
        'COYOACÁN':                'Coyoacán',
        'TLALPAN':                 'Tlalpan',
        'MAGDALENA CONTRERAS':     'Magdalena Contreras',
        'LA MAGDALENA CONTRERAS':  'Magdalena Contreras',
        'XOCHIMILCO':              'Xochimilco',
        'MILPA ALTA':              'Milpa Alta',
        'TLAHUAC':                 'Tláhuac',
        'TLÁHUAC':                 'Tláhuac',
        'CUAJIMALPA':              'Cuajimalpa',
        'CUAJIMALPA DE MORELOS':   'Cuajimalpa',
    }

    def _normalizar_alcaldia(self, alcaldia: str) -> str:
        if not alcaldia:
            return ''
        # Quitar prefijos comunes
        a = re.sub(r'^Alc(?:aldía)?\.?\s*', '', alcaldia, flags=re.IGNORECASE).strip()
        a_upper = a.upper()
        return self._ALCALDIAS_CDMX.get(a_upper, a.title())

    # ──────────────────────────────────────────────────────────
    # Utilidades
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _limpiar_str(valor) -> str:
        if valor is None:
            return ''
        s = str(valor).strip()
        return '' if s.lower() in ('nan', 'none', 'nat', '') else s
