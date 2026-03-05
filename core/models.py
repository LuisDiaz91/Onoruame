"""
Modelos de datos compartidos (dataclasses)
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple


@dataclass
class Persona:
    """Persona a la que entregar"""
    nombre_completo: str
    nombre: str
    adscripcion: str
    direccion: str
    alcaldia: str
    notas: str = ""
    fila_original: Dict = field(default_factory=dict)


@dataclass
class Edificio:
    """Una parada = un edificio/dirección"""
    direccion_original: str
    direccion_normalizada: str
    alcaldia: str
    dependencia_principal: str
    coordenadas: Optional[Tuple[float, float]]
    personas: List[Dict] = field(default_factory=list)
    zona: str = ""
    
    @property
    def total_personas(self) -> int:
        return len(self.personas)


@dataclass
class Ruta:
    """Ruta optimizada"""
    id: int
    zona: str
    edificios: List[Edificio]
    origen: str
    distancia_km: float = 0
    tiempo_min: float = 0
    polyline_data: str = ""
    db_id: Optional[int] = None  # ID en PostgreSQL
    
    @property
    def total_edificios(self) -> int:
        return len(self.edificios)
    
    @property
    def total_personas(self) -> int:
        return sum(e.total_personas for e in self.edificios)
