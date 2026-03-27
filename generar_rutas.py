from core.excel_processor import ExcelProcessor
from core.route_generator import RouteGenerator

# Procesar Excel
proc = ExcelProcessor('prueba_pjcdmx.xlsx')
personas = proc.procesar()
print(f'✅ {len(personas)} personas cargadas')

# Agrupar edificios
gen = RouteGenerator()
edificios_por_zona = gen.agrupar_edificios(personas)

# Generar rutas
rutas = gen.crear_rutas(edificios_por_zona)

# Mostrar resultados
print(f'\n✅ {len(rutas)} rutas generadas:')
for i, r in enumerate(rutas, 1):
    print(f'\n📍 Ruta {i}:')
    print(f'   Zona: {r.zona}')
    print(f'   Edificios: {r.total_edificios}')
    print(f'   Personas: {r.total_personas}')
    print(f'   Distancia: {r.distancia_km:.1f} km')
    print(f'   Tiempo: {r.tiempo_min:.0f} min')
