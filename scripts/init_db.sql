-- Tabla de rutas
CREATE TABLE IF NOT EXISTS rutas (
    id SERIAL PRIMARY KEY,
    ruta_id INTEGER,
    zona VARCHAR(50),
    origen VARCHAR(255),
    distancia_km FLOAT DEFAULT 0,
    tiempo_min INTEGER DEFAULT 0,
    polyline TEXT,
    google_maps_url TEXT,
    estado VARCHAR(20) DEFAULT 'pendiente',
    total_paradas INTEGER DEFAULT 0,
    total_personas INTEGER DEFAULT 0,
    repartidor_id INTEGER,
    creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de paradas (edificios)
CREATE TABLE IF NOT EXISTS paradas (
    id SERIAL PRIMARY KEY,
    ruta_id INTEGER REFERENCES rutas(id) ON DELETE CASCADE,
    orden INTEGER,
    direccion_original TEXT,
    direccion_normalizada TEXT,
    alcaldia VARCHAR(100),
    dependencia_principal VARCHAR(255),
    latitud FLOAT,
    longitud FLOAT,
    zona VARCHAR(50)
);

-- Tabla de personas
CREATE TABLE IF NOT EXISTS personas (
    id SERIAL PRIMARY KEY,
    parada_id INTEGER REFERENCES paradas(id) ON DELETE CASCADE,
    nombre_completo VARCHAR(255),
    nombre VARCHAR(255),
    adscripcion VARCHAR(255),
    direccion TEXT,
    alcaldia VARCHAR(100),
    notas TEXT,
    estado VARCHAR(20) DEFAULT 'pendiente'
);

-- Tabla de repartidores
CREATE TABLE IF NOT EXISTS repartidores (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL,
    telefono VARCHAR(50),
    telegram_id VARCHAR(100),
    activo BOOLEAN DEFAULT TRUE
);

-- Tabla de avances (entregas)
CREATE TABLE IF NOT EXISTS avances (
    id SERIAL PRIMARY KEY,
    ruta_id INTEGER REFERENCES rutas(id),
    persona_id INTEGER REFERENCES personas(id),
    repartidor_id INTEGER REFERENCES repartidores(id),
    tipo VARCHAR(50) DEFAULT 'entrega',
    estado VARCHAR(20) DEFAULT 'pendiente',
    foto_path VARCHAR(500),
    creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de geocache (caché de geocodificación)
CREATE TABLE IF NOT EXISTS geocache (
    key VARCHAR(64) PRIMARY KEY,
    latitud FLOAT,
    longitud FLOAT,
    exitoso BOOLEAN,
    creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para búsquedas rápidas
CREATE INDEX IF NOT EXISTS idx_rutas_estado ON rutas(estado);
CREATE INDEX IF NOT EXISTS idx_paradas_ruta ON paradas(ruta_id);
CREATE INDEX IF NOT EXISTS idx_personas_parada ON personas(parada_id);
CREATE INDEX IF NOT EXISTS idx_avances_estado ON avances(estado);
