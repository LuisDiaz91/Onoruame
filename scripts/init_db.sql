-- ============================================================
-- ONORUAME - Schema PostgreSQL
-- Ejecutar una vez al inicializar la base de datos.
-- Idempotente: se puede correr múltiples veces sin errores.
-- ============================================================

-- Extensiones
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";   -- búsqueda fuzzy de nombres

-- ============================================================
-- REPARTIDORES
-- ============================================================
CREATE TABLE IF NOT EXISTS repartidores (
    id          UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    nombre      VARCHAR(150) NOT NULL,
    telefono    VARCHAR(20),
    telegram_id VARCHAR(50),               -- ID de Telegram para el bot
    activo      BOOLEAN     DEFAULT TRUE,
    creado_en   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(nombre)
);

-- ============================================================
-- RUTAS
-- ============================================================
CREATE TABLE IF NOT EXISTS rutas (
    id               SERIAL      PRIMARY KEY,
    zona             VARCHAR(50) NOT NULL,
    origen_nombre    VARCHAR(200),
    origen_coords    VARCHAR(60),
    estado           VARCHAR(30) DEFAULT 'pendiente'
                         CHECK (estado IN (
                             'pendiente','asignada','en_progreso',
                             'completada','cancelada'
                         )),
    repartidor_id    UUID        REFERENCES repartidores(id) ON DELETE SET NULL,
    google_maps_url  TEXT,
    polyline_data    TEXT,
    distancia_km     NUMERIC(8,2) DEFAULT 0,
    tiempo_min       INTEGER      DEFAULT 0,
    total_paradas    INTEGER      DEFAULT 0,
    total_personas   INTEGER      DEFAULT 0,
    ruta_hash        VARCHAR(32) UNIQUE,    -- MD5 del contenido, evita duplicados
    fecha_asignacion TIMESTAMPTZ,
    fecha_inicio     TIMESTAMPTZ,
    fecha_completada TIMESTAMPTZ,
    creado_en        TIMESTAMPTZ DEFAULT NOW(),
    actualizado_en   TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- PARADAS  (una parada = un edificio/dirección)
-- ============================================================
CREATE TABLE IF NOT EXISTS paradas (
    id                    SERIAL  PRIMARY KEY,
    ruta_id               INTEGER NOT NULL REFERENCES rutas(id) ON DELETE CASCADE,
    orden                 INTEGER NOT NULL,
    direccion_original    TEXT    NOT NULL,
    direccion_normalizada TEXT,
    alcaldia              VARCHAR(100),
    dependencia_principal VARCHAR(200),
    lat                   NUMERIC(10,7),
    lng                   NUMERIC(10,7),
    estado                VARCHAR(30) DEFAULT 'pendiente'
                              CHECK (estado IN ('pendiente','visitada','omitida')),
    creado_en             TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ruta_id, orden)
);

-- ============================================================
-- PERSONAS  (dentro de cada parada)
-- ============================================================
CREATE TABLE IF NOT EXISTS personas (
    id              SERIAL  PRIMARY KEY,
    parada_id       INTEGER NOT NULL REFERENCES paradas(id) ON DELETE CASCADE,
    sub_orden       INTEGER NOT NULL,
    nombre_completo VARCHAR(300) NOT NULL,
    nombre          VARCHAR(200),
    adscripcion     VARCHAR(300),
    direccion       TEXT,
    alcaldia        VARCHAR(100),
    notas           TEXT,
    foto_path       TEXT,                  -- ruta local de la foto de acuse
    fecha_entrega   TIMESTAMPTZ,
    estado          VARCHAR(30) DEFAULT 'pendiente'
                        CHECK (estado IN ('pendiente','entregado','no_entregado')),
    creado_en       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(parada_id, sub_orden)
);

-- ============================================================
-- AVANCES / ENTREGAS  (registros del bot de Telegram)
-- ============================================================
CREATE TABLE IF NOT EXISTS avances (
    id            UUID    PRIMARY KEY DEFAULT uuid_generate_v4(),
    ruta_id       INTEGER NOT NULL REFERENCES rutas(id) ON DELETE CASCADE,
    parada_id     INTEGER REFERENCES paradas(id)     ON DELETE SET NULL,
    persona_id    INTEGER REFERENCES personas(id)    ON DELETE SET NULL,
    repartidor_id UUID    REFERENCES repartidores(id) ON DELETE SET NULL,
    tipo          VARCHAR(30) DEFAULT 'entrega'
                      CHECK (tipo IN ('entrega','reporte','simulacion')),
    estado        VARCHAR(30) DEFAULT 'pendiente'
                      CHECK (estado IN ('pendiente','procesado')),
    foto_path     TEXT,
    notas         TEXT,
    timestamp_bot TIMESTAMPTZ,            -- cuando llegó desde Telegram
    procesado_en  TIMESTAMPTZ,
    creado_en     TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- GEOCODING CACHE  (reemplaza el JSON en disco)
-- ============================================================
CREATE TABLE IF NOT EXISTS geocoding_cache (
    cache_key  VARCHAR(64)  PRIMARY KEY,  -- MD5 de "dirección_alcaldía"
    direccion  TEXT         NOT NULL,
    alcaldia   VARCHAR(100),
    lat        NUMERIC(10,7),
    lng        NUMERIC(10,7),
    exito      BOOLEAN      NOT NULL,     -- false = fallo conocido, no reintentar
    estrategia VARCHAR(20),               -- 'exactas' | 'aproximadas'
    creado_en  TIMESTAMPTZ  DEFAULT NOW(),
    usado_en   TIMESTAMPTZ  DEFAULT NOW() -- para limpieza LRU
);

-- ============================================================
-- ÍNDICES
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_rutas_estado         ON rutas(estado);
CREATE INDEX IF NOT EXISTS idx_rutas_repartidor     ON rutas(repartidor_id);
CREATE INDEX IF NOT EXISTS idx_paradas_ruta         ON paradas(ruta_id);
CREATE INDEX IF NOT EXISTS idx_personas_parada      ON personas(parada_id);
CREATE INDEX IF NOT EXISTS idx_personas_estado      ON personas(estado);
CREATE INDEX IF NOT EXISTS idx_avances_ruta         ON avances(ruta_id);
CREATE INDEX IF NOT EXISTS idx_avances_estado       ON avances(estado);
CREATE INDEX IF NOT EXISTS idx_geocache_usado       ON geocoding_cache(usado_en);

-- Búsqueda fuzzy por nombre (pg_trgm)
CREATE INDEX IF NOT EXISTS idx_personas_nombre_trgm
    ON personas USING gin(nombre gin_trgm_ops);

-- ============================================================
-- TRIGGER: actualizado_en en rutas
-- ============================================================
CREATE OR REPLACE FUNCTION set_actualizado_en()
RETURNS TRIGGER AS $$
BEGIN
    NEW.actualizado_en = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_rutas_actualizado ON rutas;
CREATE TRIGGER trg_rutas_actualizado
    BEFORE UPDATE ON rutas
    FOR EACH ROW EXECUTE FUNCTION set_actualizado_en();

-- ============================================================
-- DATOS INICIALES
-- ============================================================
INSERT INTO repartidores (nombre) VALUES
    ('Repartidor 1'),
    ('Repartidor 2'),
    ('Repartidor 3')
ON CONFLICT (nombre) DO NOTHING;
