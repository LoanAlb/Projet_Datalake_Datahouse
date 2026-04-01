-- ============================================
-- TABLES DE SUIVI D'INGESTION
-- ============================================

CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id            SERIAL PRIMARY KEY,
    source            VARCHAR(255) NOT NULL DEFAULT 'velib-api',
    started_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at       TIMESTAMP,
    status            VARCHAR(50) NOT NULL DEFAULT 'running',
    records_received  INT DEFAULT 0,
    records_inserted  INT DEFAULT 0
);

-- ============================================
-- TABLES MÉTIER
-- ============================================

CREATE TABLE IF NOT EXISTS stations (
    station_id          BIGINT PRIMARY KEY,
    name                VARCHAR(512),
    capacity            INT,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    commune             VARCHAR(255),
    code_insee           VARCHAR(10),
    first_seen_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    last_updated_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS station_status (
    id                      SERIAL PRIMARY KEY,
    station_id              BIGINT NOT NULL REFERENCES stations(station_id),
    snapshot_ts             TIMESTAMP NOT NULL,
    num_bikes_available     INT,
    num_docks_available     INT,
    mechanical_available    INT,
    ebike_available         INT,
    is_installed            BOOLEAN,
    is_renting              BOOLEAN,
    is_returning            BOOLEAN,
    run_id                  INT REFERENCES ingestion_runs(run_id),
    ingested_at             TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_status_station_ts ON station_status(station_id, snapshot_ts);
CREATE INDEX IF NOT EXISTS idx_status_snapshot ON station_status(snapshot_ts);

-- ============================================
-- VUES ANALYTIQUES
-- ============================================

-- KPI 1 : Taux d'occupation en temps réel par station
CREATE OR REPLACE VIEW v_station_occupancy AS
SELECT DISTINCT ON (ss.station_id)
    ss.station_id,
    s.name,
    s.capacity,
    s.lat,
    s.lon,
    s.commune,
    ss.num_bikes_available,
    ss.num_docks_available,
    ss.mechanical_available,
    ss.ebike_available,
    ss.snapshot_ts,
    CASE WHEN s.capacity > 0
        THEN ROUND((ss.num_bikes_available::NUMERIC / s.capacity) * 100, 1)
        ELSE 0
    END AS occupancy_pct
FROM station_status ss
JOIN stations s ON s.station_id = ss.station_id
ORDER BY ss.station_id, ss.snapshot_ts DESC;

-- KPI 2 : Stations critiques (< 10% ou > 90%)
CREATE OR REPLACE VIEW v_critical_stations AS
SELECT *,
    CASE
        WHEN occupancy_pct < 10 THEN 'QUASI_VIDE'
        WHEN occupancy_pct > 90 THEN 'QUASI_PLEINE'
    END AS alert_type
FROM v_station_occupancy
WHERE occupancy_pct < 10 OR occupancy_pct > 90;

-- KPI 3 : Tendance d'occupation par tranche horaire
CREATE OR REPLACE VIEW v_hourly_trend AS
SELECT
    station_id,
    EXTRACT(DOW FROM snapshot_ts)  AS day_of_week,
    EXTRACT(HOUR FROM snapshot_ts) AS hour_of_day,
    ROUND(AVG(CASE WHEN capacity > 0
        THEN (num_bikes_available::NUMERIC / capacity) * 100
        ELSE 0 END), 1) AS avg_occupancy_pct,
    COUNT(*) AS nb_snapshots
FROM station_status ss
JOIN stations s USING (station_id)
GROUP BY station_id, EXTRACT(DOW FROM snapshot_ts), EXTRACT(HOUR FROM snapshot_ts);

-- KPI 4 : Temps passé en état critique par station (nb de snapshots critiques)
CREATE OR REPLACE VIEW v_critical_duration AS
SELECT
    ss.station_id,
    s.name,
    s.commune,
    COUNT(*) FILTER (WHERE
        CASE WHEN s.capacity > 0
            THEN (ss.num_bikes_available::NUMERIC / s.capacity) * 100
            ELSE 0 END < 10
    ) AS snapshots_quasi_vide,
    COUNT(*) FILTER (WHERE
        CASE WHEN s.capacity > 0
            THEN (ss.num_bikes_available::NUMERIC / s.capacity) * 100
            ELSE 0 END > 90
    ) AS snapshots_quasi_pleine,
    COUNT(*) AS total_snapshots
FROM station_status ss
JOIN stations s USING (station_id)
GROUP BY ss.station_id, s.name, s.commune;

-- KPI 5 : Ratio mécanique vs électrique
CREATE OR REPLACE VIEW v_bike_type_ratio AS
SELECT DISTINCT ON (ss.station_id)
    ss.station_id,
    s.name,
    ss.mechanical_available,
    ss.ebike_available,
    ss.num_bikes_available,
    CASE WHEN ss.num_bikes_available > 0
        THEN ROUND((ss.ebike_available::NUMERIC / ss.num_bikes_available) * 100, 1)
        ELSE 0
    END AS ebike_pct,
    ss.snapshot_ts
FROM station_status ss
JOIN stations s USING (station_id)
ORDER BY ss.station_id, ss.snapshot_ts DESC;

-- KPI 6 : Résumé global pour dashboard / Telegram
CREATE OR REPLACE VIEW v_global_summary AS
SELECT
    COUNT(DISTINCT station_id) AS total_stations,
    SUM(num_bikes_available) AS total_bikes,
    SUM(mechanical_available) AS total_mechanical,
    SUM(ebike_available) AS total_ebike,
    SUM(num_docks_available) AS total_docks,
    COUNT(*) FILTER (WHERE occupancy_pct < 10) AS stations_quasi_vides,
    COUNT(*) FILTER (WHERE occupancy_pct > 90) AS stations_quasi_pleines,
    ROUND(AVG(occupancy_pct), 1) AS avg_occupancy_pct
FROM v_station_occupancy;
