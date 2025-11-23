-- Table pour stocker les records personnels de l'utilisateur
-- Permet de ne pas recalculer tous les records à chaque fois

CREATE TABLE IF NOT EXISTS records (
    id SERIAL PRIMARY KEY,
    distance_key VARCHAR(20) NOT NULL UNIQUE,  -- '5k', '10k', 'semi', '30k', 'marathon'
    distance_km DECIMAL(10, 4) NOT NULL,       -- Distance exacte en km
    time_seconds INTEGER NOT NULL,             -- Temps du record en secondes
    pace_seconds_per_km DECIMAL(10, 2),        -- Allure en secondes par km
    activity_id VARCHAR(50) NOT NULL,          -- ID de l'activité Strava
    activity_name TEXT,                         -- Nom de l'activité
    activity_date DATE NOT NULL,                -- Date de l'activité
    start_km DECIMAL(10, 2),                    -- Début du segment dans l'activité
    end_km DECIMAL(10, 2),                      -- Fin du segment dans l'activité
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Dernière mise à jour

    CONSTRAINT valid_distance CHECK (distance_key IN ('5k', '10k', 'semi', '30k', 'marathon'))
);

-- Index pour accélérer les requêtes
CREATE INDEX IF NOT EXISTS idx_records_distance_key ON records(distance_key);
CREATE INDEX IF NOT EXISTS idx_records_updated_at ON records(updated_at);

-- Commentaires pour documentation
COMMENT ON TABLE records IS 'Stocke les records personnels de l''utilisateur pour éviter les recalculs';
COMMENT ON COLUMN records.distance_key IS 'Clé unique pour la distance (5k, 10k, semi, 30k, marathon)';
COMMENT ON COLUMN records.time_seconds IS 'Temps du record en secondes';
COMMENT ON COLUMN records.start_km IS 'Kilomètre de début du segment record dans l''activité source';
COMMENT ON COLUMN records.end_km IS 'Kilomètre de fin du segment record dans l''activité source';
