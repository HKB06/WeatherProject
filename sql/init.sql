-- Script d'initialisation de la base de données PostgreSQL
-- Base : weather_metadata

-- Créer l'extension pour UUID si nécessaire
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Table des métadonnées d'ingestion
CREATE TABLE IF NOT EXISTS ingestion_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_type VARCHAR(50) NOT NULL CHECK (source_type IN ('CSV', 'API', 'JSON', 'OTHER')),
    source_name VARCHAR(255) NOT NULL,
    source_path TEXT,
    
    -- Statistiques d'ingestion
    records_count INTEGER NOT NULL DEFAULT 0,
    records_valid INTEGER NOT NULL DEFAULT 0,
    records_invalid INTEGER NOT NULL DEFAULT 0,
    file_size_mb NUMERIC(10, 2),
    processing_duration_seconds NUMERIC(10, 2),
    
    -- Qualité des données
    data_quality_score NUMERIC(5, 2) CHECK (data_quality_score >= 0 AND data_quality_score <= 100),
    
    -- Informations de traitement
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'SUCCESS', 'FAILED', 'PARTIAL')),
    error_message TEXT,
    
    -- Période couverte par les données
    data_start_date DATE,
    data_end_date DATE,
    
    -- Métadonnées additionnelles (JSON)
    additional_info JSONB,
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index pour améliorer les performances
CREATE INDEX idx_ingestion_timestamp ON ingestion_metadata(ingestion_timestamp DESC);
CREATE INDEX idx_source_type ON ingestion_metadata(source_type);
CREATE INDEX idx_status ON ingestion_metadata(status);
CREATE INDEX idx_data_dates ON ingestion_metadata(data_start_date, data_end_date);

-- Table des transformations Spark
CREATE TABLE IF NOT EXISTS spark_transformations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ingestion_id UUID REFERENCES ingestion_metadata(id) ON DELETE CASCADE,
    transformation_name VARCHAR(100) NOT NULL,
    transformation_type VARCHAR(50) NOT NULL,
    
    -- Statistiques de transformation
    input_records INTEGER NOT NULL,
    output_records INTEGER NOT NULL,
    records_dropped INTEGER DEFAULT 0,
    processing_time_seconds NUMERIC(10, 2),
    
    -- Configuration utilisée
    config JSONB,
    
    -- Résultats
    output_path TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
    error_message TEXT,
    
    -- Audit
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    
    CONSTRAINT valid_records CHECK (output_records <= input_records)
);

CREATE INDEX idx_transformation_ingestion ON spark_transformations(ingestion_id);
CREATE INDEX idx_transformation_status ON spark_transformations(status);

-- Table des agrégations calculées
CREATE TABLE IF NOT EXISTS calculated_aggregations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transformation_id UUID REFERENCES spark_transformations(id) ON DELETE CASCADE,
    
    aggregation_type VARCHAR(50) NOT NULL CHECK (aggregation_type IN ('DAILY', 'MONTHLY', 'SEASONAL', 'YEARLY', 'CUSTOM')),
    metric_name VARCHAR(100) NOT NULL,
    
    -- Période d'agrégation
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    
    -- Valeurs calculées
    value_mean NUMERIC(10, 2),
    value_min NUMERIC(10, 2),
    value_max NUMERIC(10, 2),
    value_std NUMERIC(10, 2),
    value_count INTEGER,
    
    -- Métadonnées
    location VARCHAR(255),
    additional_dimensions JSONB,
    
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_agg_type_period ON calculated_aggregations(aggregation_type, period_start, period_end);
CREATE INDEX idx_agg_metric ON calculated_aggregations(metric_name);
CREATE INDEX idx_agg_location ON calculated_aggregations(location);

-- Table pour le monitoring de la qualité des données
CREATE TABLE IF NOT EXISTS data_quality_checks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ingestion_id UUID REFERENCES ingestion_metadata(id) ON DELETE CASCADE,
    
    check_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(50) NOT NULL,
    
    -- Résultats
    passed BOOLEAN NOT NULL,
    total_records INTEGER NOT NULL,
    failed_records INTEGER DEFAULT 0,
    
    -- Détails
    threshold_value NUMERIC(10, 2),
    actual_value NUMERIC(10, 2),
    details JSONB,
    
    checked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_quality_ingestion ON data_quality_checks(ingestion_id);
CREATE INDEX idx_quality_passed ON data_quality_checks(passed);

-- Table des logs système
CREATE TABLE IF NOT EXISTS system_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    log_level VARCHAR(20) NOT NULL CHECK (log_level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    component VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_logs_level ON system_logs(log_level);
CREATE INDEX idx_logs_component ON system_logs(component);
CREATE INDEX idx_logs_created ON system_logs(created_at DESC);

-- Fonction pour mettre à jour automatiquement updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger pour la mise à jour automatique
CREATE TRIGGER update_ingestion_metadata_updated_at
    BEFORE UPDATE ON ingestion_metadata
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Vue pour un résumé des ingestions
CREATE OR REPLACE VIEW ingestion_summary AS
SELECT 
    source_type,
    COUNT(*) as total_ingestions,
    SUM(records_count) as total_records,
    AVG(data_quality_score) as avg_quality_score,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_ingestions,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_ingestions,
    MAX(ingestion_timestamp) as last_ingestion
FROM ingestion_metadata
GROUP BY source_type;

-- Vue pour les performances de transformation
CREATE OR REPLACE VIEW transformation_performance AS
SELECT 
    t.transformation_name,
    t.transformation_type,
    COUNT(*) as total_runs,
    AVG(t.processing_time_seconds) as avg_processing_time,
    AVG(t.output_records::NUMERIC / NULLIF(t.input_records, 0) * 100) as avg_retention_rate,
    SUM(CASE WHEN t.status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs
FROM spark_transformations t
GROUP BY t.transformation_name, t.transformation_type;

-- Insérer des données de test (optionnel - pour développement)
INSERT INTO system_logs (log_level, component, message, details)
VALUES 
    ('INFO', 'DATABASE', 'Database initialized successfully', '{"version": "1.0", "environment": "development"}'),
    ('INFO', 'DATABASE', 'Tables and indexes created', '{"tables": 6, "views": 2}');

-- Grant des permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO weather_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO weather_admin;

-- Message de confirmation
DO $$
BEGIN
    RAISE NOTICE 'Weather Metadata Database initialized successfully!';
END $$;

