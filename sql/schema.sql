-- Optional reference schema for PostgreSQL / dbase.id
-- Tables are also auto-created by SQLAlchemy during ingestion.

CREATE TABLE IF NOT EXISTS etl_ingestion_log (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT,
    report_type TEXT,
    table_name TEXT,
    raw_rows BIGINT,
    clean_rows BIGINT,
    raw_csv TEXT,
    clean_csv TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
