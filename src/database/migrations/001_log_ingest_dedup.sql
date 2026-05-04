-- Dedup en ingesta: hash estable por fila + indice unico (device_id, ingest_hash)
-- Filas existentes reciben hash legacy unico por pk para no chocar con nuevas inserciones.

ALTER TABLE log_eventos ADD COLUMN IF NOT EXISTS ingest_hash TEXT;
UPDATE log_eventos SET ingest_hash = 'mig-ev-' || pk::text WHERE ingest_hash IS NULL;
ALTER TABLE log_eventos ALTER COLUMN ingest_hash SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_log_eventos_device_ingest_hash ON log_eventos(device_id, ingest_hash);

ALTER TABLE log_hw ADD COLUMN IF NOT EXISTS ingest_hash TEXT;
UPDATE log_hw SET ingest_hash = 'mig-hw-' || pk::text WHERE ingest_hash IS NULL;
ALTER TABLE log_hw ALTER COLUMN ingest_hash SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_log_hw_device_ingest_hash ON log_hw(device_id, ingest_hash);

ALTER TABLE log_energia ADD COLUMN IF NOT EXISTS ingest_hash TEXT;
UPDATE log_energia SET ingest_hash = 'mig-en-' || pk::text WHERE ingest_hash IS NULL;
ALTER TABLE log_energia ALTER COLUMN ingest_hash SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_log_energia_device_ingest_hash ON log_energia(device_id, ingest_hash);
