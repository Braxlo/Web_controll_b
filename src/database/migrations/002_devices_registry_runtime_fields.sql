ALTER TABLE devices_registry
  ADD COLUMN IF NOT EXISTS api_key TEXT;

UPDATE devices_registry
SET api_key = md5(device_id || ':' || now()::text)
WHERE api_key IS NULL OR btrim(api_key) = '';

ALTER TABLE devices_registry
  ALTER COLUMN api_key SET NOT NULL;

ALTER TABLE devices_registry
  ADD COLUMN IF NOT EXISTS connection_status TEXT NOT NULL DEFAULT 'pending';

ALTER TABLE devices_registry
  ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NULL;

ALTER TABLE devices_registry
  ADD COLUMN IF NOT EXISTS credentials_version INT NOT NULL DEFAULT 1;

ALTER TABLE devices_registry
  ADD COLUMN IF NOT EXISTS credentials_sync_status TEXT NOT NULL DEFAULT 'pending';
