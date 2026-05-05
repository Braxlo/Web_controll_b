-- Estado vivo por dispositivo para lecturas rapidas del dashboard.
-- Guarda el ultimo log conocido y contadores precomputados.

CREATE TABLE IF NOT EXISTS device_live_state (
  device_id TEXT PRIMARY KEY,
  energia_total_rows BIGINT NOT NULL DEFAULT 0,
  energia_last JSONB NULL,
  energia_last_at TEXT NULL,
  accesos_total_rows BIGINT NOT NULL DEFAULT 0,
  accesos_ok_ultimas_24h INT NOT NULL DEFAULT 0,
  accesos_last JSONB NULL,
  accesos_last_at TEXT NULL,
  hardware_total_rows BIGINT NOT NULL DEFAULT 0,
  hardware_last JSONB NULL,
  hardware_last_at TEXT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_device_live_state_updated_at
  ON device_live_state(updated_at DESC);
