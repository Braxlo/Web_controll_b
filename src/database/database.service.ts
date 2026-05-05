import { Injectable, Logger } from '@nestjs/common';
import { existsSync, readFileSync, readdirSync } from 'fs';
import { join } from 'path';
import {
  Pool,
  type PoolClient,
  type QueryResult,
  type QueryResultRow,
} from 'pg';
import { createHash } from 'crypto';

@Injectable()
export class DatabaseService {
  private readonly logger = new Logger(DatabaseService.name);
  private readonly url = this.resolveConnectionString();
  private readonly enabled = this.url.length > 0;
  private readonly synchronize = this.resolveSynchronizeFlag();
  private readonly pool = this.enabled
    ? new Pool({ connectionString: this.url })
    : null;
  private initPromise: Promise<void> | null = null;

  isEnabled() {
    return this.enabled;
  }

  async ensureReady() {
    if (!this.enabled || !this.pool) return;
    if (!this.initPromise) {
      this.logger.log(
        `Base de datos habilitada (DB_SYNCHRONIZE=${this.synchronize ? 'true' : 'false'})`,
      );
      this.initPromise = (async () => {
        if (this.synchronize) {
          await this.initSchema();
        } else {
          this.logger.log(
            'DB_SYNCHRONIZE=false: se omite la creación/verificación automática de tablas.',
          );
        }
        await this.runPendingMigrations();
      })();
    }
    await this.initPromise;
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    sql: string,
    params: any[] = [],
  ): Promise<QueryResult<T>> {
    if (!this.pool) {
      throw new Error(
        'Base de datos no configurada (DATABASE_URL o DB_HOST/DB_PORT/DB_USERNAME/DB_PASSWORD/DB_NAME)',
      );
    }
    await this.ensureReady();
    return this.pool.query<T>(sql, params);
  }

  async withTransaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    if (!this.pool) {
      throw new Error(
        'Base de datos no configurada (DATABASE_URL o DB_HOST/DB_PORT/DB_USERNAME/DB_PASSWORD/DB_NAME)',
      );
    }
    await this.ensureReady();
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const out = await fn(client);
      await client.query('COMMIT');
      return out;
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  async healthCheck() {
    if (!this.pool) {
      return {
        enabled: false,
        ok: true,
        latencyMs: 0,
        tables: {} as Record<string, boolean>,
      };
    }

    const startedAt = Date.now();
    await this.ensureReady();
    await this.pool.query('SELECT 1');

    const requiredTables = [
      'devices_registry',
      'modules_config',
      'admin_users',
      'credenciales',
      'log_energia',
      'log_eventos',
      'log_hw',
      'device_live_state',
    ] as const;

    const existing = await this.pool.query<{ tablename: string }>(
      `SELECT tablename
         FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename = ANY($1::text[])`,
      [requiredTables],
    );

    const existingSet = new Set(existing.rows.map((r) => r.tablename));
    const tables = Object.fromEntries(
      requiredTables.map((name) => [name, existingSet.has(name)]),
    ) as Record<string, boolean>;

    return {
      enabled: true,
      ok: Object.values(tables).every(Boolean),
      latencyMs: Date.now() - startedAt,
      tables,
    };
  }

  private resolveConnectionString() {
    const direct = process.env.DATABASE_URL?.trim() ?? '';
    if (direct.length > 0) return direct;

    const host = process.env.DB_HOST?.trim() ?? '';
    const port = process.env.DB_PORT?.trim() ?? '5432';
    const user = process.env.DB_USERNAME?.trim() ?? '';
    const password = process.env.DB_PASSWORD?.trim() ?? '';
    const dbName = process.env.DB_NAME?.trim() ?? '';
    if (!host || !user || !dbName) return '';
    return `postgresql://${encodeURIComponent(user)}:${encodeURIComponent(password)}@${host}:${port}/${dbName}`;
  }

  private resolveSynchronizeFlag() {
    const raw = process.env.DB_SYNCHRONIZE?.trim().toLowerCase();
    if (!raw) return true;
    return raw === 'true' || raw === '1' || raw === 'yes';
  }

  private resolveRunMigrationsFlag() {
    const raw = process.env.DB_RUN_MIGRATIONS?.trim().toLowerCase();
    if (!raw) return true;
    return raw === 'true' || raw === '1' || raw === 'yes';
  }

  private async runPendingMigrations() {
    if (!this.pool) return;
    if (!this.resolveRunMigrationsFlag()) {
      this.logger.warn('DB_RUN_MIGRATIONS=false: se omiten migraciones SQL.');
      return;
    }
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
    const dir = join(__dirname, 'migrations');
    if (!existsSync(dir)) {
      this.logger.warn(`Carpeta de migraciones no encontrada: ${dir}`);
      return;
    }
    const names = readdirSync(dir)
      .filter((f) => f.endsWith('.sql'))
      .sort();
    for (const name of names) {
      const done = await this.pool.query(
        `SELECT 1 FROM schema_migrations WHERE version = $1`,
        [name],
      );
      if ((done.rowCount ?? 0) > 0) continue;
      const sql = readFileSync(join(dir, name), 'utf8');
      const client = await this.pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(sql);
        await client.query(
          `INSERT INTO schema_migrations (version) VALUES ($1)`,
          [name],
        );
        await client.query('COMMIT');
        this.logger.log(`Migracion aplicada: ${name}`);
      } catch (e) {
        await client.query('ROLLBACK');
        this.logger.error(`Migracion fallida: ${name}`, e as Error);
        throw e;
      } finally {
        client.release();
      }
    }
  }

  private async ensureDefaultAdmin() {
    if (!this.pool) return;
    const username = process.env.DB_DEFAULT_ADMIN_USERNAME?.trim();
    const password = process.env.DB_DEFAULT_ADMIN_PASSWORD?.trim();
    if (!username || !password) {
      this.logger.log(
        'Admin por defecto omitido (DB_DEFAULT_ADMIN_USERNAME/DB_DEFAULT_ADMIN_PASSWORD no definidos).',
      );
      return;
    }
    const passwordHash = createHash('sha256').update(password).digest('hex');
    await this.pool.query(
      `INSERT INTO admin_users (username, password_hash, is_active, created_at, updated_at)
       VALUES ($1, $2, true, now(), now())
       ON CONFLICT (username) DO NOTHING`,
      [username, passwordHash],
    );
    this.logger.log(`Admin por defecto verificado para usuario: ${username}`);
  }

  private async initSchema() {
    if (!this.pool) return;
    this.logger.log('Iniciando verificación/creación de tablas...');
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS devices_registry (
        device_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        host TEXT NOT NULL,
        panel_port INT NOT NULL,
        api_key TEXT NOT NULL,
        connection_status TEXT NOT NULL DEFAULT 'pending',
        last_seen_at TIMESTAMPTZ NULL,
        credentials_version INT NOT NULL DEFAULT 1,
        credentials_sync_status TEXT NOT NULL DEFAULT 'pending',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS modules_config (
        config_key TEXT PRIMARY KEY,
        config_value JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS admin_users (
        id BIGSERIAL PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        is_active BOOLEAN NOT NULL DEFAULT true,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS credenciales (
        device_id TEXT NOT NULL,
        id TEXT NOT NULL,
        tipo TEXT NOT NULL,
        nivel INT NOT NULL,
        usuario TEXT NOT NULL DEFAULT '',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY(device_id, id)
      );
    `);
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS log_energia (
        pk BIGSERIAL PRIMARY KEY,
        device_id TEXT NOT NULL,
        timestamp_text TEXT NOT NULL,
        vs DOUBLE PRECISION NOT NULL,
        cs DOUBLE PRECISION NOT NULL,
        sw DOUBLE PRECISION NOT NULL,
        vb DOUBLE PRECISION NOT NULL,
        cb DOUBLE PRECISION NOT NULL,
        lv DOUBLE PRECISION NOT NULL,
        lc DOUBLE PRECISION NOT NULL,
        lp DOUBLE PRECISION NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS log_eventos (
        pk BIGSERIAL PRIMARY KEY,
        device_id TEXT NOT NULL,
        fecha TEXT NOT NULL,
        id_persona TEXT NOT NULL,
        usuario_persona TEXT NOT NULL,
        id_vehiculo TEXT NOT NULL,
        usuario_vehiculo TEXT NOT NULL,
        resultado TEXT NOT NULL,
        direccion TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS log_hw (
        pk BIGSERIAL PRIMARY KEY,
        device_id TEXT NOT NULL,
        fecha TEXT NOT NULL,
        lectora TEXT NOT NULL,
        evento TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
    await this.pool.query(`
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
    `);
    await this.pool.query(
      `CREATE INDEX IF NOT EXISTS idx_log_energia_device_pk ON log_energia(device_id, pk DESC);`,
    );
    await this.pool.query(
      `CREATE INDEX IF NOT EXISTS idx_log_eventos_device_pk ON log_eventos(device_id, pk DESC);`,
    );
    await this.pool.query(
      `CREATE INDEX IF NOT EXISTS idx_log_hw_device_pk ON log_hw(device_id, pk DESC);`,
    );
    await this.pool.query(
      `CREATE INDEX IF NOT EXISTS idx_device_live_state_updated_at ON device_live_state(updated_at DESC);`,
    );
    await this.ensureDefaultAdmin();
    this.logger.log(
      'Esquema DB verificado: devices_registry, modules_config, admin_users, credenciales, log_energia, log_eventos, log_hw, device_live_state.',
    );
  }
}
