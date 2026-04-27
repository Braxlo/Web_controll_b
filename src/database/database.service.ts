import { Injectable, Logger } from '@nestjs/common';
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
  private readonly pool = this.enabled ? new Pool({ connectionString: this.url }) : null;
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
      this.initPromise = this.synchronize ? this.initSchema() : Promise.resolve();
      if (!this.synchronize) {
        this.logger.log(
          'DB_SYNCHRONIZE=false: se omite la creación/verificación automática de tablas.',
        );
      }
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
    await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_log_energia_device_pk ON log_energia(device_id, pk DESC);`);
    await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_log_eventos_device_pk ON log_eventos(device_id, pk DESC);`);
    await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_log_hw_device_pk ON log_hw(device_id, pk DESC);`);
    await this.ensureDefaultAdmin();
    this.logger.log(
      'Esquema DB verificado: devices_registry, modules_config, admin_users, credenciales, log_energia, log_eventos, log_hw.',
    );
  }
}
