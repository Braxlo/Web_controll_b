import { Injectable } from '@nestjs/common';
import {
  Pool,
  type PoolClient,
  type QueryResult,
  type QueryResultRow,
} from 'pg';

@Injectable()
export class DatabaseService {
  private readonly url = process.env.DATABASE_URL?.trim() ?? '';
  private readonly enabled = this.url.length > 0;
  private readonly pool = this.enabled ? new Pool({ connectionString: this.url }) : null;
  private initPromise: Promise<void> | null = null;

  isEnabled() {
    return this.enabled;
  }

  async ensureReady() {
    if (!this.enabled || !this.pool) return;
    if (!this.initPromise) {
      this.initPromise = this.initSchema();
    }
    await this.initPromise;
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    sql: string,
    params: any[] = [],
  ): Promise<QueryResult<T>> {
    if (!this.pool) {
      throw new Error('DATABASE_URL no configurado');
    }
    await this.ensureReady();
    return this.pool.query<T>(sql, params);
  }

  async withTransaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    if (!this.pool) {
      throw new Error('DATABASE_URL no configurado');
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

  private async initSchema() {
    if (!this.pool) return;
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
  }
}
