import { BadRequestException, Injectable } from '@nestjs/common';
import { createHash } from 'crypto';
import { appendFile, mkdir, readFile, writeFile } from 'fs/promises';
import type { PoolClient } from 'pg';
import { join } from 'path';
import { ConfigService } from '../config/config.service';
import { DatabaseService } from '../database/database.service';

const ENERGIA_HEADER = 'timestamp,VS,CS,SW,VB,CB,LV,LC,LP';
const EVENTOS_HEADER =
  'fecha,id_persona,usuario_persona,id_vehiculo,usuario_vehiculo,resultado,direccion';
const HW_HEADER = 'fecha,lectora,evento';
const CRED_HEADER = 'id,tipo,nivel,usuario';

/** Resumen JSON para el panel (credenciales, energía, accesos, hardware). */
export type BarrierIngestSummary = {
  deviceId: string;
  generatedAt: string;
  credenciales: {
    totalRows: number;
    preview: Array<{
      id: string;
      tipo: string;
      nivel: number;
      usuario: string;
    }>;
  };
  energia: {
    totalRows: number;
    last: {
      timestamp: string;
      VS: number;
      CS: number;
      SW: number;
      VB: number;
      CB: number;
      LV: number;
      LC: number;
      LP: number;
    } | null;
  };
  accesos: {
    totalRows: number;
    accesosOkUltimas24h: number;
    recent: Array<{
      fecha: string;
      id_persona: string;
      usuario_persona: string;
      id_vehiculo: string;
      usuario_vehiculo: string;
      resultado: string;
      direccion: string;
    }>;
  };
  hardware: {
    totalRows: number;
    recent: Array<{
      fecha: string;
      lectora: string;
      evento: string;
    }>;
  };
};

/** Snapshot rapido para dashboard (ultimo dato + contadores). */
export type BarrierIngestLive = {
  deviceId: string;
  generatedAt: string;
  updatedAt: string | null;
  source: 'db_live_state' | 'computed';
  energia: {
    totalRows: number;
    last: BarrierIngestSummary['energia']['last'];
  };
  accesos: {
    totalRows: number;
    accesosOkUltimas24h: number;
    last:
      | {
          fecha: string;
          id_persona: string;
          usuario_persona: string;
          id_vehiculo: string;
          usuario_vehiculo: string;
          resultado: string;
          direccion: string;
        }
      | null;
  };
  hardware: {
    totalRows: number;
    last:
      | {
          fecha: string;
          lectora: string;
          evento: string;
        }
      | null;
  };
};

@Injectable()
export class IngestionService {
  constructor(
    private readonly db: DatabaseService,
    private readonly config: ConfigService,
  ) {}

  private readonly dataRoot = join(
    process.cwd(),
    process.env.INGEST_DATA_DIR ?? 'data',
    'devices',
  );

  private devicePath(deviceId: string) {
    const safe = deviceId.replace(/[^a-zA-Z0-9_-]/g, '');
    if (!safe || safe !== deviceId) {
      throw new BadRequestException(
        'deviceId invalido: use solo letras, numeros, guion y underscore',
      );
    }
    return join(this.dataRoot, safe);
  }

  async putCredenciales(deviceId: string, csv: string) {
    if (this.db.isEnabled()) {
      const rows = this.parseCsvByHeader(
        this.normalizeCsv(csv, CRED_HEADER),
        CRED_HEADER,
      );
      if (rows.length === 0) {
        throw new BadRequestException(
          'credenciales sin filas de datos: se rechaza para evitar vaciar la barrera',
        );
      }
      await this.db.withTransaction(async (client: PoolClient) => {
        await client.query(`DELETE FROM credenciales WHERE device_id = $1`, [
          deviceId,
        ]);
        for (const r of rows) {
          await client.query(
            `INSERT INTO credenciales (device_id, id, tipo, nivel, usuario, updated_at)
             VALUES ($1, $2, $3, $4, $5, now())`,
            [
              deviceId,
              (r.id ?? '').trim(),
              (r.tipo ?? '').trim().toLowerCase(),
              Number.parseInt((r.nivel ?? '0').trim(), 10) || 0,
              (r.usuario ?? '').trim(),
            ],
          );
        }
      });
      const result = { ok: true, file: 'credenciales.csv', rows: rows.length };
      await this.config.markCredentialsPending(deviceId);
      return result;
    }
    const dir = await this.ensureDir(deviceId);
    const text = this.normalizeCsv(csv, CRED_HEADER);
    await writeFile(join(dir, 'credenciales.csv'), text, 'utf8');
    const result = {
      ok: true,
      file: 'credenciales.csv',
      bytes: Buffer.byteLength(text),
    };
    await this.config.markCredentialsPending(deviceId);
    return result;
  }

  async listCredenciales(deviceId: string) {
    if (this.db.isEnabled()) {
      const rs = await this.db.query<{
        id: string;
        tipo: string;
        nivel: number;
        usuario: string;
      }>(
        `SELECT id, tipo, nivel, usuario
           FROM credenciales
          WHERE device_id = $1
          ORDER BY id ASC`,
        [deviceId],
      );
      return { deviceId, rows: rs.rows };
    }
    const dataset = await this.readLog(deviceId, 'credenciales.csv', 5000);
    const rows = dataset.rows
      .map((r) => ({
        id: (r.id ?? '').trim(),
        tipo: (r.tipo ?? '').trim().toLowerCase(),
        nivel: Number.parseInt((r.nivel ?? '').trim(), 10),
        usuario: (r.usuario ?? '').trim(),
      }))
      .filter((r) => r.id.length > 0);
    return { deviceId, rows };
  }

  async upsertCredencial(
    deviceId: string,
    body: { id: string; tipo: string; nivel: number; usuario?: string },
  ) {
    const id = (body.id ?? '').trim();
    const tipo = (body.tipo ?? '').trim().toLowerCase();
    const nivel = Number(body.nivel);
    const usuario = (body.usuario ?? '').trim();

    if (!/^\d{8}$/.test(id)) {
      throw new BadRequestException('id debe tener 8 digitos');
    }
    if (!['p', 'v'].includes(tipo)) {
      throw new BadRequestException('tipo debe ser p o v');
    }
    if (![0, 1, 2].includes(nivel)) {
      throw new BadRequestException('nivel debe ser 0, 1 o 2');
    }

    if (this.db.isEnabled()) {
      await this.db.query(
        `INSERT INTO credenciales (device_id, id, tipo, nivel, usuario, updated_at)
         VALUES ($1, $2, $3, $4, $5, now())
         ON CONFLICT (device_id, id)
         DO UPDATE SET tipo = EXCLUDED.tipo, nivel = EXCLUDED.nivel, usuario = EXCLUDED.usuario, updated_at = now()`,
        [deviceId, id, tipo, nivel, usuario],
      );
      await this.config.markCredentialsPending(deviceId);
      return { ok: true, deviceId, id };
    }

    const current = await this.listCredenciales(deviceId);
    const rows = current.rows.filter((r) => r.id !== id);
    rows.push({ id, tipo, nivel, usuario });
    rows.sort((a, b) => (a.id > b.id ? 1 : -1));
    const csv = [
      CRED_HEADER,
      ...rows.map(
        (r) =>
          `${r.id},${r.tipo},${r.nivel},${(r.usuario ?? '').replace(/,/g, ' ')}`,
      ),
    ].join('\n');
    await this.putCredenciales(deviceId, csv);
    await this.config.markCredentialsPending(deviceId);
    return { ok: true, deviceId, id };
  }

  async deleteCredencial(deviceId: string, idRaw: string) {
    const id = (idRaw ?? '').trim();
    if (!/^\d{8}$/.test(id)) {
      throw new BadRequestException('id debe tener 8 digitos');
    }
    if (this.db.isEnabled()) {
      await this.db.query(
        `DELETE FROM credenciales WHERE device_id = $1 AND id = $2`,
        [deviceId, id],
      );
      await this.config.markCredentialsPending(deviceId);
      return { ok: true, deviceId, deletedId: id };
    }

    const current = await this.listCredenciales(deviceId);
    const rows = current.rows.filter((r) => r.id !== id);
    const csv = [
      CRED_HEADER,
      ...rows.map(
        (r) =>
          `${r.id},${r.tipo},${r.nivel},${(r.usuario ?? '').replace(/,/g, ' ')}`,
      ),
    ].join('\n');
    await this.putCredenciales(deviceId, csv);
    await this.config.markCredentialsPending(deviceId);
    return { ok: true, deviceId, deletedId: id };
  }

  async appendEnergia(deviceId: string, csv: string) {
    if (this.db.isEnabled()) {
      const out = await this.insertEnergiaRows(deviceId, csv, false);
      await this.refreshEnergiaLiveDb(deviceId);
      await this.touchDeviceSeen(deviceId);
      return out;
    }
    const out = await this.appendCsv(
      deviceId,
      'log_energia.csv',
      csv,
      ENERGIA_HEADER,
    );
    await this.touchDeviceSeen(deviceId);
    return out;
  }

  async appendEventos(deviceId: string, csv: string) {
    if (this.db.isEnabled()) {
      const out = await this.insertEventosRows(deviceId, csv, false);
      await this.refreshAccesosLiveDb(deviceId);
      await this.touchDeviceSeen(deviceId);
      return out;
    }
    const out = await this.appendCsv(
      deviceId,
      'log_eventos.csv',
      csv,
      EVENTOS_HEADER,
    );
    await this.touchDeviceSeen(deviceId);
    return out;
  }

  async appendHardware(deviceId: string, csv: string) {
    if (this.db.isEnabled()) {
      const out = await this.insertHwRows(deviceId, csv, false);
      await this.refreshHardwareLiveDb(deviceId);
      await this.touchDeviceSeen(deviceId);
      return out;
    }
    const out = await this.appendCsv(deviceId, 'log_hw.csv', csv, HW_HEADER);
    await this.touchDeviceSeen(deviceId);
    return out;
  }

  async putEnergia(deviceId: string, csv: string) {
    if (this.db.isEnabled()) {
      const out = await this.insertEnergiaRows(deviceId, csv, true);
      await this.refreshEnergiaLiveDb(deviceId);
      await this.touchDeviceSeen(deviceId);
      return out;
    }
    const out = await this.putWhole(
      deviceId,
      'log_energia.csv',
      csv,
      ENERGIA_HEADER,
    );
    await this.touchDeviceSeen(deviceId);
    return out;
  }

  async putEventos(deviceId: string, csv: string) {
    if (this.db.isEnabled()) {
      const out = await this.insertEventosRows(deviceId, csv, true);
      await this.refreshAccesosLiveDb(deviceId);
      await this.touchDeviceSeen(deviceId);
      return out;
    }
    const out = await this.putWhole(
      deviceId,
      'log_eventos.csv',
      csv,
      EVENTOS_HEADER,
    );
    await this.touchDeviceSeen(deviceId);
    return out;
  }

  async putHardware(deviceId: string, csv: string) {
    if (this.db.isEnabled()) {
      const out = await this.insertHwRows(deviceId, csv, true);
      await this.refreshHardwareLiveDb(deviceId);
      await this.touchDeviceSeen(deviceId);
      return out;
    }
    const out = await this.putWhole(deviceId, 'log_hw.csv', csv, HW_HEADER);
    await this.touchDeviceSeen(deviceId);
    return out;
  }

  async markCredentialsSync(deviceId: string, ok: boolean) {
    return this.config.markCredentialsSynced(deviceId, ok ? 'synced' : 'error');
  }

  private async touchDeviceSeen(deviceId: string) {
    await this.config.markDeviceOnline(deviceId);
  }

  private async ensureDir(deviceId: string) {
    const dir = this.devicePath(deviceId);
    await mkdir(dir, { recursive: true });
    return dir;
  }

  private normalizeCsv(csv: string, expectedHeader: string) {
    const trimmed = (csv ?? '').trim();
    if (!trimmed) {
      throw new BadRequestException('csv vacio');
    }
    const firstLine = trimmed.split(/\r?\n/)[0]?.trim() ?? '';
    if (firstLine.replace(/\s/g, '') !== expectedHeader.replace(/\s/g, '')) {
      throw new BadRequestException(
        `cabecera CSV debe ser exactamente: ${expectedHeader}`,
      );
    }
    return trimmed.endsWith('\n') ? trimmed : `${trimmed}\n`;
  }

  private stripHeaderIfPresent(lines: string[], header: string) {
    if (lines.length === 0) return lines;
    const first = lines[0].trim();
    if (first.replace(/\s/g, '') === header.replace(/\s/g, '')) {
      return lines.slice(1);
    }
    return lines;
  }

  /** Hash estable por fila para deduplicar POST (append) sin colisiones entre dispositivos. */
  private ingestRowHash(parts: string[]) {
    return createHash('sha256')
      .update(parts.map((p) => p.trim()).join('|'), 'utf8')
      .digest('hex');
  }

  private parseCsvByHeader(csv: string, header: string) {
    const lines = csv
      .split(/\r?\n/)
      .map((l) => l.trimEnd())
      .filter((l) => l.length > 0);
    if (lines.length === 0) return [] as Record<string, string>[];
    const fields = header.split(',').map((h) => h.trim());
    const dataLines = this.stripHeaderIfPresent(lines, header);
    return dataLines.map((line) => {
      const cells = line.split(',').map((c) => c.trim());
      const out: Record<string, string> = {};
      fields.forEach((f, i) => {
        out[f] = cells[i] ?? '';
      });
      return out;
    });
  }

  private async insertEnergiaRows(
    deviceId: string,
    csv: string,
    replace: boolean,
  ) {
    const rows = this.parseCsvByHeader(
      this.normalizeCsv(csv, ENERGIA_HEADER),
      ENERGIA_HEADER,
    );
    let inserted = 0;
    let skipped = 0;
    await this.db.withTransaction(async (client: PoolClient) => {
      if (replace)
        await client.query(`DELETE FROM log_energia WHERE device_id = $1`, [
          deviceId,
        ]);
      for (const r of rows) {
        const ts = (r.timestamp ?? '').trim();
        const vs = Number.parseFloat(r.VS ?? '0') || 0;
        const cs = Number.parseFloat(r.CS ?? '0') || 0;
        const sw = Number.parseFloat(r.SW ?? '0') || 0;
        const vb = Number.parseFloat(r.VB ?? '0') || 0;
        const cb = Number.parseFloat(r.CB ?? '0') || 0;
        const lv = Number.parseFloat(r.LV ?? '0') || 0;
        const lc = Number.parseFloat(r.LC ?? '0') || 0;
        const lp = Number.parseFloat(r.LP ?? '0') || 0;
        const h = this.ingestRowHash([
          deviceId,
          ts,
          String(vs),
          String(cs),
          String(sw),
          String(vb),
          String(cb),
          String(lv),
          String(lc),
          String(lp),
        ]);
        if (replace) {
          await client.query(
            `INSERT INTO log_energia (device_id, timestamp_text, vs, cs, sw, vb, cb, lv, lc, lp, ingest_hash)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
            [deviceId, ts, vs, cs, sw, vb, cb, lv, lc, lp, h],
          );
          inserted += 1;
        } else {
          const res = await client.query(
            `INSERT INTO log_energia (device_id, timestamp_text, vs, cs, sw, vb, cb, lv, lc, lp, ingest_hash)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
             ON CONFLICT (device_id, ingest_hash) DO NOTHING`,
            [deviceId, ts, vs, cs, sw, vb, cb, lv, lc, lp, h],
          );
          if ((res.rowCount ?? 0) > 0) inserted += 1;
          else skipped += 1;
        }
      }
    });
    return {
      ok: true,
      file: 'log_energia.csv',
      rows: rows.length,
      replace,
      inserted,
      skipped: replace ? 0 : skipped,
    };
  }

  private async insertEventosRows(
    deviceId: string,
    csv: string,
    replace: boolean,
  ) {
    const rows = this.parseCsvByHeader(
      this.normalizeCsv(csv, EVENTOS_HEADER),
      EVENTOS_HEADER,
    );
    let inserted = 0;
    let skipped = 0;
    await this.db.withTransaction(async (client: PoolClient) => {
      if (replace)
        await client.query(`DELETE FROM log_eventos WHERE device_id = $1`, [
          deviceId,
        ]);
      for (const r of rows) {
        const fecha = (r.fecha ?? '').trim();
        const idPersona = (r.id_persona ?? '').trim();
        const usuarioPersona = (r.usuario_persona ?? '').trim();
        const idVehiculo = (r.id_vehiculo ?? '').trim();
        const usuarioVehiculo = (r.usuario_vehiculo ?? '').trim();
        const resultado = (r.resultado ?? '').trim();
        const direccion = (r.direccion ?? '').trim();
        const h = this.ingestRowHash([
          deviceId,
          fecha,
          idPersona,
          usuarioPersona,
          idVehiculo,
          usuarioVehiculo,
          resultado,
          direccion,
        ]);
        if (replace) {
          await client.query(
            `INSERT INTO log_eventos (device_id, fecha, id_persona, usuario_persona, id_vehiculo, usuario_vehiculo, resultado, direccion, ingest_hash)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
            [
              deviceId,
              fecha,
              idPersona,
              usuarioPersona,
              idVehiculo,
              usuarioVehiculo,
              resultado,
              direccion,
              h,
            ],
          );
          inserted += 1;
        } else {
          const res = await client.query(
            `INSERT INTO log_eventos (device_id, fecha, id_persona, usuario_persona, id_vehiculo, usuario_vehiculo, resultado, direccion, ingest_hash)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
             ON CONFLICT (device_id, ingest_hash) DO NOTHING`,
            [
              deviceId,
              fecha,
              idPersona,
              usuarioPersona,
              idVehiculo,
              usuarioVehiculo,
              resultado,
              direccion,
              h,
            ],
          );
          if ((res.rowCount ?? 0) > 0) inserted += 1;
          else skipped += 1;
        }
      }
    });
    return {
      ok: true,
      file: 'log_eventos.csv',
      rows: rows.length,
      replace,
      inserted,
      skipped: replace ? 0 : skipped,
    };
  }

  private async insertHwRows(deviceId: string, csv: string, replace: boolean) {
    const rows = this.parseCsvByHeader(
      this.normalizeCsv(csv, HW_HEADER),
      HW_HEADER,
    );
    let inserted = 0;
    let skipped = 0;
    await this.db.withTransaction(async (client: PoolClient) => {
      if (replace)
        await client.query(`DELETE FROM log_hw WHERE device_id = $1`, [
          deviceId,
        ]);
      for (const r of rows) {
        const fecha = (r.fecha ?? '').trim();
        const lectora = (r.lectora ?? '').trim();
        const evento = (r.evento ?? '').trim();
        const h = this.ingestRowHash([deviceId, fecha, lectora, evento]);
        if (replace) {
          await client.query(
            `INSERT INTO log_hw (device_id, fecha, lectora, evento, ingest_hash)
             VALUES ($1,$2,$3,$4,$5)`,
            [deviceId, fecha, lectora, evento, h],
          );
          inserted += 1;
        } else {
          const res = await client.query(
            `INSERT INTO log_hw (device_id, fecha, lectora, evento, ingest_hash)
             VALUES ($1,$2,$3,$4,$5)
             ON CONFLICT (device_id, ingest_hash) DO NOTHING`,
            [deviceId, fecha, lectora, evento, h],
          );
          if ((res.rowCount ?? 0) > 0) inserted += 1;
          else skipped += 1;
        }
      }
    });
    return {
      ok: true,
      file: 'log_hw.csv',
      rows: rows.length,
      replace,
      inserted,
      skipped: replace ? 0 : skipped,
    };
  }

  private async appendCsv(
    deviceId: string,
    filename: string,
    csv: string,
    header: string,
  ) {
    const dir = await this.ensureDir(deviceId);
    const path = join(dir, filename);
    const raw = (csv ?? '').trim();
    if (!raw) {
      throw new BadRequestException('csv vacio');
    }
    const lines = raw
      .split(/\r?\n/)
      .map((l) => l.trimEnd())
      .filter((l) => l.length > 0);
    const dataLines = this.stripHeaderIfPresent(lines, header);
    if (dataLines.length === 0) {
      throw new BadRequestException(
        'sin filas de datos despues de la cabecera',
      );
    }
    let exists = true;
    try {
      await readFile(path, 'utf8');
    } catch {
      exists = false;
    }
    if (!exists) {
      await writeFile(path, `${header}\n`, 'utf8');
    }
    const block = dataLines
      .map((l) => (l.endsWith('\n') ? l : `${l}\n`))
      .join('');
    await appendFile(path, block, 'utf8');
    return {
      ok: true,
      file: filename,
      linesAppended: dataLines.length,
    };
  }

  private async putWhole(
    deviceId: string,
    filename: string,
    csv: string,
    header: string,
  ) {
    const dir = await this.ensureDir(deviceId);
    const text = this.normalizeCsv(csv, header);
    await writeFile(join(dir, filename), text, 'utf8');
    return { ok: true, file: filename, bytes: Buffer.byteLength(text) };
  }

  private parseCsvTail(content: string, limit: number) {
    const lines = content
      .split(/\r?\n/)
      .map((l) => l.trimEnd())
      .filter((l) => l.length > 0);
    if (lines.length === 0) {
      return {
        columns: [] as string[],
        rows: [] as Record<string, string>[],
        totalRows: 0,
      };
    }
    const headerCells = lines[0].split(',').map((c) => c.trim());
    const dataLines = lines.slice(1);
    const totalRows = dataLines.length;
    const cap = Math.min(Math.max(1, limit), 5000);
    const tail = dataLines.slice(-cap);
    const rows = tail.map((line) => {
      const cells = line.split(',').map((c) => c.trim());
      const row: Record<string, string> = {};
      headerCells.forEach((h, i) => {
        row[h] = cells[i] ?? '';
      });
      return row;
    });
    return { columns: headerCells, rows, totalRows };
  }

  async readRawFile(deviceId: string, filename: string): Promise<string> {
    if (this.db.isEnabled() && filename === 'credenciales.csv') {
      const listed = await this.listCredenciales(deviceId);
      const lines = listed.rows.map(
        (r) =>
          `${r.id},${r.tipo},${r.nivel},${(r.usuario ?? '').replace(/,/g, ' ')}`,
      );
      return [CRED_HEADER, ...lines].join('\n') + '\n';
    }
    const path = join(this.devicePath(deviceId), filename);
    try {
      return await readFile(path, 'utf8');
    } catch {
      return '';
    }
  }

  async readLog(deviceId: string, filename: string, limit: number) {
    if (this.db.isEnabled()) {
      const cap = Math.min(Math.max(1, limit), 5000);
      if (filename === 'credenciales.csv') {
        const rs = await this.db.query<{
          id: string;
          tipo: string;
          nivel: number;
          usuario: string;
        }>(
          `SELECT id, tipo, nivel, usuario
             FROM credenciales
            WHERE device_id = $1
            ORDER BY id ASC
            LIMIT $2`,
          [deviceId, cap],
        );
        return {
          deviceId,
          file: filename,
          columns: ['id', 'tipo', 'nivel', 'usuario'],
          rows: rs.rows.map((r) => ({
            id: r.id,
            tipo: r.tipo,
            nivel: String(r.nivel),
            usuario: r.usuario,
          })),
          totalRows: rs.rowCount ?? rs.rows.length,
        };
      }
      if (filename === 'log_energia.csv') {
        const rs = await this.db.query<any>(
          `SELECT timestamp_text AS "timestamp", vs AS "VS", cs AS "CS", sw AS "SW", vb AS "VB", cb AS "CB", lv AS "LV", lc AS "LC", lp AS "LP"
             FROM log_energia
            WHERE device_id = $1
            ORDER BY pk DESC
            LIMIT $2`,
          [deviceId, cap],
        );
        const total = await this.db.query<{ count: string }>(
          `SELECT count(*)::text AS count FROM log_energia WHERE device_id = $1`,
          [deviceId],
        );
        return {
          deviceId,
          file: filename,
          columns: [
            'timestamp',
            'VS',
            'CS',
            'SW',
            'VB',
            'CB',
            'LV',
            'LC',
            'LP',
          ],
          rows: [...rs.rows]
            .reverse()
            .map((r) =>
              Object.fromEntries(
                Object.entries(r).map(([k, v]) => [k, String(v ?? '')]),
              ),
            ),
          totalRows: Number.parseInt(total.rows[0]?.count ?? '0', 10),
        };
      }
      if (filename === 'log_eventos.csv') {
        const rs = await this.db.query<any>(
          `SELECT fecha, id_persona, usuario_persona, id_vehiculo, usuario_vehiculo, resultado, direccion
             FROM log_eventos
            WHERE device_id = $1
            ORDER BY pk DESC
            LIMIT $2`,
          [deviceId, cap],
        );
        const total = await this.db.query<{ count: string }>(
          `SELECT count(*)::text AS count FROM log_eventos WHERE device_id = $1`,
          [deviceId],
        );
        return {
          deviceId,
          file: filename,
          columns: [
            'fecha',
            'id_persona',
            'usuario_persona',
            'id_vehiculo',
            'usuario_vehiculo',
            'resultado',
            'direccion',
          ],
          rows: [...rs.rows]
            .reverse()
            .map((r) =>
              Object.fromEntries(
                Object.entries(r).map(([k, v]) => [k, String(v ?? '')]),
              ),
            ),
          totalRows: Number.parseInt(total.rows[0]?.count ?? '0', 10),
        };
      }
      if (filename === 'log_hw.csv') {
        const rs = await this.db.query<any>(
          `SELECT fecha, lectora, evento
             FROM log_hw
            WHERE device_id = $1
            ORDER BY pk DESC
            LIMIT $2`,
          [deviceId, cap],
        );
        const total = await this.db.query<{ count: string }>(
          `SELECT count(*)::text AS count FROM log_hw WHERE device_id = $1`,
          [deviceId],
        );
        return {
          deviceId,
          file: filename,
          columns: ['fecha', 'lectora', 'evento'],
          rows: [...rs.rows]
            .reverse()
            .map((r) =>
              Object.fromEntries(
                Object.entries(r).map(([k, v]) => [k, String(v ?? '')]),
              ),
            ),
          totalRows: Number.parseInt(total.rows[0]?.count ?? '0', 10),
        };
      }
    }

    const path = join(this.devicePath(deviceId), filename);
    let raw: string;
    try {
      raw = await readFile(path, 'utf8');
    } catch {
      return {
        deviceId,
        file: filename,
        columns: [] as string[],
        rows: [] as Record<string, string>[],
        totalRows: 0,
      };
    }
    const lim = Number.isFinite(limit) ? limit : 500;
    const { columns, rows, totalRows } = this.parseCsvTail(raw, lim);
    return { deviceId, file: filename, columns, rows, totalRows };
  }

  async getBarrierIngestSummary(
    deviceId: string,
    opts?: { eventosLimit?: number; hwLimit?: number },
  ): Promise<BarrierIngestSummary> {
    this.devicePath(deviceId);
    const eventosLimit = Math.min(Math.max(opts?.eventosLimit ?? 500, 1), 5000);
    const hwLimit = Math.min(Math.max(opts?.hwLimit ?? 100, 1), 5000);
    if (this.db.isEnabled()) {
      return this.getBarrierIngestSummaryDb(deviceId, eventosLimit, hwLimit);
    }
    return this.getBarrierIngestSummaryFiles(deviceId, eventosLimit, hwLimit);
  }

  private mapEnergiaLastFromRow(
    row: Record<string, unknown> | undefined,
  ): BarrierIngestSummary['energia']['last'] {
    if (!row) return null;
    const num = (k: string) =>
      Number.parseFloat(String((row as Record<string, unknown>)[k] ?? '0')) ||
      0;
    const ts = String(
      (row as Record<string, unknown>).timestamp ??
        (row as Record<string, unknown>).timestamp_text ??
        '',
    ).trim();
    if (!ts) return null;
    return {
      timestamp: ts,
      VS: num('VS'),
      CS: num('CS'),
      SW: num('SW'),
      VB: num('VB'),
      CB: num('CB'),
      LV: num('LV'),
      LC: num('LC'),
      LP: num('LP'),
    };
  }

  private parseLocalEventDate(raw: string): Date | null {
    const s = (raw ?? '').trim();
    if (!s) return null;
    const normalized = s.includes('T')
      ? s
      : s.replace(/^(\d{4}-\d{2}-\d{2})\s+/, '$1T');
    const d = new Date(normalized);
    return Number.isFinite(d.getTime()) ? d : null;
  }

  private countOkAccesos24h(rows: Record<string, string>[]): number {
    const cutoff = Date.now() - 24 * 3600_000;
    let n = 0;
    for (const r of rows) {
      const res = (r.resultado ?? '').toLowerCase().trim();
      if (!res.startsWith('ok')) continue;
      const d = this.parseLocalEventDate(r.fecha ?? '');
      if (d && d.getTime() >= cutoff) n += 1;
    }
    return n;
  }

  private mapAccesoLastFromRow(row: Record<string, unknown> | undefined) {
    if (!row) return null;
    const fecha = String((row as any).fecha ?? '').trim();
    if (!fecha) return null;
    return {
      fecha,
      id_persona: String((row as any).id_persona ?? ''),
      usuario_persona: String((row as any).usuario_persona ?? ''),
      id_vehiculo: String((row as any).id_vehiculo ?? ''),
      usuario_vehiculo: String((row as any).usuario_vehiculo ?? ''),
      resultado: String((row as any).resultado ?? ''),
      direccion: String((row as any).direccion ?? ''),
    };
  }

  private mapHwLastFromRow(row: Record<string, unknown> | undefined) {
    if (!row) return null;
    const fecha = String((row as any).fecha ?? '').trim();
    if (!fecha) return null;
    return {
      fecha,
      lectora: String((row as any).lectora ?? ''),
      evento: String((row as any).evento ?? ''),
    };
  }

  private async refreshEnergiaLiveDb(deviceId: string) {
    if (!this.db.isEnabled()) return;
    const [lastR, countR] = await Promise.all([
      this.db.query<{
        timestamp: string;
        VS: number;
        CS: number;
        SW: number;
        VB: number;
        CB: number;
        LV: number;
        LC: number;
        LP: number;
      }>(
        `SELECT timestamp_text AS "timestamp", vs AS "VS", cs AS "CS", sw AS "SW", vb AS "VB", cb AS "CB", lv AS "LV", lc AS "LC", lp AS "LP"
           FROM log_energia
          WHERE device_id = $1
          ORDER BY pk DESC
          LIMIT 1`,
        [deviceId],
      ),
      this.db.query<{ c: string }>(
        `SELECT count(*)::text AS c FROM log_energia WHERE device_id = $1`,
        [deviceId],
      ),
    ]);
    const last = this.mapEnergiaLastFromRow(
      lastR.rows[0] as Record<string, unknown> | undefined,
    );
    const total = Number.parseInt(countR.rows[0]?.c ?? '0', 10) || 0;
    await this.db.query(
      `INSERT INTO device_live_state (device_id, energia_total_rows, energia_last, energia_last_at, updated_at)
       VALUES ($1, $2, $3::jsonb, $4, now())
       ON CONFLICT (device_id) DO UPDATE
       SET energia_total_rows = EXCLUDED.energia_total_rows,
           energia_last = EXCLUDED.energia_last,
           energia_last_at = EXCLUDED.energia_last_at,
           updated_at = now()`,
      [deviceId, total, last ? JSON.stringify(last) : null, last?.timestamp ?? null],
    );
  }

  private async refreshAccesosLiveDb(deviceId: string) {
    if (!this.db.isEnabled()) return;
    const [lastR, countR, okR] = await Promise.all([
      this.db.query<{
        fecha: string;
        id_persona: string;
        usuario_persona: string;
        id_vehiculo: string;
        usuario_vehiculo: string;
        resultado: string;
        direccion: string;
      }>(
        `SELECT fecha, id_persona, usuario_persona, id_vehiculo, usuario_vehiculo, resultado, direccion
           FROM log_eventos
          WHERE device_id = $1
          ORDER BY pk DESC
          LIMIT 1`,
        [deviceId],
      ),
      this.db.query<{ c: string }>(
        `SELECT count(*)::text AS c FROM log_eventos WHERE device_id = $1`,
        [deviceId],
      ),
      this.db.query<{ c: string }>(
        `SELECT count(*)::text AS c FROM log_eventos
         WHERE device_id = $1
           AND lower(trim(resultado)) LIKE 'ok%'
           AND fecha ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
           AND (fecha::timestamp without time zone) >= (now() - interval '24 hours')`,
        [deviceId],
      ),
    ]);
    const last = this.mapAccesoLastFromRow(
      lastR.rows[0] as Record<string, unknown> | undefined,
    );
    const total = Number.parseInt(countR.rows[0]?.c ?? '0', 10) || 0;
    const ok24 = Number.parseInt(okR.rows[0]?.c ?? '0', 10) || 0;
    await this.db.query(
      `INSERT INTO device_live_state (device_id, accesos_total_rows, accesos_ok_ultimas_24h, accesos_last, accesos_last_at, updated_at)
       VALUES ($1, $2, $3, $4::jsonb, $5, now())
       ON CONFLICT (device_id) DO UPDATE
       SET accesos_total_rows = EXCLUDED.accesos_total_rows,
           accesos_ok_ultimas_24h = EXCLUDED.accesos_ok_ultimas_24h,
           accesos_last = EXCLUDED.accesos_last,
           accesos_last_at = EXCLUDED.accesos_last_at,
           updated_at = now()`,
      [deviceId, total, ok24, last ? JSON.stringify(last) : null, last?.fecha ?? null],
    );
  }

  private async refreshHardwareLiveDb(deviceId: string) {
    if (!this.db.isEnabled()) return;
    const [lastR, countR] = await Promise.all([
      this.db.query<{ fecha: string; lectora: string; evento: string }>(
        `SELECT fecha, lectora, evento
           FROM log_hw
          WHERE device_id = $1
          ORDER BY pk DESC
          LIMIT 1`,
        [deviceId],
      ),
      this.db.query<{ c: string }>(
        `SELECT count(*)::text AS c FROM log_hw WHERE device_id = $1`,
        [deviceId],
      ),
    ]);
    const last = this.mapHwLastFromRow(
      lastR.rows[0] as Record<string, unknown> | undefined,
    );
    const total = Number.parseInt(countR.rows[0]?.c ?? '0', 10) || 0;
    await this.db.query(
      `INSERT INTO device_live_state (device_id, hardware_total_rows, hardware_last, hardware_last_at, updated_at)
       VALUES ($1, $2, $3::jsonb, $4, now())
       ON CONFLICT (device_id) DO UPDATE
       SET hardware_total_rows = EXCLUDED.hardware_total_rows,
           hardware_last = EXCLUDED.hardware_last,
           hardware_last_at = EXCLUDED.hardware_last_at,
           updated_at = now()`,
      [deviceId, total, last ? JSON.stringify(last) : null, last?.fecha ?? null],
    );
  }

  private async readBarrierIngestLiveDb(
    deviceId: string,
  ): Promise<BarrierIngestLive | null> {
    if (!this.db.isEnabled()) return null;
    const rs = await this.db.query<{
      device_id: string;
      energia_total_rows: string | number;
      energia_last: Record<string, unknown> | null;
      accesos_total_rows: string | number;
      accesos_ok_ultimas_24h: number;
      accesos_last: Record<string, unknown> | null;
      hardware_total_rows: string | number;
      hardware_last: Record<string, unknown> | null;
      updated_at: string | null;
    }>(
      `SELECT device_id, energia_total_rows, energia_last, accesos_total_rows, accesos_ok_ultimas_24h, accesos_last, hardware_total_rows, hardware_last, updated_at
         FROM device_live_state
        WHERE device_id = $1`,
      [deviceId],
    );
    const row = rs.rows[0];
    if (!row) return null;
    return {
      deviceId,
      generatedAt: new Date().toISOString(),
      updatedAt: row.updated_at ?? null,
      source: 'db_live_state',
      energia: {
        totalRows: Number.parseInt(String(row.energia_total_rows ?? '0'), 10) || 0,
        last: this.mapEnergiaLastFromRow(row.energia_last ?? undefined),
      },
      accesos: {
        totalRows: Number.parseInt(String(row.accesos_total_rows ?? '0'), 10) || 0,
        accesosOkUltimas24h: Number(row.accesos_ok_ultimas_24h ?? 0) || 0,
        last: this.mapAccesoLastFromRow(row.accesos_last ?? undefined),
      },
      hardware: {
        totalRows: Number.parseInt(String(row.hardware_total_rows ?? '0'), 10) || 0,
        last: this.mapHwLastFromRow(row.hardware_last ?? undefined),
      },
    };
  }

  async getBarrierIngestLive(deviceId: string): Promise<BarrierIngestLive> {
    this.devicePath(deviceId);
    if (this.db.isEnabled()) {
      let live = await this.readBarrierIngestLiveDb(deviceId);
      if (!live) {
        await Promise.all([
          this.refreshEnergiaLiveDb(deviceId),
          this.refreshAccesosLiveDb(deviceId),
          this.refreshHardwareLiveDb(deviceId),
        ]);
        live = await this.readBarrierIngestLiveDb(deviceId);
      }
      if (live) return live;
    }
    const sum = await this.getBarrierIngestSummary(deviceId, {
      eventosLimit: 1,
      hwLimit: 1,
    });
    return {
      deviceId,
      generatedAt: new Date().toISOString(),
      updatedAt:
        sum.energia.last?.timestamp ??
        sum.accesos.recent[0]?.fecha ??
        sum.hardware.recent[0]?.fecha ??
        null,
      source: 'computed',
      energia: {
        totalRows: sum.energia.totalRows,
        last: sum.energia.last,
      },
      accesos: {
        totalRows: sum.accesos.totalRows,
        accesosOkUltimas24h: sum.accesos.accesosOkUltimas24h,
        last: sum.accesos.recent[0] ?? null,
      },
      hardware: {
        totalRows: sum.hardware.totalRows,
        last: sum.hardware.recent[0] ?? null,
      },
    };
  }

  private async getBarrierIngestSummaryDb(
    deviceId: string,
    eventosLimit: number,
    hwLimit: number,
  ): Promise<BarrierIngestSummary> {
    const credCountP = this.db.query<{ c: string }>(
      `SELECT count(*)::text AS c FROM credenciales WHERE device_id = $1`,
      [deviceId],
    );
    const credPreviewP = this.db.query<{
      id: string;
      tipo: string;
      nivel: number;
      usuario: string;
    }>(
      `SELECT id, tipo, nivel, usuario FROM credenciales WHERE device_id = $1 ORDER BY id ASC LIMIT 8`,
      [deviceId],
    );
    const enLastP = this.db.query(
      `SELECT timestamp_text AS "timestamp", vs AS "VS", cs AS "CS", sw AS "SW", vb AS "VB", cb AS "CB", lv AS "LV", lc AS "LC", lp AS "LP"
         FROM log_energia WHERE device_id = $1 ORDER BY pk DESC LIMIT 1`,
      [deviceId],
    );
    const enCountP = this.db.query<{ c: string }>(
      `SELECT count(*)::text AS c FROM log_energia WHERE device_id = $1`,
      [deviceId],
    );
    const evP = this.db.query<{
      fecha: string;
      id_persona: string;
      usuario_persona: string;
      id_vehiculo: string;
      usuario_vehiculo: string;
      resultado: string;
      direccion: string;
    }>(
      `SELECT fecha, id_persona, usuario_persona, id_vehiculo, usuario_vehiculo, resultado, direccion
         FROM log_eventos WHERE device_id = $1 ORDER BY pk DESC LIMIT $2`,
      [deviceId, eventosLimit],
    );
    const evCountP = this.db.query<{ c: string }>(
      `SELECT count(*)::text AS c FROM log_eventos WHERE device_id = $1`,
      [deviceId],
    );
    const hwP = this.db.query<{
      fecha: string;
      lectora: string;
      evento: string;
    }>(
      `SELECT fecha, lectora, evento FROM log_hw WHERE device_id = $1 ORDER BY pk DESC LIMIT $2`,
      [deviceId, hwLimit],
    );
    const hwCountP = this.db.query<{ c: string }>(
      `SELECT count(*)::text AS c FROM log_hw WHERE device_id = $1`,
      [deviceId],
    );

    const [
      credCount,
      credPreview,
      enLast,
      enCount,
      ev,
      evCount,
      hw,
      hwCount,
    ] = await Promise.all([
      credCountP,
      credPreviewP,
      enLastP,
      enCountP,
      evP,
      evCountP,
      hwP,
      hwCountP,
    ]);

    let accesosOkUltimas24h = 0;
    try {
      const okR = await this.db.query<{ c: string }>(
        `SELECT count(*)::text AS c FROM log_eventos
         WHERE device_id = $1
           AND lower(trim(resultado)) LIKE 'ok%'
           AND fecha ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
           AND (fecha::timestamp without time zone) >= (now() - interval '24 hours')`,
        [deviceId],
      );
      accesosOkUltimas24h =
        Number.parseInt(okR.rows[0]?.c ?? '0', 10) || 0;
    } catch {
      accesosOkUltimas24h = this.countOkAccesos24h(
        ev.rows.map((r) => ({
          fecha: r.fecha,
          id_persona: r.id_persona,
          usuario_persona: r.usuario_persona,
          id_vehiculo: r.id_vehiculo,
          usuario_vehiculo: r.usuario_vehiculo,
          resultado: r.resultado,
          direccion: r.direccion,
        })),
      );
    }

    const recentEv = ev.rows.map((r) => ({
      fecha: r.fecha,
      id_persona: r.id_persona ?? '',
      usuario_persona: r.usuario_persona ?? '',
      id_vehiculo: r.id_vehiculo ?? '',
      usuario_vehiculo: r.usuario_vehiculo ?? '',
      resultado: r.resultado ?? '',
      direccion: r.direccion ?? '',
    }));

    const recentHw = hw.rows.map((r) => ({
      fecha: r.fecha,
      lectora: r.lectora ?? '',
      evento: r.evento ?? '',
    }));

    return {
      deviceId,
      generatedAt: new Date().toISOString(),
      credenciales: {
        totalRows: Number.parseInt(credCount.rows[0]?.c ?? '0', 10) || 0,
        preview: credPreview.rows.map((r) => ({
          id: r.id,
          tipo: r.tipo,
          nivel: Number(r.nivel) || 0,
          usuario: r.usuario ?? '',
        })),
      },
      energia: {
        totalRows: Number.parseInt(enCount.rows[0]?.c ?? '0', 10) || 0,
        last: this.mapEnergiaLastFromRow(enLast.rows[0] as Record<string, unknown>),
      },
      accesos: {
        totalRows: Number.parseInt(evCount.rows[0]?.c ?? '0', 10) || 0,
        accesosOkUltimas24h,
        recent: recentEv,
      },
      hardware: {
        totalRows: Number.parseInt(hwCount.rows[0]?.c ?? '0', 10) || 0,
        recent: recentHw,
      },
    };
  }

  private async getBarrierIngestSummaryFiles(
    deviceId: string,
    eventosLimit: number,
    hwLimit: number,
  ): Promise<BarrierIngestSummary> {
    const credCap = 5000;
    const enCap = 5000;
    const evCap = 5000;
    const [cred, en, ev, hw] = await Promise.all([
      this.readLog(deviceId, 'credenciales.csv', credCap),
      this.readLog(deviceId, 'log_energia.csv', enCap),
      this.readLog(deviceId, 'log_eventos.csv', evCap),
      this.readLog(deviceId, 'log_hw.csv', hwLimit),
    ]);

    const preview = cred.rows.slice(0, 8).map((r) => ({
      id: r.id ?? '',
      tipo: r.tipo ?? '',
      nivel: Number.parseInt(r.nivel ?? '0', 10) || 0,
      usuario: r.usuario ?? '',
    }));

    const enRows = en.rows as Record<string, string>[];
    const evRows = ev.rows as Record<string, string>[];
    const hwRows = hw.rows as Record<string, string>[];

    const lastEnRow =
      enRows.length > 0 ? enRows[enRows.length - 1] : undefined;
    const last = lastEnRow
      ? this.mapEnergiaLastFromRow({
          timestamp: lastEnRow.timestamp,
          VS: lastEnRow.VS,
          CS: lastEnRow.CS,
          SW: lastEnRow.SW,
          VB: lastEnRow.VB,
          CB: lastEnRow.CB,
          LV: lastEnRow.LV,
          LC: lastEnRow.LC,
          LP: lastEnRow.LP,
        } as Record<string, unknown>)
      : null;

    const recentEvDesc = [...evRows]
      .reverse()
      .slice(0, eventosLimit)
      .map((r) => ({
        fecha: r.fecha ?? '',
        id_persona: r.id_persona ?? '',
        usuario_persona: r.usuario_persona ?? '',
        id_vehiculo: r.id_vehiculo ?? '',
        usuario_vehiculo: r.usuario_vehiculo ?? '',
        resultado: r.resultado ?? '',
        direccion: r.direccion ?? '',
      }));

    const accesosOkUltimas24h = this.countOkAccesos24h(evRows);

    const recentHwDesc = [...hwRows].reverse().map((r) => ({
      fecha: r.fecha ?? '',
      lectora: r.lectora ?? '',
      evento: r.evento ?? '',
    }));

    return {
      deviceId,
      generatedAt: new Date().toISOString(),
      credenciales: {
        totalRows: cred.totalRows,
        preview,
      },
      energia: {
        totalRows: en.totalRows,
        last,
      },
      accesos: {
        totalRows: ev.totalRows,
        accesosOkUltimas24h,
        recent: recentEvDesc,
      },
      hardware: {
        totalRows: hw.totalRows,
        recent: recentHwDesc,
      },
    };
  }
}
