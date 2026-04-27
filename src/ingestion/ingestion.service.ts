import { BadRequestException, Injectable } from '@nestjs/common';
import { appendFile, mkdir, readFile, writeFile } from 'fs/promises';
import type { PoolClient } from 'pg';
import { join } from 'path';
import { DatabaseService } from '../database/database.service';

const ENERGIA_HEADER =
  'timestamp,VS,CS,SW,VB,CB,LV,LC,LP';
const EVENTOS_HEADER =
  'fecha,id_persona,usuario_persona,id_vehiculo,usuario_vehiculo,resultado,direccion';
const HW_HEADER = 'fecha,lectora,evento';
const CRED_HEADER = 'id,tipo,nivel,usuario';

@Injectable()
export class IngestionService {
  constructor(private readonly db: DatabaseService) {}

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
      const rows = this.parseCsvByHeader(this.normalizeCsv(csv, CRED_HEADER), CRED_HEADER);
      await this.db.withTransaction(async (client: PoolClient) => {
        await client.query(`DELETE FROM credenciales WHERE device_id = $1`, [deviceId]);
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
      return { ok: true, file: 'credenciales.csv', rows: rows.length };
    }
    const dir = await this.ensureDir(deviceId);
    const text = this.normalizeCsv(csv, CRED_HEADER);
    await writeFile(join(dir, 'credenciales.csv'), text, 'utf8');
    return { ok: true, file: 'credenciales.csv', bytes: Buffer.byteLength(text) };
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
      return { ok: true, deviceId, id };
    }

    const current = await this.listCredenciales(deviceId);
    const rows = current.rows.filter((r) => r.id !== id);
    rows.push({ id, tipo, nivel, usuario });
    rows.sort((a, b) => (a.id > b.id ? 1 : -1));
    const csv = [CRED_HEADER, ...rows.map((r) => `${r.id},${r.tipo},${r.nivel},${(r.usuario ?? '').replace(/,/g, ' ')}`)].join('\n');
    await this.putCredenciales(deviceId, csv);
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
      return { ok: true, deviceId, deletedId: id };
    }

    const current = await this.listCredenciales(deviceId);
    const rows = current.rows.filter((r) => r.id !== id);
    const csv = [
      CRED_HEADER,
      ...rows.map((r) => `${r.id},${r.tipo},${r.nivel},${(r.usuario ?? '').replace(/,/g, ' ')}`),
    ].join('\n');
    await this.putCredenciales(deviceId, csv);
    return { ok: true, deviceId, deletedId: id };
  }

  async appendEnergia(deviceId: string, csv: string) {
    if (this.db.isEnabled()) {
      return this.insertEnergiaRows(deviceId, csv, false);
    }
    return this.appendCsv(deviceId, 'log_energia.csv', csv, ENERGIA_HEADER);
  }

  async appendEventos(deviceId: string, csv: string) {
    if (this.db.isEnabled()) {
      return this.insertEventosRows(deviceId, csv, false);
    }
    return this.appendCsv(deviceId, 'log_eventos.csv', csv, EVENTOS_HEADER);
  }

  async appendHardware(deviceId: string, csv: string) {
    if (this.db.isEnabled()) {
      return this.insertHwRows(deviceId, csv, false);
    }
    return this.appendCsv(deviceId, 'log_hw.csv', csv, HW_HEADER);
  }

  async putEnergia(deviceId: string, csv: string) {
    if (this.db.isEnabled()) {
      return this.insertEnergiaRows(deviceId, csv, true);
    }
    return this.putWhole(deviceId, 'log_energia.csv', csv, ENERGIA_HEADER);
  }

  async putEventos(deviceId: string, csv: string) {
    if (this.db.isEnabled()) {
      return this.insertEventosRows(deviceId, csv, true);
    }
    return this.putWhole(deviceId, 'log_eventos.csv', csv, EVENTOS_HEADER);
  }

  async putHardware(deviceId: string, csv: string) {
    if (this.db.isEnabled()) {
      return this.insertHwRows(deviceId, csv, true);
    }
    return this.putWhole(deviceId, 'log_hw.csv', csv, HW_HEADER);
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

  private async insertEnergiaRows(deviceId: string, csv: string, replace: boolean) {
    const rows = this.parseCsvByHeader(this.normalizeCsv(csv, ENERGIA_HEADER), ENERGIA_HEADER);
    await this.db.withTransaction(async (client: PoolClient) => {
      if (replace) await client.query(`DELETE FROM log_energia WHERE device_id = $1`, [deviceId]);
      for (const r of rows) {
        await client.query(
          `INSERT INTO log_energia (device_id, timestamp_text, vs, cs, sw, vb, cb, lv, lc, lp)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
          [
            deviceId,
            (r.timestamp ?? '').trim(),
            Number.parseFloat(r.VS ?? '0') || 0,
            Number.parseFloat(r.CS ?? '0') || 0,
            Number.parseFloat(r.SW ?? '0') || 0,
            Number.parseFloat(r.VB ?? '0') || 0,
            Number.parseFloat(r.CB ?? '0') || 0,
            Number.parseFloat(r.LV ?? '0') || 0,
            Number.parseFloat(r.LC ?? '0') || 0,
            Number.parseFloat(r.LP ?? '0') || 0,
          ],
        );
      }
    });
    return { ok: true, file: 'log_energia.csv', rows: rows.length, replace };
  }

  private async insertEventosRows(deviceId: string, csv: string, replace: boolean) {
    const rows = this.parseCsvByHeader(this.normalizeCsv(csv, EVENTOS_HEADER), EVENTOS_HEADER);
    await this.db.withTransaction(async (client: PoolClient) => {
      if (replace) await client.query(`DELETE FROM log_eventos WHERE device_id = $1`, [deviceId]);
      for (const r of rows) {
        await client.query(
          `INSERT INTO log_eventos (device_id, fecha, id_persona, usuario_persona, id_vehiculo, usuario_vehiculo, resultado, direccion)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
          [
            deviceId,
            (r.fecha ?? '').trim(),
            (r.id_persona ?? '').trim(),
            (r.usuario_persona ?? '').trim(),
            (r.id_vehiculo ?? '').trim(),
            (r.usuario_vehiculo ?? '').trim(),
            (r.resultado ?? '').trim(),
            (r.direccion ?? '').trim(),
          ],
        );
      }
    });
    return { ok: true, file: 'log_eventos.csv', rows: rows.length, replace };
  }

  private async insertHwRows(deviceId: string, csv: string, replace: boolean) {
    const rows = this.parseCsvByHeader(this.normalizeCsv(csv, HW_HEADER), HW_HEADER);
    await this.db.withTransaction(async (client: PoolClient) => {
      if (replace) await client.query(`DELETE FROM log_hw WHERE device_id = $1`, [deviceId]);
      for (const r of rows) {
        await client.query(
          `INSERT INTO log_hw (device_id, fecha, lectora, evento)
           VALUES ($1,$2,$3,$4)`,
          [deviceId, (r.fecha ?? '').trim(), (r.lectora ?? '').trim(), (r.evento ?? '').trim()],
        );
      }
    });
    return { ok: true, file: 'log_hw.csv', rows: rows.length, replace };
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
    const lines = raw.split(/\r?\n/).map((l) => l.trimEnd()).filter((l) => l.length > 0);
    const dataLines = this.stripHeaderIfPresent(lines, header);
    if (dataLines.length === 0) {
      throw new BadRequestException('sin filas de datos despues de la cabecera');
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
    const block = dataLines.map((l) => (l.endsWith('\n') ? l : `${l}\n`)).join('');
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
      return { columns: [] as string[], rows: [] as Record<string, string>[], totalRows: 0 };
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
        (r) => `${r.id},${r.tipo},${r.nivel},${(r.usuario ?? '').replace(/,/g, ' ')}`,
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
        const rs = await this.db.query<{ id: string; tipo: string; nivel: number; usuario: string }>(
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
          columns: ['timestamp', 'VS', 'CS', 'SW', 'VB', 'CB', 'LV', 'LC', 'LP'],
          rows: [...rs.rows].reverse().map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, String(v ?? '')]))),
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
          columns: ['fecha', 'id_persona', 'usuario_persona', 'id_vehiculo', 'usuario_vehiculo', 'resultado', 'direccion'],
          rows: [...rs.rows].reverse().map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, String(v ?? '')]))),
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
          rows: [...rs.rows].reverse().map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, String(v ?? '')]))),
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
}
