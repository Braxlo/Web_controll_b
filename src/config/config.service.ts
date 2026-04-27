import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { mkdir, readFile, writeFile } from 'fs/promises';
import { join } from 'path';
import type { PoolClient } from 'pg';
import { DatabaseService } from '../database/database.service';
import type { DeviceRegistryFile, RaspberryDevice } from './device-registry.types';

const DEFAULT_REGISTRY: DeviceRegistryFile = {
  devices: [
    {
      deviceId: 'barrera_01',
      name: 'Barrera principal',
      host: '192.168.2.205',
      panelPort: 8000,
    },
  ],
};

@Injectable()
export class ConfigService {
  constructor(private readonly db: DatabaseService) {}

  private readonly registryPath = join(
    process.cwd(),
    process.env.INGEST_DATA_DIR ?? 'data',
    'device-registry.json',
  );

  private validateDevice(d: RaspberryDevice) {
    const id = d.deviceId?.trim() ?? '';
    if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
      throw new BadRequestException(
        'deviceId invalido: solo letras, numeros, guion y underscore',
      );
    }
    if (!d.name?.trim()) {
      throw new BadRequestException('nombre requerido');
    }
    const host = d.host?.trim() ?? '';
    if (!host) {
      throw new BadRequestException('host / IP requerido');
    }
    const port = Number(d.panelPort);
    if (!Number.isFinite(port) || port < 1 || port > 65535) {
      throw new BadRequestException('panelPort invalido');
    }
    return {
      deviceId: id,
      name: d.name.trim(),
      host,
      panelPort: Math.floor(port),
    };
  }

  async load(): Promise<DeviceRegistryFile> {
    if (this.db.isEnabled()) {
      const rs = await this.db.query<{
        device_id: string;
        name: string;
        host: string;
        panel_port: number;
      }>(
        `SELECT device_id, name, host, panel_port
           FROM devices_registry
          ORDER BY device_id ASC`,
      );
      const devices = rs.rows.map((r) =>
        this.validateDevice({
          deviceId: r.device_id,
          name: r.name,
          host: r.host,
          panelPort: r.panel_port,
        }),
      );
      if (devices.length === 0) {
        return structuredClone(DEFAULT_REGISTRY);
      }
      return { devices };
    }

    await mkdir(join(this.registryPath, '..'), { recursive: true });
    try {
      const raw = await readFile(this.registryPath, 'utf8');
      const parsed = JSON.parse(raw) as DeviceRegistryFile;
      if (!parsed?.devices || !Array.isArray(parsed.devices)) {
        return { devices: [] };
      }
      const devices: RaspberryDevice[] = [];
      for (const x of parsed.devices) {
        try {
          devices.push(this.validateDevice(x));
        } catch {
          /* omitir entradas invalidas */
        }
      }
      return { devices };
    } catch {
      return structuredClone(DEFAULT_REGISTRY);
    }
  }

  async save(body: DeviceRegistryFile): Promise<DeviceRegistryFile> {
    if (!body?.devices || !Array.isArray(body.devices)) {
      throw new BadRequestException('body debe incluir array devices');
    }
    const devices = body.devices.map((x) => this.validateDevice(x));
    const seen = new Set<string>();
    for (const d of devices) {
      if (seen.has(d.deviceId)) {
        throw new BadRequestException(`deviceId duplicado: ${d.deviceId}`);
      }
      seen.add(d.deviceId);
    }
    const next: DeviceRegistryFile = { devices };

    if (this.db.isEnabled()) {
      await this.db.withTransaction(async (client: PoolClient) => {
        await client.query('DELETE FROM devices_registry');
        for (const d of devices) {
          await client.query(
            `INSERT INTO devices_registry (device_id, name, host, panel_port, updated_at)
             VALUES ($1, $2, $3, $4, now())`,
            [d.deviceId, d.name, d.host, d.panelPort],
          );
        }
      });
      return next;
    }

    await mkdir(join(this.registryPath, '..'), { recursive: true });
    await writeFile(
      this.registryPath,
      JSON.stringify(next, null, 2),
      'utf8',
    );
    return next;
  }

  async ping(deviceId: string) {
    const { devices } = await this.load();
    const d = devices.find((x) => x.deviceId === deviceId);
    if (!d) {
      throw new NotFoundException(`dispositivo no encontrado: ${deviceId}`);
    }
    const url = `http://${d.host}:${d.panelPort}/login`;
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), 4000);
    try {
      const res = await fetch(url, {
        method: 'GET',
        signal: ac.signal,
        redirect: 'manual',
      });
      clearTimeout(t);
      return {
        deviceId,
        url,
        ok: res.ok || res.status === 302 || res.status === 301,
        status: res.status,
      };
    } catch (e) {
      clearTimeout(t);
      const msg = e instanceof Error ? e.message : 'error';
      return {
        deviceId,
        url,
        ok: false,
        status: 0,
        error: msg,
      };
    }
  }
}
