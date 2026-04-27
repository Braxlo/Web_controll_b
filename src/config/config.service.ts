import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { mkdir, readFile, writeFile } from 'fs/promises';
import { join } from 'path';
import type { PoolClient } from 'pg';
import { DatabaseService } from '../database/database.service';
import type { DeviceRegistryFile, RaspberryDevice } from './device-registry.types';
import type {
  BarrierControlConfig,
  ModulesConfigFile,
  SignboardConfig,
} from './modules-config.types';

const DEFAULT_REGISTRY: DeviceRegistryFile = {
  devices: [],
};

const DEFAULT_MODULES_CONFIG: ModulesConfigFile = {
  mqtt: {
    brokerUrl: 'mqtt://127.0.0.1:1883',
    connected: false,
    topics: [],
  },
  barriers: {
    activeDeviceId: '',
    controlsByDeviceId: {},
  },
  signboards: {
    items: [],
  },
};

@Injectable()
export class ConfigService {
  constructor(private readonly db: DatabaseService) {}
  private readonly logger = new Logger(ConfigService.name);

  private readonly registryPath = join(
    process.cwd(),
    process.env.INGEST_DATA_DIR ?? 'data',
    'device-registry.json',
  );
  private readonly modulesConfigPath = join(
    process.cwd(),
    process.env.INGEST_DATA_DIR ?? 'data',
    'modules-config.json',
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

  private validateBarrierControl(
    deviceId: string,
    raw: Partial<BarrierControlConfig> | undefined,
  ): BarrierControlConfig {
    const id = deviceId.trim();
    if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
      throw new BadRequestException(`deviceId invalido en configuracion: ${deviceId}`);
    }
    const lastState = raw?.lastState ?? 'desconocido';
    if (!['arriba', 'abajo', 'desconocido'].includes(lastState)) {
      throw new BadRequestException(`lastState invalido para ${deviceId}`);
    }
    return {
      area: (raw?.area ?? '').trim(),
      topic: (raw?.topic ?? `barreras/control/${id}`).trim(),
      cmdOpen: (raw?.cmdOpen ?? 'barreras/cmd/abrir').trim(),
      cmdClose: (raw?.cmdClose ?? 'barreras/cmd/cerrar').trim(),
      cmdState: (raw?.cmdState ?? 'barreras/cmd/estado').trim(),
      cameraName: (raw?.cameraName ?? '').trim(),
      cameraStreamUrl: (raw?.cameraStreamUrl ?? '').trim(),
      lastState: lastState as BarrierControlConfig['lastState'],
    };
  }

  private validateSignboard(raw: SignboardConfig): SignboardConfig {
    const id = raw?.id?.trim() ?? '';
    if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
      throw new BadRequestException(`id de letrero invalido: ${raw?.id ?? ''}`);
    }
    const name = raw?.name?.trim() ?? '';
    const topic = raw?.topic?.trim() ?? '';
    if (!name || !topic) {
      throw new BadRequestException(`letrero ${id}: nombre y topic son requeridos`);
    }
    return {
      id,
      name,
      topic,
      batteryType: raw?.batteryType?.trim() ?? '',
    };
  }

  private validateModulesConfig(raw: ModulesConfigFile | undefined): ModulesConfigFile {
    const mqtt = {
      brokerUrl: raw?.mqtt?.brokerUrl?.trim() ?? DEFAULT_MODULES_CONFIG.mqtt.brokerUrl,
      connected: Boolean(raw?.mqtt?.connected),
      topics: Array.isArray(raw?.mqtt?.topics)
        ? raw!.mqtt.topics
            .map((t, i) => ({
              id: (t?.id?.trim() || `topic-${i}`).trim(),
              topic: (t?.topic ?? '').trim(),
              category: (t?.category ?? 'general').trim() || 'general',
            }))
            .filter((t) => t.topic.length > 0)
        : [],
    };
    const seenTopicIds = new Set<string>();
    for (const t of mqtt.topics) {
      if (seenTopicIds.has(t.id)) {
        throw new BadRequestException(`topic id duplicado: ${t.id}`);
      }
      seenTopicIds.add(t.id);
    }

    const controlsByDeviceId: Record<string, BarrierControlConfig> = {};
    const rawControls = raw?.barriers?.controlsByDeviceId ?? {};
    for (const [deviceId, cfg] of Object.entries(rawControls)) {
      controlsByDeviceId[deviceId] = this.validateBarrierControl(deviceId, cfg);
    }

    const rawItems = Array.isArray(raw?.signboards?.items) ? raw!.signboards.items : [];
    const items = rawItems.map((x) => this.validateSignboard(x));
    const seenSignboards = new Set<string>();
    for (const x of items) {
      if (seenSignboards.has(x.id)) {
        throw new BadRequestException(`id de letrero duplicado: ${x.id}`);
      }
      seenSignboards.add(x.id);
    }

    return {
      mqtt,
      barriers: {
        activeDeviceId: raw?.barriers?.activeDeviceId?.trim() ?? '',
        controlsByDeviceId,
      },
      signboards: {
        items,
      },
    };
  }

  async load(): Promise<DeviceRegistryFile> {
    if (this.db.isEnabled()) {
      try {
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
      } catch (error) {
        const msg = error instanceof Error ? error.message : 'error desconocido';
        this.logger.warn(`DB no disponible para devices_registry, usando archivo local: ${msg}`);
      }
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

  async loadModulesConfig(): Promise<ModulesConfigFile> {
    if (this.db.isEnabled()) {
      try {
        const rs = await this.db.query<{ config_value: ModulesConfigFile }>(
          `SELECT config_value
             FROM modules_config
            WHERE config_key = $1`,
          ['main'],
        );
        const row = rs.rows[0];
        if (!row) return structuredClone(DEFAULT_MODULES_CONFIG);
        return this.validateModulesConfig(row.config_value);
      } catch (error) {
        const msg = error instanceof Error ? error.message : 'error desconocido';
        this.logger.warn(`DB no disponible para modules_config, usando archivo local: ${msg}`);
      }
    }

    await mkdir(join(this.modulesConfigPath, '..'), { recursive: true });
    try {
      const raw = await readFile(this.modulesConfigPath, 'utf8');
      const parsed = JSON.parse(raw) as ModulesConfigFile;
      return this.validateModulesConfig(parsed);
    } catch {
      return structuredClone(DEFAULT_MODULES_CONFIG);
    }
  }

  async saveModulesConfig(body: ModulesConfigFile): Promise<ModulesConfigFile> {
    const next = this.validateModulesConfig(body);

    if (this.db.isEnabled()) {
      await this.db.query(
        `INSERT INTO modules_config (config_key, config_value, updated_at)
         VALUES ($1, $2::jsonb, now())
         ON CONFLICT (config_key)
         DO UPDATE SET config_value = EXCLUDED.config_value, updated_at = now()`,
        ['main', JSON.stringify(next)],
      );
      return next;
    }

    await mkdir(join(this.modulesConfigPath, '..'), { recursive: true });
    await writeFile(this.modulesConfigPath, JSON.stringify(next, null, 2), 'utf8');
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
