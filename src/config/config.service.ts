import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { randomBytes } from 'crypto';
import { mkdir, readFile, writeFile } from 'fs/promises';
import { join } from 'path';
import type { PoolClient } from 'pg';
import { DatabaseService } from '../database/database.service';
import type {
  DeviceRegistryFile,
  RaspberryDevice,
} from './device-registry.types';
import type {
  BarrierControlConfig,
  BarrierLocation,
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
    locations: [],
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
    const status = this.normalizeConnectionStatus(d.connectionStatus);
    const syncStatus = this.normalizeCredentialsSyncStatus(
      d.credentialsSyncStatus,
    );
    const credentialsVersion = this.normalizeCredentialsVersion(
      d.credentialsVersion,
    );
    return {
      deviceId: id,
      name: d.name.trim(),
      host,
      panelPort: Math.floor(port),
      apiKey: this.normalizeApiKey(d.apiKey),
      connectionStatus: status,
      lastSeenAt: this.normalizeLastSeenAt(d.lastSeenAt),
      credentialsVersion,
      credentialsSyncStatus: syncStatus,
    };
  }

  private normalizeApiKey(value: string | undefined) {
    const key = (value ?? '').trim();
    if (!key) return '';
    if (!/^[a-fA-F0-9]{32,128}$/.test(key)) {
      throw new BadRequestException('apiKey invalida');
    }
    return key.toLowerCase();
  }

  private generateApiKey() {
    return randomBytes(24).toString('hex');
  }

  private normalizeConnectionStatus(
    value: RaspberryDevice['connectionStatus'],
  ): NonNullable<RaspberryDevice['connectionStatus']> {
    if (value === 'online' || value === 'offline' || value === 'pending')
      return value;
    return 'pending';
  }

  private normalizeCredentialsSyncStatus(
    value: RaspberryDevice['credentialsSyncStatus'],
  ): NonNullable<RaspberryDevice['credentialsSyncStatus']> {
    if (value === 'synced' || value === 'error' || value === 'pending')
      return value;
    return 'pending';
  }

  private normalizeCredentialsVersion(
    value: RaspberryDevice['credentialsVersion'],
  ) {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 1) return 1;
    return Math.floor(n);
  }

  /**
   * PostgreSQL (TIMESTAMPTZ) suele devolver `Date` en node-pg; antes se llamaba `.trim()` y lanzaba TypeError → HTTP 500 en GET /config/devices.
   */
  private normalizeLastSeenAt(
    value: RaspberryDevice['lastSeenAt'] | Date | null | undefined,
  ): string | undefined {
    if (value === null || value === undefined) return undefined;
    if (value instanceof Date) {
      const t = value.getTime();
      return Number.isFinite(t) ? value.toISOString() : undefined;
    }
    const raw = String(value).trim();
    if (!raw) return undefined;
    const d = new Date(raw);
    return Number.isFinite(d.getTime()) ? d.toISOString() : undefined;
  }

  private validateBarrierLocation(raw: unknown): BarrierLocation {
    const o = raw as Partial<BarrierLocation>;
    const id = (o?.id ?? '').trim();
    if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
      throw new BadRequestException(
        'id de área inválido: solo letras, numeros, guion y underscore',
      );
    }
    const name = (o?.name ?? '').trim();
    if (!name) {
      throw new BadRequestException('nombre de área requerido');
    }
    return { id, name };
  }

  private validateBarrierControl(
    deviceId: string,
    raw: Partial<BarrierControlConfig> | undefined,
    locationsById: Map<string, BarrierLocation>,
  ): BarrierControlConfig {
    const id = deviceId.trim();
    if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
      throw new BadRequestException(
        `deviceId invalido en configuracion: ${deviceId}`,
      );
    }
    const lastState = raw?.lastState ?? 'desconocido';
    if (!['arriba', 'abajo', 'desconocido'].includes(lastState)) {
      throw new BadRequestException(`lastState invalido para ${deviceId}`);
    }
    let locationId = (raw?.locationId ?? '').trim();
    let area = (raw?.area ?? '').trim();
    if (locationId) {
      const loc = locationsById.get(locationId);
      if (!loc) {
        throw new BadRequestException(
          `área '${locationId}' no existe (barrera ${id})`,
        );
      }
      area = loc.name;
    } else {
      locationId = '';
    }
    const out: BarrierControlConfig = {
      area,
      topic: (raw?.topic ?? `barreras/control/${id}`).trim(),
      cmdOpen: (raw?.cmdOpen ?? 'barreras/cmd/abrir').trim(),
      cmdClose: (raw?.cmdClose ?? 'barreras/cmd/cerrar').trim(),
      cmdState: (raw?.cmdState ?? 'barreras/cmd/estado').trim(),
      cameraName: (raw?.cameraName ?? '').trim(),
      cameraStreamUrl: (raw?.cameraStreamUrl ?? '').trim(),
      lastState: lastState,
    };
    if (locationId) {
      out.locationId = locationId;
    }
    return out;
  }

  private validateSignboard(raw: SignboardConfig): SignboardConfig {
    const id = raw?.id?.trim() ?? '';
    if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
      throw new BadRequestException(`id de letrero invalido: ${raw?.id ?? ''}`);
    }
    const name = raw?.name?.trim() ?? '';
    const topic = raw?.topic?.trim() ?? '';
    if (!name || !topic) {
      throw new BadRequestException(
        `letrero ${id}: nombre y topic son requeridos`,
      );
    }
    return {
      id,
      name,
      topic,
      batteryType: raw?.batteryType?.trim() ?? '',
    };
  }

  private validateModulesConfig(
    raw: ModulesConfigFile | undefined,
  ): ModulesConfigFile {
    const mqtt = {
      brokerUrl:
        raw?.mqtt?.brokerUrl?.trim() ?? DEFAULT_MODULES_CONFIG.mqtt.brokerUrl,
      connected: Boolean(raw?.mqtt?.connected),
      topics: Array.isArray(raw?.mqtt?.topics)
        ? raw.mqtt.topics
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

    const rawLocs = Array.isArray(raw?.barriers?.locations)
      ? raw.barriers.locations
      : [];
    const locations = rawLocs.map((x) => this.validateBarrierLocation(x));
    const seenLocIds = new Set<string>();
    for (const loc of locations) {
      if (seenLocIds.has(loc.id)) {
        throw new BadRequestException(`id de área duplicado: ${loc.id}`);
      }
      seenLocIds.add(loc.id);
    }
    const locationsById = new Map(locations.map((l) => [l.id, l] as const));

    const controlsByDeviceId: Record<string, BarrierControlConfig> = {};
    const rawControls = raw?.barriers?.controlsByDeviceId ?? {};
    for (const [deviceId, cfg] of Object.entries(rawControls)) {
      controlsByDeviceId[deviceId] = this.validateBarrierControl(
        deviceId,
        cfg,
        locationsById,
      );
    }

    const rawItems = Array.isArray(raw?.signboards?.items)
      ? raw.signboards.items
      : [];
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
        locations,
        controlsByDeviceId,
      },
      signboards: {
        items,
      },
    };
  }

  async load(): Promise<DeviceRegistryFile> {
    if (this.db.isEnabled()) {
      /** Sin fallback silencioso al JSON: si la guardada es en PostgreSQL, leer archivo vacío haría “desaparecer” barreras en el panel. */
      const rs = await this.db.query<{
        device_id: string;
        name: string;
        host: string;
        panel_port: number;
        api_key: string;
        connection_status: 'pending' | 'online' | 'offline';
        last_seen_at: string | null;
        credentials_version: number;
        credentials_sync_status: 'pending' | 'synced' | 'error';
      }>(
        `SELECT device_id, name, host, panel_port, api_key, connection_status, last_seen_at, credentials_version, credentials_sync_status
           FROM devices_registry
          ORDER BY device_id ASC`,
      );
      const devices: RaspberryDevice[] = [];
      for (const r of rs.rows) {
        try {
          devices.push(
            this.validateDevice({
              deviceId: r.device_id,
              name: r.name,
              host: r.host,
              panelPort: Number(r.panel_port),
              apiKey: r.api_key,
              connectionStatus: r.connection_status,
              lastSeenAt: r.last_seen_at ?? undefined,
              credentialsVersion: r.credentials_version,
              credentialsSyncStatus: r.credentials_sync_status,
            }),
          );
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          this.logger.warn(
            `devices_registry: omitiendo fila ${String(r.device_id)} (${msg})`,
          );
        }
      }
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
    const devices = body.devices.map((x) => {
      const parsed = this.validateDevice(x);
      if (!parsed.apiKey) {
        parsed.apiKey = this.generateApiKey();
      }
      return parsed;
    });
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
            `INSERT INTO devices_registry (
              device_id,
              name,
              host,
              panel_port,
              api_key,
              connection_status,
              last_seen_at,
              credentials_version,
              credentials_sync_status,
              updated_at
            )
             VALUES ($1, $2, $3, $4, $5, $6, $7::timestamptz, $8, $9, now())`,
            [
              d.deviceId,
              d.name,
              d.host,
              d.panelPort,
              d.apiKey,
              d.connectionStatus,
              d.lastSeenAt ?? null,
              d.credentialsVersion,
              d.credentialsSyncStatus,
            ],
          );
        }
      });
      return next;
    }

    await mkdir(join(this.registryPath, '..'), { recursive: true });
    await writeFile(this.registryPath, JSON.stringify(next, null, 2), 'utf8');
    return next;
  }

  async loadModulesConfig(): Promise<ModulesConfigFile> {
    if (this.db.isEnabled()) {
      const rs = await this.db.query<{ config_value: ModulesConfigFile }>(
        `SELECT config_value
           FROM modules_config
          WHERE config_key = $1`,
        ['main'],
      );
      const row = rs.rows[0];
      if (!row) return structuredClone(DEFAULT_MODULES_CONFIG);
      return this.validateModulesConfig(row.config_value);
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
    await writeFile(
      this.modulesConfigPath,
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

  async heartbeat(deviceId: string, apiKey: string) {
    const now = new Date().toISOString();
    const { devices } = await this.load();
    const idx = devices.findIndex((x) => x.deviceId === deviceId);
    if (idx < 0) {
      throw new NotFoundException(`dispositivo no encontrado: ${deviceId}`);
    }
    const device = devices[idx];
    const givenKey = (apiKey ?? '').trim().toLowerCase();
    if (!givenKey || givenKey !== (device.apiKey ?? '').trim().toLowerCase()) {
      throw new BadRequestException('apiKey invalida para heartbeat');
    }
    await this.markDeviceOnline(deviceId, now);
    return { ok: true, deviceId, connectionStatus: 'online', lastSeenAt: now };
  }

  async markDeviceOnline(deviceId: string, at?: string) {
    const now = at ?? new Date().toISOString();
    const { devices } = await this.load();
    const d = devices.find((x) => x.deviceId === deviceId);
    if (!d) {
      this.logger.warn(
        `markDeviceOnline: dispositivo "${deviceId}" no está en el registro; omitiendo lastSeen en panel.`,
      );
      return {
        ok: true,
        deviceId,
        connectionStatus: 'online' as const,
        lastSeenAt: now,
        unregistered: true as const,
      };
    }
    await this.updateDeviceRuntime(deviceId, {
      connectionStatus: 'online',
      lastSeenAt: now,
    });
    return { ok: true, deviceId, connectionStatus: 'online', lastSeenAt: now };
  }

  async markCredentialsPending(deviceId: string) {
    const { devices } = await this.load();
    const d = devices.find((x) => x.deviceId === deviceId);
    if (!d) {
      this.logger.warn(
        `markCredentialsPending: dispositivo "${deviceId}" no está en el registro; se acepta la ingesta pero no se actualiza versión en panel. Registra el dispositivo en Configuración con el mismo deviceId.`,
      );
      return {
        ok: true,
        deviceId,
        credentialsVersion: 1,
        credentialsSyncStatus: 'pending' as const,
        unregistered: true as const,
      };
    }
    const nextVersion = Math.max(1, Number(d.credentialsVersion ?? 1)) + 1;
    await this.updateDeviceRuntime(deviceId, {
      credentialsVersion: nextVersion,
      credentialsSyncStatus: 'pending',
    });
    return {
      ok: true,
      deviceId,
      credentialsVersion: nextVersion,
      credentialsSyncStatus: 'pending',
    };
  }

  async markCredentialsSynced(
    deviceId: string,
    status: 'synced' | 'error' = 'synced',
  ) {
    const { devices } = await this.load();
    const d = devices.find((x) => x.deviceId === deviceId);
    if (!d) {
      this.logger.warn(
        `markCredentialsSynced: dispositivo "${deviceId}" no está en el registro; omitiendo actualización de estado de sync.`,
      );
      return {
        ok: true,
        deviceId,
        credentialsVersion: 1,
        credentialsSyncStatus: status,
        unregistered: true as const,
      };
    }
    await this.updateDeviceRuntime(deviceId, {
      credentialsSyncStatus: status,
      lastSeenAt: new Date().toISOString(),
      connectionStatus: status === 'synced' ? 'online' : 'offline',
    });
    const after = await this.load();
    const d2 = after.devices.find((x) => x.deviceId === deviceId);
    if (!d2) {
      return {
        ok: true,
        deviceId,
        credentialsVersion: 1,
        credentialsSyncStatus: status,
      };
    }
    return {
      ok: true,
      deviceId,
      credentialsVersion: d2.credentialsVersion ?? 1,
      credentialsSyncStatus: d2.credentialsSyncStatus ?? status,
    };
  }

  private async updateDeviceRuntime(
    deviceId: string,
    patch: Partial<RaspberryDevice>,
  ) {
    if (this.db.isEnabled()) {
      const sets: string[] = [];
      const params: unknown[] = [];
      const push = (sql: string, value: unknown) => {
        params.push(value);
        sets.push(`${sql} = $${params.length}`);
      };
      if (patch.connectionStatus)
        push('connection_status', patch.connectionStatus);
      if (patch.lastSeenAt !== undefined) {
        push(
          'last_seen_at',
          patch.lastSeenAt ? new Date(patch.lastSeenAt).toISOString() : null,
        );
      }
      if (patch.credentialsVersion !== undefined)
        push('credentials_version', patch.credentialsVersion);
      if (patch.credentialsSyncStatus)
        push('credentials_sync_status', patch.credentialsSyncStatus);
      if (sets.length === 0) return;
      params.push(deviceId);
      const wherePos = params.length;
      const sql = `UPDATE devices_registry SET ${sets.join(', ')}, updated_at = now() WHERE device_id = $${wherePos}`;
      await this.db.query(sql, params as any[]);
      return;
    }

    const data = await this.load();
    const idx = data.devices.findIndex((x) => x.deviceId === deviceId);
    if (idx < 0) {
      throw new NotFoundException(`dispositivo no encontrado: ${deviceId}`);
    }
    const cur = data.devices[idx];
    data.devices[idx] = this.validateDevice({ ...cur, ...patch });
    await mkdir(join(this.registryPath, '..'), { recursive: true });
    await writeFile(this.registryPath, JSON.stringify(data, null, 2), 'utf8');
  }
}
