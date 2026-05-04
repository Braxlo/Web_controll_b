export type RaspberryDevice = {
  deviceId: string;
  name: string;
  /** IP o hostname de la Raspberry (panel Flask, etc.) */
  host: string;
  /** Puerto del panel local en la Pi (por defecto 8000) */
  panelPort: number;
  /** Token tecnico para heartbeat/ingesta dispositivo -> backend */
  apiKey?: string;
  /** Estado de conexion reportado por heartbeat */
  connectionStatus?: 'pending' | 'online' | 'offline';
  /** ISO datetime de ultimo heartbeat */
  lastSeenAt?: string;
  /** Version logica de credenciales publicadas para el dispositivo */
  credentialsVersion?: number;
  /** Estado de sincronizacion de credenciales */
  credentialsSyncStatus?: 'pending' | 'synced' | 'error';
};

export type DeviceRegistryFile = {
  devices: RaspberryDevice[];
};
