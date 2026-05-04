export type MqttTopicConfig = {
  id: string;
  topic: string;
  category: string;
};

export type MqttModuleConfig = {
  brokerUrl: string;
  connected: boolean;
  topics: MqttTopicConfig[];
};

export type BarrierLocation = {
  id: string;
  name: string;
};

export type BarrierControlConfig = {
  area: string;
  /** Si existe en `barriers.locations`, el servidor sincroniza `area` con el nombre de la ubicación. */
  locationId?: string;
  topic: string;
  cmdOpen: string;
  cmdClose: string;
  cmdState: string;
  cameraName: string;
  cameraStreamUrl: string;
  lastState: 'arriba' | 'abajo' | 'desconocido';
};

export type BarriersModuleConfig = {
  activeDeviceId: string;
  locations: BarrierLocation[];
  controlsByDeviceId: Record<string, BarrierControlConfig>;
};

export type SignboardConfig = {
  id: string;
  name: string;
  topic: string;
  batteryType: string;
};

export type SignboardsModuleConfig = {
  items: SignboardConfig[];
};

export type ModulesConfigFile = {
  mqtt: MqttModuleConfig;
  barriers: BarriersModuleConfig;
  signboards: SignboardsModuleConfig;
};
