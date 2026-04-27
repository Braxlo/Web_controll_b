export type RaspberryDevice = {
  deviceId: string
  name: string
  /** IP o hostname de la Raspberry (panel Flask, etc.) */
  host: string
  /** Puerto del panel local en la Pi (por defecto 8000) */
  panelPort: number
}

export type DeviceRegistryFile = {
  devices: RaspberryDevice[]
}
