import { Body, Controller, Get, Headers, Param, Put } from '@nestjs/common';
import { AuthService } from '../auth/auth.service';
import type { DeviceRegistryFile } from './device-registry.types';
import type { ModulesConfigFile } from './modules-config.types';
import { ConfigService } from './config.service';

type HeartbeatBody = {
  apiKey: string;
};

@Controller('config')
export class ConfigController {
  constructor(
    private readonly config: ConfigService,
    private readonly auth: AuthService,
  ) {}

  @Get('devices')
  async listDevices() {
    return this.config.load();
  }

  @Put('devices')
  async putDevices(
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Body() body: DeviceRegistryFile,
  ) {
    this.auth.assertPlatformBearer(headers);
    return this.config.save(body);
  }

  @Get('devices/:deviceId/ping')
  async ping(@Param('deviceId') deviceId: string) {
    return this.config.ping(deviceId);
  }

  @Put('devices/:deviceId/heartbeat')
  async heartbeat(
    @Param('deviceId') deviceId: string,
    @Body() body: HeartbeatBody,
  ) {
    return this.config.heartbeat(deviceId, body?.apiKey ?? '');
  }

  @Get('modules')
  async getModulesConfig() {
    return this.config.loadModulesConfig();
  }

  @Put('modules')
  async putModulesConfig(
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Body() body: ModulesConfigFile,
  ) {
    this.auth.assertPlatformBearer(headers);
    return this.config.saveModulesConfig(body);
  }
}
