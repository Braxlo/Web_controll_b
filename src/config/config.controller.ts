import {
  Body,
  Controller,
  Get,
  Headers,
  Param,
  Put,
  UnauthorizedException,
} from '@nestjs/common';
import type { DeviceRegistryFile } from './device-registry.types';
import type { ModulesConfigFile } from './modules-config.types';
import { ConfigService } from './config.service';

@Controller('config')
export class ConfigController {
  constructor(private readonly config: ConfigService) {}

  private assertSecret(headers: Record<string, string | string[] | undefined>) {
    const expected = process.env.INGESTION_SECRET?.trim();
    if (!expected) return;
    const auth = headers['authorization'];
    const token =
      typeof auth === 'string' && auth.startsWith('Bearer ')
        ? auth.slice(7).trim()
        : '';
    if (token !== expected) {
      throw new UnauthorizedException('token invalido');
    }
  }

  @Get('devices')
  async listDevices() {
    return this.config.load();
  }

  @Put('devices')
  async putDevices(
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Body() body: DeviceRegistryFile,
  ) {
    this.assertSecret(headers);
    return this.config.save(body);
  }

  @Get('devices/:deviceId/ping')
  async ping(@Param('deviceId') deviceId: string) {
    return this.config.ping(deviceId);
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
    this.assertSecret(headers);
    return this.config.saveModulesConfig(body);
  }
}
