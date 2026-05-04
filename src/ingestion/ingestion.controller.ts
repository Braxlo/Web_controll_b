import {
  Body,
  Controller,
  Delete,
  Get,
  Headers,
  Param,
  Post,
  Put,
  Query,
  Req,
  Res,
  UnauthorizedException,
  Logger,
} from '@nestjs/common';
import type { Response } from 'express';
import type { Request } from 'express';
import { IngestionService } from './ingestion.service';

type CsvBody = { csv: string };
type CredBody = { id: string; tipo: 'p' | 'v'; nivel: number; usuario?: string };

@Controller('ingest')
export class IngestionController {
  private readonly logger = new Logger(IngestionController.name);

  constructor(private readonly ingestion: IngestionService) {}

  private getClientIp(req: Request) {
    const forwarded = req.headers['x-forwarded-for'];
    if (typeof forwarded === 'string' && forwarded.trim().length > 0) {
      return forwarded.split(',')[0]!.trim();
    }
    if (Array.isArray(forwarded) && forwarded.length > 0) {
      return forwarded[0];
    }
    return req.ip ?? req.socket?.remoteAddress ?? 'unknown';
  }

  private extractRows(result: unknown) {
    if (!result || typeof result !== 'object') return undefined;
    const maybeRows = (result as { rows?: unknown; totalRows?: unknown }).rows;
    if (Array.isArray(maybeRows)) return maybeRows.length;
    const maybeTotal = (result as { totalRows?: unknown }).totalRows;
    return typeof maybeTotal === 'number' ? maybeTotal : undefined;
  }

  private async withIngestLog<T>(
    req: Request,
    deviceId: string,
    handler: () => Promise<T> | T,
  ) {
    const startedAt = Date.now();
    const route = req.originalUrl || req.url || '';
    const method = req.method || 'UNKNOWN';
    const ip = this.getClientIp(req);

    try {
      const result = await handler();
      const rows = this.extractRows(result);
      this.logger.log(
        JSON.stringify({
          event: 'ingest_request',
          status: 'ok',
          method,
          route,
          deviceId,
          ip,
          durationMs: Date.now() - startedAt,
          rows,
        }),
      );
      return result;
    } catch (error) {
      const err = error as { status?: number; message?: string };
      this.logger.error(
        JSON.stringify({
          event: 'ingest_request',
          status: 'error',
          method,
          route,
          deviceId,
          ip,
          durationMs: Date.now() - startedAt,
          httpStatus: err?.status ?? 500,
          message: err?.message ?? 'unknown error',
        }),
      );
      throw error;
    }
  }

  private assertSecret(headers: Record<string, string | string[] | undefined>) {
    const expected = process.env.INGESTION_SECRET?.trim();
    if (!expected) return;
    const auth = headers['authorization'];
    const token =
      typeof auth === 'string' && auth.startsWith('Bearer ')
        ? auth.slice(7).trim()
        : '';
    if (token !== expected) {
      throw new UnauthorizedException('token de ingestion invalido');
    }
  }

  private parseLimit(raw?: string) {
    const n = Number.parseInt(String(raw ?? '500'), 10);
    if (!Number.isFinite(n) || n < 1) return 500;
    return Math.min(n, 5000);
  }

  @Get(':deviceId/log-energia')
  getEnergia(
    @Param('deviceId') deviceId: string,
    @Query('limit') limit: string | undefined,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.readLog(deviceId, 'log_energia.csv', this.parseLimit(limit)),
    );
  }

  @Get(':deviceId/log-eventos')
  getEventos(
    @Param('deviceId') deviceId: string,
    @Query('limit') limit: string | undefined,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.readLog(deviceId, 'log_eventos.csv', this.parseLimit(limit)),
    );
  }

  @Get(':deviceId/log-hw')
  getHw(
    @Param('deviceId') deviceId: string,
    @Query('limit') limit: string | undefined,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.readLog(deviceId, 'log_hw.csv', this.parseLimit(limit)),
    );
  }

  /** CSV plano para descargar en la Raspberry y actualizar credenciales locales. */
  @Get(':deviceId/credenciales/raw')
  async getCredencialesRaw(
    @Param('deviceId') deviceId: string,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Res() res: Response,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    await this.withIngestLog(req, deviceId, async () => {
      const text = await this.ingestion.readRawFile(deviceId, 'credenciales.csv');
      const body = text.trim().length > 0 ? text : 'id,tipo,nivel,usuario\n';
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.send(body);
      return { rows: body.split(/\r?\n/).filter((line) => line.trim().length > 0).length - 1 };
    });
  }

  @Get(':deviceId/credenciales')
  getCredenciales(
    @Param('deviceId') deviceId: string,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.readLog(deviceId, 'credenciales.csv', 5000),
    );
  }

  /** CRUD JSON para administrar credenciales desde web controller. */
  @Get(':deviceId/credenciales/items')
  getCredencialesItems(
    @Param('deviceId') deviceId: string,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.listCredenciales(deviceId),
    );
  }

  @Post(':deviceId/credenciales/items')
  upsertCredencialesItem(
    @Param('deviceId') deviceId: string,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Body() body: CredBody,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.upsertCredencial(deviceId, body),
    );
  }

  @Delete(':deviceId/credenciales/items/:id')
  deleteCredencialesItem(
    @Param('deviceId') deviceId: string,
    @Param('id') id: string,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.deleteCredencial(deviceId, id),
    );
  }

  @Put(':deviceId/credenciales')
  async putCredenciales(
    @Param('deviceId') deviceId: string,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Body() body: CsvBody,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.putCredenciales(deviceId, body?.csv ?? ''),
    );
  }

  @Post(':deviceId/log-energia')
  async postEnergia(
    @Param('deviceId') deviceId: string,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Body() body: CsvBody,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.appendEnergia(deviceId, body?.csv ?? ''),
    );
  }

  @Put(':deviceId/log-energia')
  async putEnergia(
    @Param('deviceId') deviceId: string,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Body() body: CsvBody,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.putEnergia(deviceId, body?.csv ?? ''),
    );
  }

  @Post(':deviceId/log-eventos')
  async postEventos(
    @Param('deviceId') deviceId: string,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Body() body: CsvBody,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.appendEventos(deviceId, body?.csv ?? ''),
    );
  }

  @Put(':deviceId/log-eventos')
  async putEventos(
    @Param('deviceId') deviceId: string,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Body() body: CsvBody,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.putEventos(deviceId, body?.csv ?? ''),
    );
  }

  @Post(':deviceId/log-hw')
  async postHw(
    @Param('deviceId') deviceId: string,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Body() body: CsvBody,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.appendHardware(deviceId, body?.csv ?? ''),
    );
  }

  @Put(':deviceId/log-hw')
  async putHw(
    @Param('deviceId') deviceId: string,
    @Headers() headers: Record<string, string | string[] | undefined>,
    @Body() body: CsvBody,
    @Req() req: Request,
  ) {
    this.assertSecret(headers);
    return this.withIngestLog(req, deviceId, () =>
      this.ingestion.putHardware(deviceId, body?.csv ?? ''),
    );
  }
}
