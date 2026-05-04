import { Injectable, Logger, NestMiddleware } from '@nestjs/common';
import type { NextFunction, Request, Response } from 'express';

/**
 * Registra cada petición al terminar la respuesta (método, ruta, código, ms).
 * Nest no hace esto por defecto; sin esto solo ves el mapeo de rutas al arrancar.
 */
@Injectable()
export class HttpLoggerMiddleware implements NestMiddleware {
  private readonly logger = new Logger('HTTP');

  use(req: Request, res: Response, next: NextFunction) {
    const { method, originalUrl } = req;
    const started = Date.now();
    res.on('finish', () => {
      const path = originalUrl.split('?')[0] ?? originalUrl;
      if (method === 'GET' && path === '/api/health' && res.statusCode === 200) {
        return;
      }
      const ms = Date.now() - started;
      this.logger.log(`${method} ${originalUrl} → ${res.statusCode} (${ms}ms)`);
    });
    next();
  }
}
