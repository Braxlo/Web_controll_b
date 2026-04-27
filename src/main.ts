import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { DatabaseService } from './database/database.service';

async function bootstrap() {
  if (typeof process.loadEnvFile === 'function') {
    try {
      process.loadEnvFile('.env');
    } catch {
      /* usar variables de entorno ya definidas si no existe .env */
    }
  }
  const app = await NestFactory.create(AppModule);
  const db = app.get(DatabaseService);
  if (db.isEnabled()) {
    await db.ensureReady();
  }

  const port = Number(process.env.PORT ?? 3000);
  const host = process.env.HOST ?? 'localhost';
  const apiPrefix = 'api';
  const corsOrigins = (process.env.CORS_ORIGINS ??
    'http://localhost:3005,http://127.0.0.1:3005')
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean);

  app.setGlobalPrefix(apiPrefix);
  app.enableCors({ origin: corsOrigins });
  await app.listen(port, '0.0.0.0');

  const baseUrl = `http://${host}:${port}`;
  Logger.log(`🚀 Backend corriendo en ${baseUrl}`, 'Bootstrap');
  Logger.log(`📡 API disponible en ${baseUrl}/${apiPrefix}`, 'Bootstrap');
  Logger.log(`🌐 CORS: ${corsOrigins.join(', ')}`, 'Bootstrap');
}
bootstrap();
