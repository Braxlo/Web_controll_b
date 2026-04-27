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
  app.enableCors();
  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
