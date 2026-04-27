import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { DatabaseService } from './database/database.service';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const db = app.get(DatabaseService);
  if (db.isEnabled()) {
    await db.ensureReady();
  }
  app.enableCors();
  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
