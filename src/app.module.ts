import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { AuthModule } from './auth/auth.module';
import { ConfigModule } from './config/config.module';
import { DatabaseModule } from './database/database.module';
import { IngestionModule } from './ingestion/ingestion.module';

@Module({
  imports: [DatabaseModule, AuthModule, IngestionModule, ConfigModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
