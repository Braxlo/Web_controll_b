import { Injectable } from '@nestjs/common';
import { DatabaseService } from './database/database.service';

@Injectable()
export class AppService {
  constructor(private readonly db: DatabaseService) {}

  getHello(): string {
    return 'Hello World!';
  }

  async getHealth() {
    const startedAt = Date.now();
    const database = await this.db.healthCheck();
    const ok = database.ok;

    return {
      ok,
      service: 'backend',
      timestamp: new Date().toISOString(),
      uptimeSec: Math.floor(process.uptime()),
      checks: {
        api: { ok: true },
        database,
      },
      durationMs: Date.now() - startedAt,
    };
  }
}
