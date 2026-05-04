import { HttpException, HttpStatus, Injectable, Logger } from '@nestjs/common';

/**
 * Limita intentos de login fallidos por clave (p. ej. IP) para frenar fuerza bruta.
 */
@Injectable()
export class LoginRateLimiterService {
  private readonly logger = new Logger(LoginRateLimiterService.name);
  private readonly failures = new Map<string, number[]>();
  private readonly windowMs: number;
  private readonly maxFailures: number;

  constructor() {
    const w = Number(process.env.LOGIN_RATE_LIMIT_WINDOW_MS ?? '900000');
    const m = Number(process.env.LOGIN_RATE_LIMIT_MAX ?? '10');
    this.windowMs = Number.isFinite(w) && w > 10_000 ? w : 900_000;
    this.maxFailures = Number.isFinite(m) && m >= 3 && m <= 200 ? m : 10;
  }

  /** Si ya superó el umbral en la ventana, lanza 429. */
  assertNotLocked(clientKey: string): void {
    const key = clientKey.trim() || 'unknown';
    const now = Date.now();
    const arr = (this.failures.get(key) ?? []).filter(
      (t) => now - t < this.windowMs,
    );
    this.failures.set(key, arr);
    if (arr.length >= this.maxFailures) {
      this.logger.warn(
        `login rate limit: ${key} (${arr.length} fallos en ventana)`,
      );
      throw new HttpException(
        'Demasiados intentos fallidos. Espere unos minutos e intente de nuevo.',
        HttpStatus.TOO_MANY_REQUESTS,
      );
    }
  }

  registerFailure(clientKey: string): void {
    const key = clientKey.trim() || 'unknown';
    const now = Date.now();
    const arr = (this.failures.get(key) ?? []).filter(
      (t) => now - t < this.windowMs,
    );
    arr.push(now);
    this.failures.set(key, arr);
  }

  clearFailures(clientKey: string): void {
    this.failures.delete(clientKey.trim() || 'unknown');
  }
}
