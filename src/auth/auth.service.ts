import {
  HttpException,
  Injectable,
  InternalServerErrorException,
  Logger,
  ServiceUnavailableException,
  UnauthorizedException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { createHash } from 'crypto';
import { DatabaseService } from '../database/database.service';

@Injectable()
export class AuthService {
  private readonly logger = new Logger(AuthService.name);

  constructor(
    private readonly db: DatabaseService,
    private readonly jwt: JwtService,
  ) {}

  async login(usernameRaw: string, passwordRaw: string) {
    const username = usernameRaw.trim();
    const password = passwordRaw;
    if (!username || !password) {
      throw new UnauthorizedException('Usuario y contraseña requeridos');
    }
    if (!this.db.isEnabled()) {
      throw new ServiceUnavailableException(
        'Base de datos no disponible para autenticación',
      );
    }
    try {
      const { rows } = await this.db.query<{
        password_hash: string;
        is_active: boolean;
      }>(
        `SELECT password_hash, is_active FROM admin_users WHERE username = $1`,
        [username],
      );
      const row = rows[0];
      if (!row || !row.is_active) {
        throw new UnauthorizedException('Credenciales invalidas');
      }
      const hash = createHash('sha256').update(password).digest('hex');
      if (hash !== row.password_hash) {
        throw new UnauthorizedException('Credenciales invalidas');
      }
      const accessToken = await this.jwt.signAsync({ sub: username });
      return { accessToken };
    } catch (e) {
      if (e instanceof HttpException) {
        throw e;
      }
      const err = e as Error;
      const msg = err.message ?? '';
      this.logger.error(`login interno: ${msg}`, err.stack);

      if (
        /ECONNREFUSED|ETIMEDOUT|ENOTFOUND|getaddrinfo|connect ECONNREFUSED/i.test(
          msg,
        ) ||
        /password authentication failed|no pg_hba.conf entry|too many connections/i.test(
          msg,
        ) ||
        /database .* does not exist|role .* does not exist/i.test(msg)
      ) {
        throw new ServiceUnavailableException(
          'No se pudo conectar a PostgreSQL. Revise DB_HOST, DB_PORT, credenciales y que el contenedor del backend alcance la base de datos.',
        );
      }

      if (/relation .* does not exist/i.test(msg)) {
        throw new ServiceUnavailableException(
          'Falta el esquema en la base de datos (p. ej. tabla admin_users). Arranque el backend con DB_SYNCHRONIZE=true al menos una vez o aplique las migraciones.',
        );
      }

      const dev = process.env.NODE_ENV !== 'production';
      throw new InternalServerErrorException(
        dev
          ? `Login falló: ${msg}`
          : 'Error interno al iniciar sesión (revise PostgreSQL y tabla admin_users).',
      );
    }
  }

  getSubjectFromUserJwt(
    headers: Record<string, string | string[] | undefined>,
  ): string | null {
    const token = this.extractBearer(headers);
    if (!token) return null;
    try {
      const payload = this.jwt.verify<{ sub?: string }>(token);
      const sub = payload?.sub;
      return typeof sub === 'string' && sub.length > 0 ? sub : null;
    } catch {
      return null;
    }
  }

  /**
   * Acepta `INGESTION_SECRET` (dispositivos / proxy sin sesión) o JWT de admin (panel web).
   * Si no hay `INGESTION_SECRET` en entorno, el comportamiento heredado es no exigir token.
   */
  assertPlatformBearer(
    headers: Record<string, string | string[] | undefined>,
  ): void {
    const token = this.extractBearer(headers);
    const secret = process.env.INGESTION_SECRET?.trim();
    if (!secret) {
      if (!token) return;
      try {
        this.jwt.verify(token);
        return;
      } catch {
        throw new UnauthorizedException('token invalido');
      }
    }
    if (token === secret) return;
    try {
      this.jwt.verify(token);
      return;
    } catch {
      throw new UnauthorizedException('token invalido');
    }
  }

  private extractBearer(
    headers: Record<string, string | string[] | undefined>,
  ): string {
    const auth = headers['authorization'];
    if (typeof auth !== 'string') return '';
    const trimmed = auth.trim();
    const m = /^Bearer\s+(.+)$/i.exec(trimmed);
    if (m) return m[1]!.trim();
    // Valor crudo sin prefijo (Postman a veces pega solo el secreto en Authorization)
    if (trimmed.length > 0 && !/\s/.test(trimmed)) {
      return trimmed;
    }
    return '';
  }
}
