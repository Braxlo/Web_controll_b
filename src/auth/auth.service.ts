import {
  Injectable,
  ServiceUnavailableException,
  UnauthorizedException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { createHash } from 'crypto';
import { DatabaseService } from '../database/database.service';

@Injectable()
export class AuthService {
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
    if (typeof auth !== 'string' || !auth.startsWith('Bearer ')) return '';
    return auth.slice(7).trim();
  }
}
