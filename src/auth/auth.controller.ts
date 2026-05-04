import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  HttpStatus,
  Ip,
  Post,
  UnauthorizedException,
} from '@nestjs/common';
import { AuthService } from './auth.service';
import { readLoginBody } from './login-security';

@Controller('auth')
export class AuthController {
  constructor(private readonly auth: AuthService) {}

  @Post('login')
  @HttpCode(HttpStatus.OK)
  async login(@Body() body: unknown, @Ip() ip: string) {
    const { username, password } = readLoginBody(body);
    return this.auth.login(username, password, this.clientRateLimitKey(ip));
  }

  /** IP del request (mejor con `trust proxy` en Express detrás de un reverse proxy). */
  private clientRateLimitKey(ip: string): string {
    const v = (ip ?? '').trim();
    return v.length > 0 ? v : 'unknown';
  }

  @Get('me')
  me(@Headers() headers: Record<string, string | string[] | undefined>) {
    const username = this.auth.getSubjectFromUserJwt(headers);
    if (!username) {
      throw new UnauthorizedException('Sesion no valida');
    }
    return { username };
  }
}
