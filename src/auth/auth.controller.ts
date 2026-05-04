import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  HttpStatus,
  Post,
  UnauthorizedException,
} from '@nestjs/common';
import { AuthService } from './auth.service';

@Controller('auth')
export class AuthController {
  constructor(private readonly auth: AuthService) {}

  @Post('login')
  @HttpCode(HttpStatus.OK)
  async login(@Body() body: { username?: string; password?: string }) {
    return this.auth.login(body.username ?? '', body.password ?? '');
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
