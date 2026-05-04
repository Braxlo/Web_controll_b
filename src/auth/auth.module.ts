import { Module } from '@nestjs/common';
import { JwtModule } from '@nestjs/jwt';
import { AuthController } from './auth.controller';
import { AuthService } from './auth.service';
import { LoginRateLimiterService } from './login-rate-limiter.service';

@Module({
  imports: [
    JwtModule.registerAsync({
      useFactory: () => ({
        secret:
          process.env.JWT_SECRET?.trim() ||
          'cambie-jwt_secret-en-produccion-no-usar-en-prod',
        signOptions: { expiresIn: '7d' },
      }),
    }),
  ],
  controllers: [AuthController],
  providers: [AuthService, LoginRateLimiterService],
  exports: [AuthService],
})
export class AuthModule {}
