import { UnauthorizedException } from '@nestjs/common';

/** Límite práctico usuario (email u otro identificador en admin_users). */
export const LOGIN_USERNAME_MAX = 254;
/** Evita cuerpos enormes / DoS en hash. */
export const LOGIN_PASSWORD_MAX = 256;

const CTRL = /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/;

export function readLoginBody(body: unknown): {
  username: string;
  password: string;
} {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return { username: '', password: '' };
  }
  const o = body as Record<string, unknown>;
  const username = typeof o.username === 'string' ? o.username : '';
  const password = typeof o.password === 'string' ? o.password : '';
  return { username, password };
}

/**
 * Rechaza entradas fuera de rango o con caracteres de control (inyección / binarios en JSON).
 * Mismo mensaje que credenciales incorrectas para no filtrar detalles.
 */
export function assertLoginInputShape(
  username: string,
  password: string,
): void {
  if (
    username.length > LOGIN_USERNAME_MAX ||
    password.length > LOGIN_PASSWORD_MAX ||
    CTRL.test(username) ||
    CTRL.test(password)
  ) {
    throw new UnauthorizedException('Credenciales invalidas');
  }
}
