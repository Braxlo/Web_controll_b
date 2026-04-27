# Ingestion HTTP (Raspberry → Web controller)

La Raspberry envía CSV por HTTP. MQTT (comandos PLC) queda para una fase posterior.

## Autenticación opcional

Si defines la variable de entorno `INGESTION_SECRET`, todas las rutas bajo `/api/ingest/...` exigen:

```http
Authorization: Bearer <INGESTION_SECRET>
```

Si no defines `INGESTION_SECRET`, los endpoints quedan abiertos (solo para desarrollo en red local).

## Almacenamiento: PostgreSQL (recomendado)

Si defines `DATABASE_URL`, el backend usa PostgreSQL y crea tablas automáticamente:

- `credenciales`
- `log_energia`
- `log_eventos`
- `log_hw`
- `devices_registry`

Sin `DATABASE_URL`, cae en modo archivo (compatibilidad), usando `INGEST_DATA_DIR`:
`backend/data/devices/<deviceId>/...csv`.

## Rutas

Base: `http://<host>:<PORT>/api/ingest/<deviceId>/...`

`deviceId`: solo letras, números, `_` y `-` (ej. `barrera_norte_01`).

### Energía

- **POST** `.../log-energia` — anexa filas al CSV (si el archivo no existe, crea cabecera).
- **PUT** `.../log-energia` — reemplaza el archivo completo (debe incluir cabecera).

Cabecera obligatoria:

```text
timestamp,VS,CS,SW,VB,CB,LV,LC,LP
```

### Eventos (accesos / doble validación)

- **POST** `.../log-eventos` — anexa filas.
- **PUT** `.../log-eventos` — reemplaza archivo completo.

Cabecera obligatoria:

```text
fecha,id_persona,usuario_persona,id_vehiculo,usuario_vehiculo,resultado,direccion
```

### Hardware

- **POST** `.../log-hw` — anexa filas.
- **PUT** `.../log-hw` — reemplaza archivo completo.

Cabecera obligatoria:

```text
fecha,lectora,evento
```

### Credenciales

- **PUT** `.../credenciales` — reemplaza el archivo completo.

Cabecera obligatoria:

```text
id,tipo,nivel,usuario
```

## Cuerpo de la petición

JSON con un solo campo `csv` (texto multilínea):

```json
{
  "csv": "timestamp,VS,CS,SW,VB,CB,LV,LC,LP\n2026-04-20 12:00:01,90.2,6,347,54.12,6,230.3,0.00,0\n"
}
```

En **POST** (append), puedes enviar solo filas nuevas **sin** repetir la cabecera, o incluir cabecera (se ignora al anexar).

## Lectura (dashboard / Next.js)

Misma regla de `Authorization` si `INGESTION_SECRET` está definido.

| Método | Ruta | Query | Respuesta JSON |
|--------|------|-------|----------------|
| **GET** | `.../log-energia` | `limit` (default 500, máx 5000) | `{ deviceId, file, columns, rows, totalRows }` |
| **GET** | `.../log-eventos` | `limit` | idem |
| **GET** | `.../log-hw` | `limit` | idem |
| **GET** | `.../credenciales` | — | idem (hasta 5000 filas, JSON) |
| **GET** | `.../credenciales/raw` | — | Cuerpo **text/csv** completo (para que la Raspberry sobrescriba `credenciales.csv` local) |

`rows` son objetos `{ cabecera: valor }` por fila (últimas `limit` filas de datos).

### Frontend (proxy Next)

El frontend llama a rutas relativas `/api/backend/...`, que reenvían al Nest usando variables de entorno **solo en servidor**:

- `BACKEND_URL` — URL del Nest (ej. `http://127.0.0.1:3000`)
- `INGESTION_SECRET` — opcional, mismo valor que en el backend

Ver `Plataforma/frontend/.env.example`.

## Registro de Raspberry (IPs / panel)

Rutas bajo `/api/config/...` (mismo `INGESTION_SECRET` en **PUT** si está definido).

| Método | Ruta | Descripción |
|--------|------|-------------|
| **GET** | `/api/config/devices` | Lista `{ devices: [{ deviceId, name, host, panelPort }] }` |
| **PUT** | `/api/config/devices` | Guarda el registro completo (body igual que GET). Requiere Bearer si hay secreto. |
| **GET** | `/api/config/devices/:deviceId/ping` | Prueba `http://host:panelPort/login` en la Raspberry (panel Flask). |

Con PostgreSQL se usa tabla `devices_registry`.
Solo en fallback sin DB se usa `data/device-registry.json`.

## Ejemplo con curl (append energía)

```bash
curl -sS -X POST "http://localhost:3000/api/ingest/barrera_01/log-energia" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer TU_SECRETO" \
  -d '{"csv":"2026-04-20 12:00:01,90.2,6,347,54.12,6,230.3,0.00,0\n"}'
```
