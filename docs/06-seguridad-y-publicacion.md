# Seguridad y publicacion del monitor

## Veredicto

El proyecto es razonable para uso local con token fuerte. No debe publicarse
directamente a internet exponiendo `monitor_api` sin una capa adicional de acceso.

El monitor es read-only, pero muestra estado operativo, cartera, fills,
decisiones y logs recientes. Eso lo convierte en superficie sensible.

## Medidas actuales

- `MONITOR_API_TOKEN` obligatorio.
- TOTP opcional con `MONITOR_TOTP_SECRET`.
- Endpoints API protegidos por `Authorization: Bearer` o `X-API-Token`.
- Redaccion basica de secretos en `/api/logs/recent`.
- Headers de seguridad en respuestas HTTP.
- Rate-limit simple para intentos invalidos de auth.
- Docker publica el monitor solo en `127.0.0.1` por defecto.
- Headers `X-Frame-Options`, `nosniff`, `Referrer-Policy`, `Permissions-Policy`
  y CSP basica.

## Reglas de exposicion

No exponer estos servicios directamente:

- PostgreSQL / TimescaleDB.
- Redis.
- Scheduler.
- Telegram bot.
- Scraper.
- Cualquier volumen `/app/secrets`.

El unico componente candidato a acceso remoto es `monitor_api`, y solo con:

- token largo;
- TOTP habilitado;
- tunnel privado o reverse proxy con auth;
- HTTPS;
- allowlist de usuario/correo cuando sea posible.

## Opcion recomendada: Cloudflare Tunnel + Access

Para ver el monitor desde afuera sin abrir puertos:

1. Mantener `MONITOR_BIND_ADDRESS=127.0.0.1`.
2. Crear un tunnel hacia `http://localhost:8010`.
3. Proteger la app con Cloudflare Access.
4. Permitir solo tu email.
5. Mantener igualmente `MONITOR_API_TOKEN` y, si se puede, TOTP.
6. Activar `MONITOR_TRUST_PROXY_HEADERS=true` solo si el acceso llega por ese
   tunnel/proxy confiable.

Esto evita publicar la DB o abrir el puerto en el router. El trafico sale desde
la maquina local hacia Cloudflare y el acceso queda detras de identidad.

## Sobre Vercel o Firebase

Vercel/Firebase sirven para frontend estatico, pero no resuelven bien este caso
si la API vive junto a una DB local. Para que funcionen tendrias que:

- exponer la API local a internet; o
- mover DB/API a cloud; o
- crear un proxy serverless que igual necesitaria llegar a tu red.

Para este proyecto personal, eso agrega riesgo sin mucho beneficio. Si se quiere
un frontend externo, lo correcto es que llame a una API detras de tunnel privado,
no a una DB expuesta.

## Checklist minimo

- Usar token aleatorio largo:

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

- Configurar:

```env
MONITOR_API_TOKEN=...
MONITOR_BIND_ADDRESS=127.0.0.1
MONITOR_TOTP_SECRET=...
```

- No subir `.env`.
- No publicar puertos de DB/Redis.
- Revisar logs antes de compartir capturas.
- Rotar tokens si fueron copiados a chats, screenshots o repos.
