# Seguridad y acceso remoto del monitor

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
- El bot de Telegram queda limitado por allowlist en modo single-user.
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

## Opcion recomendada para uso personal: Tailscale

Para revisar el monitor desde el celular sin publicar el dashboard en internet:

1. Instalar Tailscale en la PC y en el celular.
2. Iniciar sesion con la misma cuenta/tailnet.
3. Publicar `monitor_api` solo sobre la IP Tailscale de la PC:

```env
MONITOR_BIND_ADDRESS=100.x.y.z
```

4. Recrear el monitor:

```bash
docker compose up -d --force-recreate monitor_api
```

5. Abrir desde el celular, con Tailscale activo:

```text
http://100.x.y.z:8010/
```

Esta opcion evita abrir puertos del router y evita exponer el monitor como web
publica. El token del monitor sigue siendo obligatorio.

## Opcion alternativa: Cloudflare Tunnel + Access

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

## Checklist antes de compartir capturas o reportes

- Ocultar portfolio real, movimientos reales, fills reales, chat ids y tokens.
- Revisar logs antes de mostrarlos.
- Usar capturas anonimizadas cuando el material salga del entorno privado.
- Rotar tokens si fueron copiados a chats, screenshots o documentos externos.
