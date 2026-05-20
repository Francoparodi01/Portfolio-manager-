# Multi-user sandbox

Este entorno existe para probar multiusuario sin tocar produccion ni Supabase.

## Principios

- Un scraper de servicio compartido sigue alimentando `market_prices` y `market_candles`.
- Los portfolios, decisiones y fills de cada persona se aislan por `owner_chat_id`.
- Las credenciales Cocos de usuarios nuevos no se guardan en claro: se cifran con `APP_ENCRYPTION_KEY`.
- El bot de prueba debe usar un token distinto al bot productivo.

## Levantar el sandbox

1. Copiar `.env.multiuser.example` a `.env.multiuser.local`.
2. Generar `APP_ENCRYPTION_KEY`:

   ```powershell
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

3. Levantar solo servicios locales:

   ```powershell
   docker compose -f docker-compose.multiuser.yml up -d db redis
   ```

4. Inicializar schema:

   ```powershell
   docker compose -f docker-compose.multiuser.yml run --rm scheduler python scripts/init_db.py
   ```

5. Cuando exista onboarding multiusuario, levantar procesos de prueba:

   ```powershell
   docker compose -f docker-compose.multiuser.yml up -d scheduler telegram_bot
   ```

## Puertos locales

- PostgreSQL: `localhost:55432`
- Redis: `localhost:56379`

## Seguridad operativa

- No reutilizar tokens de Telegram ni claves de produccion.
- No commitear `.env.multiuser.local`.
- No usar la cuenta scraper de servicio para portfolios de terceros.
- Antes de migrar a Supabase, rotar claves y revisar RLS/roles de DB.
