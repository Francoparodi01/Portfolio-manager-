#!/bin/bash
# Asegura que /app/secrets sea escribible por el usuario actual.
# Necesario porque Docker monta volúmenes nombrados nuevos como root.
if [ -d /app/secrets ]; then
    chmod 700 /app/secrets 2>/dev/null || true
fi
exec "$@"