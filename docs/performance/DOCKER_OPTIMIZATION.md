# Docker Optimization

## Estado observado

- `Dockerfile` usa `COPY . .`, por lo tanto cambios en modulos compartidos requieren rebuild de servicios consumidores.
- El contexto de build observado fue aproximadamente `916 KB`.
- `docker-compose.yml` no monta el codigo como volumen para app services; el contenedor solo ve archivos nuevos despues de rebuild.
- Servicios afectados por modulos compartidos de output: `scheduler`, `monitor_api`, `telegram_bot`.

## Flujo correcto de deploy local/live

```powershell
python -m py_compile <archivos tocados>
python -m pytest <tests focalizados> -q
git diff --check
docker compose up -d --no-deps --build scheduler monitor_api telegram_bot
docker compose ps
docker compose logs --since 5m --tail 120 scheduler monitor_api telegram_bot
```

## Smoke seguro

```powershell
docker compose exec -T scheduler python scripts/run_analysis.py --no-persist --no-telegram --no-llm
docker compose exec -T scheduler python scripts/run_decision_timeline.py --days 2 --limit 5 --json
python scripts\benchmark_outputs.py --runs 5 --include-http --json
```

## Pendiente

- Medir tamano final de imagen por servicio.
- Separar dependencias pesadas si el startup de CLI sigue dominando.
- Evaluar healthcheck nativo de compose para `monitor_api`.
- No cambiar topologia a microservicios sin evidencia.
