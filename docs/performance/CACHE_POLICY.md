# Cache Policy

## Caches habilitadas o preservadas

| Cache | Clave | TTL | Fuente de verdad | Invalidacion | Riesgo stale | Fallback |
|---|---|---:|---|---|---|---|
| Portfolio live | `cocos:portfolio:snapshot[:chat_id]` | existente | scraper / DB | nuevo scrape | medio si mercado abierto | DB snapshot |
| Monitor HTML | app memory `index_html` | vida del proceso | `src/monitor/static/index.html` | restart/rebuild monitor | bajo | lectura de archivo si falta |

## Politicas propuestas

| Output | Candidato cache | TTL sugerido | Motivo | Riesgo |
|---|---|---:|---|---|
| `/help`, `/menu` | constante en proceso | vida del proceso | texto estatico | bajo |
| `/status` | no cachear completo | no aplica | estado operativo debe ser fresco | alto |
| `/portfolio` | snapshot live | 30-90 s en rueda | evitar DB si Redis caliente | medio |
| `/performance` | reporte por ventana | 5-15 min | resultado historico no cambia por segundo | bajo-medio |
| `/ledger` | view model por ventana | 5-15 min | cuello de 16 s medido | bajo-medio |
| `/radar` | no cache largo | 1-5 min | precios y liquidez cambian | medio |
| Analysis EOD | hash por evidencia | hasta nuevo snapshot | evitar recalculo completo | medio |

## Regla

No cachear decisiones sensibles ni precios operativos con TTL largo. Dentro de un mismo comando, reutilizar resultados aunque no se persistan globalmente.
