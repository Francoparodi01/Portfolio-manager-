# Output Style Guide

## Objetivo

Todos los outputs deben ser breves, claros, auditables y consistentes entre Telegram, CLI y Monitor.

## Orden de lectura

1. Que esta pasando.
2. Que significa.
3. Que accion existe, si corresponde.
4. Evidencia principal.
5. Alcance y limitaciones.

## Reglas

- Espanol claro, sin tecnicismos si no agregan decision.
- Sin emojis decorativos en outputs nuevos.
- Sin separadores largos por defecto.
- Una conclusion primero; detalle despues.
- No mostrar stack traces al usuario.
- No llamar igual a cosas distintas: ejecucion real, plan, radar, shadow y auditoria deben quedar separados.
- No usar precision falsa: porcentajes con 1 decimal salvo necesidad.
- Timestamp y cobertura al pie, no al comienzo, salvo `status`.

## Longitudes objetivo

| Tipo | Limite objetivo |
|---|---:|
| Respuesta rapida | 2 a 6 lineas |
| Status | 5 a 10 lineas |
| Decision individual | 8 a 15 lineas |
| Portfolio | 10 a 20 lineas |
| Radar | top 3 a 5 + resumen |
| Performance | resumen ejecutivo + detalle opcional |
| Auditoria | resultado primero, metodologia despues |
| Error | 2 a 5 lineas + accion posible |

## Terminos canonicos

| Concepto | Usar | Evitar cuando confunde |
|---|---|---|
| Orden/fill real | ejecucion real | decision, recomendacion |
| Recomendacion operativa | plan | ejecucion |
| Candidato no ejecutado | radar | trade real |
| Experimento no ejecutable | shadow | senal operativa |
| Resultado posterior | outcome | performance si no hay fill |
| Comparacion bot/manual | Bot vs Humano | performance global |

## Patron recomendado

```text
<Titulo corto>
Conclusion en una linea.

Metricas: A | B | C
Accion: ...
Motivo: ...
Riesgo: ...

Cobertura: ... | actualizado ...
```
