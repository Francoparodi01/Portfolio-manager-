# Technical Analysis Desk — etapa 0c

**5/5 paquetes de datos `READY` para `SHADOW_ONLY`; etapa 1 no iniciada.** Corte de mercado conservado: **15/11/2026 19:24 ART**, última rueda 13/11. La validación del 16/11 no atribuye frescura a un corte nuevo. Sin llamadas a modelos, escrituras de base, cambios productivos, commit ni push.

| Ticker | Inicio | Velas D | W completas | Ratios D disponibles | Estado y motivo |
|---|---|---:|---:|---:|---|
| AMD | 13/10/2025 | 267 | 57 | 247 | READY: calendario íntegro del tramo; volumen relativo aceptado |
| GDX | 13/10/2025 | 267 | 57 | 247 | READY: calendario íntegro del tramo; volumen relativo aceptado |
| GOOGL | 16/10/2025 | 264 | 56 | 244 | READY: inicio original; volumen relativo aceptado |
| MU | 13/10/2025 | 267 | 57 | 247 | READY: calendario íntegro del tramo; volumen relativo aceptado |
| NVDA | 13/10/2025 | 267 | 57 | 247 | READY: calendario íntegro del tramo; volumen relativo aceptado |

Total: **1.332 D / 284 W**. `history_start_override` registra los cuatro cambios y su motivo; GOOGL lleva `null`. El **10/10 sigue `UNKNOWN_TRADING_STATUS`**, con calendario intacto. Se excluyeron 17 D y 2 W por cada inicio modificado; 0b se conserva. [Manifiesto](C:/Users/Franco/Documents/Codex/worktrees/quantia-technical-desk-stage0b/output/technical-desk-stage0c/packets/manifest.json).

## Volumen y estados

La [política preregistrada](C:/Users/Franco/Documents/Codex/worktrees/quantia-technical-desk-stage0b/docs/technical-desk-stage0c-policy.json) usa **volumen D / mediana de las 20 ruedas anteriores**, excluyendo el día actual. Primeras 20: `null/WARMUP`; denominadores cero/faltantes no se imputan. Wyckoff recibe sólo volumen relativo, `volume_basis=RELATIVE_UNVERIFIED`. W conserva OHLC y contexto D; nunca suma volumen.

`quality.status=READY`, `ready=true`, `ready_scope=SHADOW_ONLY_RELATIVE_VOLUME`; **`volume_quality_status=PARTIAL`, unidad/ajustes `UNKNOWN`**. Otros fallos de datos mantienen `PARTIAL`. Es aptitud de datos, sin habilitar ejecución. Wyckoff queda separado de `OTHER_ROLES` en entradas, comparación, ledger y conteos.

Saltos: medianas de **10 ruedas antes / 10 desde el evento**, umbral **máx(post/pre, pre/post) ≥ 3**; sólo diagnóstico retrospectivo, sin reajustes ni uso como feature. **Resultado: no evaluable; fechas aplicables ausentes, cobertura `UNKNOWN`.** Los cuatro avisos encontrados en [Comafi](https://www.comafi.com.ar/custodiaglobal/eventos-corporativos.aspx) son ampliaciones de monto máximo, no ajustes. No prueba ausencia de eventos. [Evidencia](C:/Users/Franco/Documents/Codex/worktrees/quantia-technical-desk-stage0b/output/technical-desk-stage0c/known_adjustments.json).

## Contrato v3

- Nuevos `technical-desk-semantic-v3` / `technical-desk-pilot-v3` en [pilot_protocol_v3.py](C:/Users/Franco/Documents/Codex/worktrees/quantia-technical-desk-stage0b/src/technical_desk/pilot_protocol_v3.py); **v2 y sus tests intactos, comprobados por SHA-256**.
- Ruido sólo del único `PRIMARY`. Alternativos íntegros en ledger y fuera de métricas. El caso Elliott solicitado pasa sin `NOT_COMPARABLE` ni reintento; cambios de alternativos válidos no afectan el cálculo.
- Binario: estructura, dirección y anclas. `ROLE_CHANGE` queda reservado: PRIMARY→PRIMARY no permite observarlo separadamente; cambia lo medible del principal. Abstenciones: insuficiencia, sin clasificación binaria.
- `LEVEL_CHANGE` → `LEVEL_DISPERSION`: diferencias firmadas/absolutas en ticks y ATR, sin umbral. ATR **Wilder 14 D/W**, semilla SMA, precisión 34. Objetivos de distinta cantidad no se emparejan; ATR ausente: `null` con motivo. `STABLE` significa estabilidad estructural.
- Sin debate; **5 paquetes × 2 réplicas × 5 roles = 50 llamadas lógicas**, hasta 100 intentos con el único reintento; reserva máxima **USD 2**. Modelo/tarifa ausentes ⇒ **`NOT_STARTED`**. Costo observado `null`; no se ejecutó ningún piloto.

## Tres verificaciones solicitadas

1. **Baseline:** HEAD `93edb79012e49ee4ee97a91ace6c710cbf06f96f`; `git status --short` vacío. Guardadas las primeras 80 líneas de `git show --stat HEAD`. En 711 rutas rastreadas: **3 `.env.example`, 3 coincidencias `credential` y 8 `.sql`; 0 `.pem`, `.key`, `secret` o `.dump`**. Son coincidencias por nombre, sin inspeccionar contenido ni reparar nada. `git remote -v`: origin `https://github.com/Francoparodi01/Portfolio-manager-.git`, fetch/push, sin credenciales incrustadas. [Salida completa de comandos](C:/Users/Franco/Documents/Codex/worktrees/quantia-technical-desk-stage0b/output/technical-desk-stage0c/verifications/baseline.json).
2. **Respaldo:** [ZIP fuera de Git](C:/Users/Franco/Documents/Codex/evidence-backups/technical-desk-stage0b-20261116T050523Z.zip), **86 archivos de `output/technical-desk-stage0b*/`**, manifiesto SHA-256 interno/externo y checksum adyacente. CRC, hashes contra fuentes y fuentes sin cambios: **OK**. SHA-256 ZIP: `d7b04da8cc6f0af60401fbb4a8033f19c2949e78cb97edb43713fd5688b010a9`. [Verificación](C:/Users/Franco/Documents/Codex/worktrees/quantia-technical-desk-stage0b/output/technical-desk-stage0c/verifications/backup.json).
3. **Sin escrituras de base:** auditoría de 8 archivos/40 funciones alcanzables: **0 rutas identificadas a DB, `market_candles` o modelos**. El constructor ejecutado sólo escribe artefactos locales nuevos; guardas activas impiden sockets, SQLite y subprocess, verificadas con cuatro eventos sintéticos sin abrir conexiones. El helper heredado de descarga queda fuera del flujo offline. Confirmación limitada a las rutas auditadas y ejecutadas, no a toda la aplicación. [Auditoría](C:/Users/Franco/Documents/Codex/worktrees/quantia-technical-desk-stage0b/output/technical-desk-stage0c/verifications/no_database_writes.json) · [Validación dinámica y de datos](C:/Users/Franco/Documents/Codex/worktrees/quantia-technical-desk-stage0b/output/technical-desk-stage0c/verifications/artifact_validation.json).

## Pruebas ejecutadas

```text
python -m pytest -q tests/test_technical_desk_evidence.py tests/test_technical_desk_pilot_protocol.py tests/test_technical_desk_stage0c.py tests/test_technical_desk_pilot_protocol_v3.py tests/test_contextual_g3.py
→ 146 passed in 3.84s

python output/technical-desk-stage0c/verifications/verify_stage0c_artifacts.py
→ PASS: 5 paquetes idénticos al reproducir; manifiesto igual salvo hora de construcción;
  1.616 hashes D/W, ratios causales y OHLC W verificados independientemente.

python output/technical-desk-stage0c/verifications/baseline_backup.py
→ exit 0; baseline y respaldo verificados (no volver a ejecutar para reutilizar el ZIP).

python output/technical-desk-stage0c/verifications/audit_offline_paths.py
→ PASS_SCOPED_STATIC_REVIEW; 0 hallazgos.

python -m py_compile src/technical_desk/stage0c.py src/technical_desk/pilot_protocol_v3.py scripts/build_technical_desk_stage0c.py tests/test_technical_desk_stage0c.py tests/test_technical_desk_pilot_protocol_v3.py
git diff --check
→ ambos exit 0.
```

[Comando de construcción y hashes del código](C:/Users/Franco/Documents/Codex/worktrees/quantia-technical-desk-stage0b/output/technical-desk-stage0c/verifications/checks-final.json). **Etapa 0c cerrada; no se avanza a etapa 1.** Ruido real, costo y desempeño predictivo siguen sin medir.
