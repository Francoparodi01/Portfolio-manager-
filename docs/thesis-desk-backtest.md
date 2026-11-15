# Desk por tesis: backtest experimental

El [piloto en USD](thesis-desk-usd-pilot.md), ampliado el 13/11 a 50 acciones,
ya tiene decisiones y resultados a 5/10/20 ruedas. Este documento describe el
motor integral CEDEAR; ambos experimentos conservan sus propias cohortes y límites.

Runner: `python -m scripts.backtest_thesis_desk`. Motor aislado: `src/backtesting/thesis_desk.py`.
No está conectado al bot, scheduler, scoring, planner, broker ni flota LLM.
Research, veto, selección, tamaño, ejecución y bitácora son etapas deterministas auditables.
Una prueba histórica con LLM actuales añadiría conocimiento futuro que este experimento no puede verificar.

## Reglas y procedencia

Fuentes: [guía del 7 de septiembre](https://x.com/MatiasScalbi/status/2097105195415097524)
y [actualización del 4 de noviembre ART](https://x.com/MatiasScalbi/status/2118142082275577874).
Ambas se aplican retrospectivamente al 01/09–31/10/2026. La segunda es posterior a toda la ventana;
ninguna variante constituye validación fuera de muestra o reproducción de las operaciones del autor.
Los nombres y beneficios mencionados en los posts no se usan para seleccionar activos.

| Regla | Implementación | Procedencia |
|---|---|---|
| Tesis viva | Guía elevada/reafirmada o acuerdo firmado/cerrado, oficial y disponible al corte, antigüedad ≤14 días | Guía |
| Universo | CORE: membresía IVV fechada ≤14 días; SMID: capitalización USD 3–70 mil millones con fecha y fuente | Guía |
| Instrumentos | Mapeo explícito a acción común y precio ARS; se excluyen ETF, apalancados e identidades ambiguas | Adaptación a Quantia |
| Entrada | Zona ±2% del promedio de 20 cierres anteriores a la publicación del hecho; sin perseguir gaps | Parámetros de investigación: el post no cuantifica la zona |
| Invalidación | Hecho adverso oficial que referencia la tesis; macro/técnico no generan tesis | Guía |
| Tamaño | CORE ≤8%, SMID ≤4%, starter 70%, riesgo al stop ≤1,5% del NAV; amarillo divide el tamaño por dos | CORE/starter/riesgo: guía; SMID 4%: supuesto |
| Stops | CORE 10%, SMID 15% desde el fill; venta a siguiente cotización tras latencia, incluyendo gaps y costos | SMID: guía; CORE 10% y latencia: supuestos |
| Cupo | Máximo 2 entradas semanales; una hasta la tercera rueda de la semana | Guía, sin excepción subjetiva A+ ni reposición adicional |
| Selección | Hecho más reciente, luego ticker/ID; una entrada por tesis, sin promediar ni reentrar | Adaptación explícita, no optimizada |
| RISK_V2 | Semáforo fechado: rojo/ausente bloquea, amarillo media posición; blackout fechado de balances/empleo | Actualización de noviembre |
| RISK_V2 | Recorta peso >12%; caída de 8% desde máximo observado congela compras y solicita liquidación | Actualización; liquidación y latch permanente: supuestos conservadores |

No se implementan coberturas PSQ, renta del efectivo, convicción, venta por bajo retorno esperado
ni venta por extensión respecto de una media: los posts no suministran una regla numérica completa.
No se sustituyen esas reglas por datos estimados. El calendario y las cotizaciones son BYMA/ARS;
los rendimientos no son comparables con una cuenta Alpaca en USD.

## Ejecución y contabilidad

Capital ficticio predeterminado ARS 3.000.000. Comisión 0,6% y deslizamiento adverso 0,15% por lado,
los supuestos de investigación usados por el backtest local. No son comisiones reales verificadas.
Nominales enteros, sin saldo negativo, ventas parciales con costo medio y ciclos completos.
Se decide a las 11, 13 y 16 ART. Cada compra vence en el siguiente corte/cierre;
sólo puede llenar desde 60 segundos después de la decisión, dentro de su zona incluso tras slippage.
Los stops se detectan en precios observados y venden desde 60 segundos después: no simulan fills
al nivel teórico ni garantizan un stop continuo. Ventas pendientes persisten entre sesiones.
En un mismo instante se procesan ventas antes de compras; ventas futuras no financian compras previas.
Las acciones corporativas aplican factores documentados y conocidos; no cambian el costo total.
Fracciones no soportadas producen error explícito. No se modelan dividendos en efectivo, impuestos,
profundidad, interés del efectivo o ajuste por inflación. Drawdown reportado: cierres diarios;
el freno usa cotizaciones observadas y puede dispararse antes del cierre.

## Evidencia requerida

JSON `thesis-desk-evidence-v1`, con arrays `facts`, `universe`, `risk`, `blackouts`, `coverage`.
Cada registro necesita `id` único dentro del array y `source_url` HTTPS inspeccionada.
Salvo coverage, también `available_at` con timezone y `knowledge_basis`:
`ARCHIVED_AT_TIME` o `RECONSTRUCTED_PUBLICATION`. Lo reconstruido hoy no es un archivo original.

- `facts`: `ticker`, `thesis_id`, `kind`, `source_type` SEC/IR/CNV, `published_at`, y `invalidation`
  escrita para hechos positivos. Tipos positivos: GUIDANCE_RAISED, GUIDANCE_REAFFIRMED, DEAL_SIGNED,
  DEAL_CLOSED; negativos: GUIDANCE_CUT, DEAL_CANCELLED, LEGAL_ADVERSE. Un negativo referencia
  el mismo thesis_id o `*` para todas las tesis anteriores del emisor.
- `universe`: `ticker`, `asset_type` exacto de market_prices, `currency=ARS`,
  `underlying_kind=COMMON_STOCK`, `sleeve=CORE|SMID`, `asof_at`. CORE necesita
  `membership_source=IVV`; SMID necesita `market_cap_usd`. El ticker no prueba identidad o membresía.
- `risk`: `state=GREEN|YELLOW|RED`, `valid_until`. Debe cubrir cada corte de RISK_V2.
- `blackouts`: `ticker` o `*`, `start_at`, `end_at`. Debe documentar el intervalo previo al evento;
  sólo se aplica desde available_at. Ausencia de archivo no significa ausencia de evento.
- `coverage`: `domain=facts|universe|risk|blackouts`, `start`, `end` (fechas inclusivas),
  `status=COMPLETE|PARTIAL|UNKNOWN`, `scope=DECLARED_UNIVERSE`, fuente y descripción del alcance.
  COMPLETE es una declaración de quien aporta el archivo, que debe estar respaldada por la fuente;
  no se genera automáticamente al encontrar unos pocos ejemplos.

Las velas deben tener bar_end, available_at, scraped_at, is_closed, interval=1d, cierre positivo
y política explícita RAW/UNADJUSTED. Deben ser anteriores al hecho y estar disponibles al corte.
Las zonas usan los factores de precio de acciones corporativas documentadas y conocidas al corte;
no se infiere un ratio desde precios. La captura actual conserva revisiones;
no prueba que todos los valores sean vintages originales. No usar miembros actuales de IVV
ni titulares de prensa como sustituto silencioso del archivo requerido.

### Referencia histórica desde cotizaciones registradas

`--recorded-closes` habilita la misma adaptación de precio usada por PAPER: última
cotización local registrada entre 16:30 inclusive y 17:00 exclusive ART de cada rueda.
No es el cierre oficial de BYMA. La captura incluye 120 días previos para construir
la referencia de 20 observaciones anteriores a cada publicación. El precio nominal
no se reajusta; el motor conserva su tratamiento explícito de acciones corporativas.

`market_prices.ts` proviene de `MarketAsset.scraped_at` en el colector. La referencia
conserva ese timestamp, el hash de la fila y la fecha de captura actual. La ventana
de agregación termina a las 17:00, por lo que no puede utilizarse antes de esa hora.
`available_at=17:00` documenta ese límite de reconstrucción, no la hora de publicación
de una vela oficial. La tabla admite actualizaciones: sigue siendo una captura
actual de observaciones históricas, sin demostrar la ausencia de revisiones.

La opción no rellena las velas antiguas ni modifica la base. Produce
`recorded-daily-references.json` y `price-recovery.json`, registra la adaptación en
`policy.json` y en ambos resultados, y conserva todos los restantes controles.
Los universos, hechos y riesgo faltantes siguen bloqueando su evaluación económica.

```powershell
python -m scripts.backtest_thesis_desk --start 2026-09-01 --end 2026-10-31 --db-container cocos_db --evidence outputs/thesis-history-sep-oct-2026-v2/evidence.json --recorded-closes --output outputs/thesis-desk-sep-oct-2026-recorded-20261112
```

Para repetir offline, usar `--capture` con el `capture.json` de esa corrida,
mantener `--recorded-closes` y elegir un directorio nuevo de salida.

## Uso

El colector histórico descarga las publicaciones de la ventana solicitada y 14 días
previos, sin inferencia LLM. Usa el catálogo actual sólo para descubrir emisores;
no lo convierte en prueba del universo histórico. SEC incluye 8-K, 6-K, enmiendas,
archivos de historial y todos los EX-99 vinculados. CNV resuelve los PDF adjuntos a
las presentaciones ya identificadas en la captura. Conserva cada original, URL,
SHA-256, fecha de descarga y fecha de publicación cuando la fuente la proporciona.

```powershell
python -m scripts.collect_thesis_history --source-run outputs/thesis-desk-all-local/runs/20261107T210015-4d9db4d3 --start 2026-09-01 --end 2026-10-31 --market-capture outputs/thesis-desk-sep-oct-2026-final/capture.json --output outputs/thesis-history-sep-oct-2026-v2 --memory-cache-mb 1024 --reuse-archive outputs/thesis-history-sep-oct-2026
```

La caché en RAM tiene un límite de 1 GB en este ejemplo, cuatro descargas simultáneas
y separación entre solicitudes. Repetir el comando retoma checkpoints verificados;
un cambio de código o inputs requiere otro directorio. `--reuse-archive DIRECTORIO`
permite reconstruir con los originales de una corrida terminada, verificando su
manifiesto y hashes. No copia clasificaciones ni resultados derivados. La RAM evita
lecturas y descargas repetidas; analizar texto sigue requiriendo CPU o GPU.
En una instalación sin archivo previo, omitir `--reuse-archive` y usar otro directorio
de salida. Al reanudar, mantener los mismos argumentos de la primera ejecución.

`documents.json` es material pendiente de revisión, no `facts` aprobados.
`ivv-snapshots.json` exige que la fecha devuelta coincida con la solicitada y no
infiere el mapeo histórico al instrumento local. `macro.json` guarda VIX y empleo;
no inventa la hora de publicación de los cierres del VIX. `evidence.json` incorpora
sólo los blackouts de empleo con fecha de calendario reconstruible y declara
cobertura parcial: todavía faltan los calendarios de balances. `summary.json`
cuantifica cobertura, errores y límites; `manifest.json` permite verificar originales
y código. `coverage.csv` desglosa documentos y errores por las 290 acciones identificadas
en el catálogo usado; no representa cobertura de las identidades aún desconocidas.
Fuentes públicas: [SEC](https://www.sec.gov/search-filings/edgar-application-programming-interfaces),
[iShares IVV](https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf),
[Cboe](https://www.cboe.com/tradable_products/vix/vix_historical_data) y
[BLS 2026](https://www.bls.gov/schedule/2026/home.htm).

```powershell
python -m scripts.backtest_thesis_desk --start 2026-09-01 --end 2026-10-31 --db-container cocos_db --output outputs/thesis-desk-sep-oct-2026
python -m scripts.backtest_thesis_desk --start 2026-09-01 --end 2026-10-31 --capture outputs/thesis-desk-sep-oct-2026/capture.json --evidence PATH_TO_VERIFIED_EVIDENCE.json --output outputs/thesis-desk-with-evidence
python -m pytest tests/test_thesis_desk_backtest.py -q
```

Captura con transacción REPEATABLE READ / READ ONLY, sólo tablas públicas de mercado/eventos,
sin cuentas ni credenciales de broker. Alternativamente lee una captura offline existente.
El runner rechaza sobrescribir corridas, congela policy.json antes de ejecutar y registra hashes.
Genera una bitácora y resultados JSON de las dos variantes, source-audit.json y result.md.
Sin evidencia suficiente recorre igualmente todas las ruedas y cortes y devuelve NOT_EVALUABLE
con `metrics=null`: cero fills por falta de datos no se presenta como rentabilidad del 0%.
Las valuaciones internas se conservan como trazas de contabilidad, no como performance válida.
CASH nominal es el control; HOLD requiere una cartera de referencia fechada y financiada y se
declara no evaluable en este runner. No se afirma superioridad frente a HOLD.
