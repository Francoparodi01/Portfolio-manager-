# Radar diario y /top10 — validación del 20/10/2026

El PR #31 incorpora al scheduler existente una captura diaria reproducible, con
corroboración directa de BYMADATA y lectores compartidos. Parte de `cca0c56`;
`main` revisado: `1687cdcce50895a9dfacb6dae04188fd8d8d5626`.
**Preparado para revisión, desactivado por defecto y sin despliegue operativo.**
No cambia estrategias, pesos, thresholds, optimizer ni planner. No ejecuta órdenes.

## Resultado con fuentes reales

Se consultó el mercado el **20/10, 08:03–08:10 ART**, antes de la apertura, para
la rueda cerrada del **19/10**. El snapshot de cartera es del 20/10 01:06 ART;
por eso el resultado es un recálculo actual con el último cierre disponible,
no una decisión retrospectiva tomada el 19/10.

- Run: `f19a922b-350d-551d-bfeb-302d5ee8494b`.
- Corte: `2026-10-20T11:10:04.905079+00:00`.
- Entradas: `b45e8ed8627928dc92e80464b907409a37b23f1bc3d899eb23e80b3c3855bf5f`.
- Motor: `3e04cdee39f71c6202bb9e0f221c5d704b5724a7421696c2df0e847cc07ecf8e`.
- [Recibo público por instrumento y lista de rechazos](evidence/radar-daily-20261020.json).

| Evidencia | Resultado y límite |
| --- | --- |
| TradingView/BYMA vía ICE | 366/399 series con última vela de la rueda correcta; 87.749 velas válidas. ARS y geometría OHLCV verificados; plazo de liquidación y unidad primaria de volumen no declarados. |
| BYMADATA directo | 363 cierres fechados T+1/ARS, campo `precioCierre`; 247 difieren de la última vela. **0 cierres corroborados con identidad completa** porque falta el plazo de la serie primaria. |
| Volumen | BYMADATA declara `volumenNominal`; coincide numéricamente en 10/10 del Top 10. Esto no acredita unidad ni metodología de toda la historia de TradingView. |
| Comafi | Lista del 30/09/2026 recuperada el 20/10; ratio por ticker e ISIN en 9/10 candidatos. BBAS3 no tiene coincidencia verificable. Sin completar cambios históricos de ratio. |
| CCL | Referencia de DolarAPI con fecha/fuente propia, no CCL implícito por instrumento ni precio USD del CEDEAR. |
| Contexto local | SELECT por propietario, conexión de solo lectura. Se congelan cartera, sentiment, macro y eventos manuales. No se recuperan scores anteriores. |

17 series válidas tienen menos de 60 velas. Hay 33 rechazos: 26 sin metadatos
utilizables, 6 con última vela antigua (BAYN, BSN, DD, ESGU, FMCC, JCI) y
IWDA con identidad de mercado/moneda no verificada. No se buscan aliases ni
se rellenan ceros. Entre las 399 observaciones hay 347 señales técnicas
(36 BUY, 39 SELL, 272 HOLD) y 52 sin señal conservada; no son órdenes.

Yahoo no se contó como corroboración independiente: su [tabla de proveedores](https://help.yahoo.com/kb/finance/article-exchanges-data-delays-sln2310.html)
identifica BYMA `.BA` vía ICE con 30 minutos de demora, proveedor también
declarado por TradingView. Se usa [BYMADATA público](https://www.byma.com.ar/newsroom/byma-habilita-un-acceso-abierto-a-bymadata)
directamente, con consultas históricas fechadas. Sus cotizaciones instantáneas
carecen de fecha y **no bastan** para certificar un cierre histórico.

Ejemplo: TXN tiene último precio/vela 89.650 ARS, pero BYMADATA publica cierre
T+1 89.875 ARS y 2.164 nominales. El último negocio observado a las 16:57:59
coincide con 89.650. Es una diferencia de campos/metodología que exige
conciliación; no se reemplaza silenciosamente una vela ni se califica toda
discrepancia como error matemático.

## Top 10 guardado

Todos: **ESPERAR / PARTIAL**. Los huecos en posiciones conservan el ranking del
motor antes de excluir instrumentos de cartera; no se renumera ni recalcula.
Precio primario y cierre independiente se muestran por separado.

| Pos. | Ticker | Vela ARS | BYMADATA cierre T+1 ARS | Score técnico | Score ranking |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | META | 47.860 | 47.720 | 3,30 | 40,1830 |
| 2 | TXN | 89.650 | 89.875 | 4,05 | 40,1225 |
| 3 | ITUB | 15.380 | 15.380 | 3,45 | 30,2047 |
| 4 | SHOP | 2.500 | 2.585 | 4,00 | 30,1987 |
| 5 | CAAP | 161.450 | 161.475 | 6,70 | 30,1888 |
| 6 | MA | 28.800 | 30.320 | 6,25 | 30,1833 |
| 7 | STNE | 6.210 | 6.200 | 1,60 | 30,1660 |
| 8 | VST | 9.370 | 9.350 | 5,75 | 30,1547 |
| 10 | PYPL | 11.410 | 11.430 | 2,55 | 30,1389 |
| 11 | BBAS3 | 3.850 | 3.840 | 1,50 | 30,1149 |

El ranking difiere del publicado el 19/10 porque cambió el contexto congelado.
**El impacto causal sobre el ranking productivo anterior NO ES VERIFICABLE**
con este recálculo; no es un experimento de mismas entradas ni evidencia de retorno.

## Automatización y contrato de lectura

`register_daily_top10` añade un job al APScheduler existente, sólo cuando
`RADAR_TOP10_DAILY_ENABLED=true`. Intentos lunes–viernes a las 17:45, 18:45,
19:45 y 20:45 ART. Se verifica calendario, fin de negociación y demora antes
de recoger fuentes. [Comunicado 18782](https://www.byma.com.ar/comunicados/comunicado-18782-horarios):
fin 17:15; [19024](https://www.byma.com.ar/comunicados/com19024-horarios):
18:15 **desde 02/11/2026**. Margen conservador: 30 minutos; las
[fuentes públicas BYMA](https://www.byma.com.ar/byma-apis) tienen demora.
Calendario validado hasta 31/12/2026: después falla cerrado hasta revisarlo.

Lock PostgreSQL por propietario/rueda, un solo proceso por job, timeout 40 min,
publicación atómica y omisión si ya existe el run diario completo. Fallos previos
se conservan en otra carpeta; los siguientes horarios permiten reintentar.
Cero cierres independientes bloquean la publicación; cobertura parcial produce
una captura con faltantes y ESPERAR, sin rellenarla con precios viejos. Una
captura publicada no se sobrescribe en reintentos. No hay recuperación automática
de una rueda perdida después de las 20:45: al siguiente vencimiento el lector
marca el run viejo como vencido.

`RadarDiscoveryStore` persiste en las tablas existentes, sin DDL. `/top10` y
`/api/opportunities/top10` usan `fetch_top_opportunities` y el mismo propietario,
run, hash y orden. El dashboard consume ese endpoint; no calcula otro ranking.
COMPRAR/VIGILAR preservan el mapeo existente de estados; cualquier evidencia
faltante impone ESPERAR. Con los proveedores actuales faltan requisitos de
calidad/PIT y precio canónico: esta entrega no habilita recomendaciones de compra.

## Reproducción y límites de acceso

```powershell
# Sólo archivos; --env-file contiene credenciales locales, nunca se versiona.
python scripts/run_daily_top10.py --env-file <archivo-privado> --host localhost --session 2026-10-19 --output outputs/radar-daily
# Reproducir sin consultas nuevas ni escritura en SQL.
python scripts/build_radar_market_snapshot.py --inputs <bundle>/inputs --ratios <bundle>/comafi-ratios.xlsx --output <directorio-nuevo>
# Imágenes desde árbol Git comprometido; no inicia servicios.
python scripts/build_radar_images.py --revision <sha> --receipt outputs/images-receipt.json
```

Entradas privadas y respuestas se conservan localmente bajo
`outputs/radar-daily-20261020/live/2026-10-19/65e5ffe2ba344607a19803f70b061802/`.
El primer intento falló por exigir cartera y rueda en el mismo día; queda
preservado. `replay-v3` aplica el contrato corregido con ambas fechas explícitas.
Este bundle conserva respuestas BYMA parseadas y sus hashes; se recogió antes
de añadir conservación del cuerpo original `raw_response`. Los próximos
bundles guardan ambos y verifican correspondencia. No se afirma recuperación
de bytes que no se conservaron.

Docker usa una base de dependencias por ID inmutable
`sha256:8e95486558630441c661ed066367c261151b80850e2bc9562a9703899dca48f9`
y copia sólo archivos de `git archive`, sin `.env`, outputs ni código local
ajeno al commit. Otra máquina necesita exportar/importar esa base con
`docker save/load`: no está publicada ni sustituye un lock portable desde PyPI.
Frontend: Node 22 Alpine fijado por digest y `npm ci` con lock. Se generan
recibos con SHA e IDs; no se reemplazan imágenes ni contenedores operativos.

## Validación y pasos pendientes

- 205 pruebas focalizadas aprobadas: contratos independientes, datos inválidos,
  cambio horario, feriados, contexto del día siguiente, locks, idempotencia,
  caducidad, lectores y correcciones matemáticas previas.
- Once tarjetas Telegram, máximo 1.622 caracteres en este payload.
- Validación Docker, API y transporte Telegram: recibo de ejecución adjunto al
  completar la prueba aislada. No se considera validado el polling productivo.

Antes de activar: conciliar plazo y definición de cierre/volumen, completar
ratios/identidad y procedencia histórica, revisar schema ya existente y la
selección del propietario, y autorizar por separado el despliegue. Después,
observar una ejecución real posterior al cierre y el comando recibido por el
poller. PR #25 y #29 intactos. PR #31 permanece Draft, sin merge.
