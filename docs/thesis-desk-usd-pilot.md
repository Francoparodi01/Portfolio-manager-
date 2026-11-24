# Piloto de selección de tesis en USD

Implementado y ejecutado el 13/11/2026. Evalúa las acciones estadounidenses subyacentes
del 01/09 al 31/10/2026, según la última elección del usuario. Es un experimento de
selección de tesis por eventos; no reproduce la cartera CEDEAR ni sus reglas RISK_V2.

## Ampliación a 50 acciones

Ejecutada a pedido del usuario con el mismo ranking IVV al 31/08, reglas, modelo,
costos y corte de precios que la corrida de 25. Los 124 artefactos de referencia
comprobados y sus 135 filas de resultados se conservaron sin cambios.
Es una muestra ampliada que incluye la anterior, no una validación independiente.

Resultado: 50 acciones seleccionadas, 46 con publicaciones en el archivo,
166 documentos agrupados en 92 eventos, 14 BUY y 78 CASH.
PLTR, HD, CRWD y ANET quedan sin publicaciones en el archivo, sin reemplazarlas.
Hay 51 series de precios —50 acciones e IVV—, 4.845 velas y ninguna falla de fuente.
Se reutilizaron 45 decisiones y 26 series; las otras 47 decisiones requirieron
75 llamadas locales, que sumaron 203,0 segundos. Replay offline: 92/92.

| Horizonte | Pares principales | BUY / CASH | Agente neto | HOLD neto | SMA20 neto |
|---|---:|---:|---:|---:|---:|
| 5 ruedas | 60 | 10 / 50 | −0,30% | −1,86% | +0,13% |
| 10 ruedas | 60 | 10 / 50 | −0,27% | −1,48% | −0,28% |
| 20 ruedas | 35 | 4 / 31 | −0,76% | −0,36% | −1,83% |

Medias por evento con costos hipotéticos, no PnL de cartera. Quedan 25 ventanas
principales de veinte ruedas inmaduras. Persisten las alertas sobre MSFT y LLY.
No hay una ventaja consistente frente a los comparadores: a cinco ruedas pierde
frente a SMA20 y a veinte frente a HOLD.

Corrida ampliada: outputs/thesis-usd-pilot-sep-oct-2026-50-v1/.
report.md contiene la evaluación completa; comparison-25-50.md y
comparison-25-50.json comparan las 25 originales, las 25 añadidas y el conjunto.
validation.json conserva las pruebas y la comprobación de igualdad de resultados.

    # Consultar, reproducir o evaluar las 50 acciones
    python -m scripts.pilot_thesis_usd status --cohort-size 50
    python -m scripts.pilot_thesis_usd replay --cohort-size 50
    python -m scripts.pilot_thesis_usd evaluate --cohort-size 50

    # Crear o continuar esta ampliación, conservando las primeras 25
    python -m scripts.pilot_thesis_usd run --cohort-size 50 --reuse-run outputs/thesis-usd-pilot-sep-oct-2026-v1

    # Actualizar únicamente los precios de las 50 cuando maduren las ventanas
    python -m scripts.pilot_thesis_usd capture --cohort-size 50 --refresh-prices
    python -m scripts.pilot_thesis_usd evaluate --cohort-size 50

El tamaño se parametriza con --cohort-size. Cada tamaño tiene su directorio
predeterminado; una corrida congelada no admite cambiar de tamaño sobre el mismo
--output. --reuse-run exige reglas, calendario, fuentes, modelo e identidades
iguales, y conserva el corte de precios original. No se combina con --refresh-prices.
La comparación entregada es una captura fechada; una actualización posterior de
precios produce una evaluación nueva, no modifica aquella comparación.

Pruebas tras la ampliación: 193 passed, incluidas 22 del piloto USD.

## Resultado de la primera corrida de 25 acciones

Se congelaron las 25 mayores posiciones de IVV del 31/08 con mapeo SEC en el catálogo.
24 tienen publicaciones en el archivo del período; PLTR queda sin evento, sin reemplazarla.
78 documentos forman 45 publicaciones únicas por emisor/accession.
Research, Contrarian y PM produjeron 40 decisiones CASH y 5 BUY, sin errores de transporte
ni validación. Son tres roles del mismo Qwen 3.5 9B, no verificadores independientes.

La corrida local usó la GPU: Ollama informó 5.779.509.738 bytes cargados en VRAM.
71 llamadas (45 Research, 20 Contrarian, 6 PM) sumaron 217,7 segundos de solicitud.
Las 45 decisiones se reprodujeron offline con las respuestas archivadas.
No hubo API paga, órdenes, cambios de capital, commit, push ni deploy.

Se capturaron 2.470 velas diarias en USD para las 25 acciones e IVV, desde julio hasta
el último cierre completo del 12/11. No hubo fallas de fuente ni splits en este intervalo.

| Horizonte | Pares principales | BUY / CASH | Agente neto | HOLD neto | SMA20 neto |
|---|---:|---:|---:|---:|---:|
| 5 ruedas | 30 | 3 / 27 | +0,03% | −1,60% | −0,58% |
| 10 ruedas | 30 | 3 / 27 | −0,10% | −1,91% | −1,09% |
| 20 ruedas | 18 | 2 / 16 | −0,80% | +0,13% | −2,00% |

Son medias por evento con costos hipotéticos, **no retorno de una cartera financiada ni
PnL de la cuenta**. Los tres comparadores usan los mismos pares. A 20 ruedas quedan
12 ventanas principales inmaduras; la última termina el 27/11/2026.

Solo tres compras entran en la muestra principal sin solapamiento: NVDA, AMD y LLY.
En promedio habían subido 2,39% en las cinco ruedas anteriores al corte, y rindieron
0,33% neto en las cinco ruedas posteriores a la entrada y −1,04% a diez.
Esto no demuestra anticipación ni una ventaja estadística: la muestra de compras es pequeña.
El mejor resultado relativo de CASH frente a HOLD en ciertos horizontes no prueba capacidad
para descubrir información que el mercado todavía no incorporó.

Dos propuestas tienen alertas de calidad:

- MSFT trató un acuerdo fechado en octubre de 2025 como una tesis nueva. Esta decisión
  permanece en el conjunto completo de 45; ya quedaba fuera de la muestra principal
  por la regla de solapamiento fijada antes de ver resultados.
- El PM de LLY agregó cifras ausentes del contexto suministrado. La cita sí dice que
  eleva guidance, pero los números de la explicación no están sustentados por su input.

Las alertas no modifican decisiones ni retornos. Así se mide el comportamiento real
del agente y se conserva el error para una futura versión del clasificador.

## Uso

Desde la raíz principal del proyecto:

    # Estado, reproducción y medición sin inferencia ni red
    python -m scripts.pilot_thesis_usd status
    python -m scripts.pilot_thesis_usd replay
    python -m scripts.pilot_thesis_usd evaluate

    # Continuar una ejecución parcial; conserva cada respuesta y decisión
    python -m scripts.pilot_thesis_usd run

    # Actualizar precios al madurar ventanas pendientes, sin reclasificar
    python -m scripts.pilot_thesis_usd capture --refresh-prices
    python -m scripts.pilot_thesis_usd evaluate

El comando prepare congela universo, documentos, calendario, reglas, fuentes y digest del modelo.
capture reutiliza la captura si existe; --refresh-prices conserva la anterior y obtiene
otra con nueva fecha. decide --max-events N permite pausas acotadas.
--retry-errors reintenta únicamente decisiones UNKNOWN; no altera CASH ni BUY.
replay no llama a Ollama y falla si las respuestas, fuentes o decisiones no coinciden.

Ollama debe estar disponible en 127.0.0.1:11434 solo para preparar una corrida nueva
o ejecutar inferencia. Se usa el mismo modelo instalado, qwen3.5:9b, cuyo digest queda
congelado. El piloto no inicia servidores ni cambia drivers, dispositivos o variables
persistentes. El servidor que ya estaba activo utilizó GPU en esta corrida.

No cambiar el protocolo existente para optimizar resultados. Una nueva versión de
estrategia requiere otro directorio --output y debe compararse como experimento separado.

## Contrato de medición

- El evento agrupa un accession SEC y sus anexos. Cada llamada ve únicamente esa
  publicación; no recibe precios ni documentos posteriores. La aceptación SEC es
  el corte reconstruido de disponibilidad, separado de la descarga real en noviembre.
- El extractor conserva citas exactas de 20 palabras, hasta 30 ventanas por documento,
  y contexto circundante. No equivale a revisión íntegra ni cobertura completa de noticias.
- Entrada en la primera apertura regular estrictamente posterior a publicación + 60 minutos.
  Salida al cierre de la rueda 5, 10 o 20, contando la entrada como rueda 1.
- Calendario [NYSE 2026](https://www.nyse.com/trade/hours-calendars), incluyendo feriados,
  cambio horario y cierre anticipado del 27/11. No se cuentan días calendario como ruedas.
- Costos de investigación: comisión 0,60% y deslizamiento 0,15% por lado; no representan
  las tarifas de un broker estadounidense. El costo entra en los precios de compra/venta.
- Dividendos en efectivo según ex-fecha, excluyendo la fecha de entrada e incluyendo
  la de salida. Sin reinversión. Los OHLC de Yahoo ya están ajustados por splits:
  si existe un split en la captura se bloquea el símbolo hasta verificarlo, sin doble ajuste.
- HOLD compra cada acción en las mismas fechas del evento, incluso si el agente elige CASH.
  SMA20 compra si el último cierre anterior al corte de decisión supera el promedio de
  los veinte cierres anteriores a ese mismo corte. CASH rinde cero sin costos.
- Se reportan todos los eventos, pero la muestra principal excluye solapamientos de
  veinte ruedas por acción independientemente de decisión y retorno. Las fechas
  compartidas entre acciones todavía generan dependencia; no se afirma significancia.
- Las ventanas inmaduras, agujeros de precios y errores del modelo tienen estado explícito.
  Ningún faltante se rellena como cero. Las comparaciones usan un denominador común.
- El precio es de la versión descargada ahora y puede contener revisiones. El diseño
  y la inferencia son retrospectivos; el segundo post es posterior a octubre.
  Las restricciones del prompt no prueban que los pesos del modelo carezcan de
  conocimiento posterior. Se necesita validación prospectiva para demostrar una ventaja.

## Artefactos

Corrida: outputs/thesis-usd-pilot-sep-oct-2026-v1/.

- report.md, result.json, outcomes.csv y outcomes.json: comparación y detalle.
- protocol.json, events.json: protocolo, calendario, universo, fuentes y citas.
- calls/ y decisions/: entradas, respuestas, decisiones, fechas de ejecución y hashes.
- market.json y market_raw/: precios, dividendos, originales y recibos verificables.
- market_snapshots/ y evaluations/: versiones preservadas al actualizar precios/resultados.
- validation.json: comandos, pruebas, replay e integridad de la corrida entregada.

Código: src/thesis_desk/usd_pilot.py, usd_pilot_market.py y scripts/pilot_thesis_usd.py.
Pruebas: tests/test_thesis_desk_usd_pilot.py.
