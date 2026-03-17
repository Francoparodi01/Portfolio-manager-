# CÓMO INTERPRETAR EL OUTPUT
### Cocos Copilot — Guía de lectura e interpretación de reportes

---

## Reporte de cartera (run_analysis)

### Score final

El score va de −1.0 (vender todo) a +1.0 (comprar todo). En la práctica los valores útiles están entre −0.30 y +0.30:

| Rango | Decisión típica | Interpretación |
|-------|-----------------|----------------|
| +0.25 o más | BUY / ACCUMULATE | Señal fuerte — múltiples capas alineadas al alza |
| +0.10 a +0.24 | ACCUMULATE / HOLD+ | Señal moderada — hay ventaja pero no es urgente |
| −0.09 a +0.09 | HOLD | Sin ventaja clara — mantener sin cambios |
| −0.24 a −0.10 | REDUCE | Señal moderada a la baja — considerar reducción |
| −0.25 o menos | SELL / SALIR | Señal fuerte bajista — acción recomendada |

### Conviction (acuerdo entre capas)

La conviction mide qué porcentaje de las capas activas apuntan en la misma dirección que el score. Es más importante que el score mismo: un score de +0.30 con conviction 33% es mucho menos confiable que uno de +0.15 con conviction 100%.

| Conviction | Significado | Acción sugerida |
|------------|-------------|-----------------|
| 75% – 100% | Alta — capas alineadas | Actuar con confianza, tamaño normal/ampliado |
| 50% – 74% | Media — mayoría alineada | Actuar con tamaño conservador |
| 25% – 49% | Baja — señales mixtas | Reducir tamaño a mínimo o esperar confirmación |
| < 25% | Muy baja — sin consenso | No actuar — mercado sin dirección clara |

### Capas individuales (técnico / macro / sentiment)

Los tres valores bajo cada ticker muestran la contribución ponderada de cada capa al score final:

```
técnico +0.085 | macro -0.030 | sentiment +0.020
```

- `técnico +0.085`: el análisis de precio ve momentum alcista
- `macro -0.030`: el entorno macro es levemente adverso para el sector
- `sentiment +0.020`: noticias ligeramente positivas

Cuando técnico y macro coinciden en dirección, la conviction sube. Cuando divergen, la conviction baja aunque el score sea positivo.

### Acciones y qué significan

| Acción | Descripción |
|--------|-------------|
| AUMENTAR | El optimizer recomienda subir el peso. Delta positivo, gate permite compras |
| MANTENER | Sin cambios. El peso actual está dentro del rango óptimo |
| RECORTAR | El optimizer recomienda bajar el peso. Puede ser por concentración excesiva o señal negativa |
| SALIR | Score muy negativo + peso > 1%. Salida completa recomendada |
| NO AUMENTAR | La señal es positiva pero el gate CAUTIOUS bloquea nuevas compras. Esperar mejora del régimen |

### Cash negativo en el resultado esperado

Si el reporte muestra "Cash luego del ajuste" negativo, significa que el optimizer quiere comprar más de lo que se vende + el cash disponible. En la práctica esto es una recomendación de prioridad, no de ejecución literal — ejecutar primero las ventas antes de las compras.

---

## Reporte de oportunidades (run_opportunity)

### COMPRABLE_AHORA vs EN_VIGILANCIA

- **COMPRABLE_AHORA**: el precio está en zona técnica favorable, la asimetría es adecuada, y el score + conviction cumplen los umbrales. Se puede abrir posición hoy.
- **EN_VIGILANCIA**: el activo tiene potencial pero el precio no llegó a la zona de entrada todavía, o falta confirmación técnica. Agregar a watchlist, no ejecutar.

### Edge vs cartera

El edge muestra cuánto supera el score del candidato al mejor score actual en cartera. Un edge positivo (+0.05 o más) sugiere que el candidato nuevo puede ser mejor oportunidad que aumentar lo existente.

Un edge negativo no significa descartar el candidato — puede tener valor de diversificación sectorial aunque el score sea levemente inferior.

### R/R (Risk/Reward)

El R/R es el ratio upside/stop. Se calcula como la distancia al máximo de 6 meses dividido el stop sugerido (1.5 × ATR):

| R/R | Label | Interpretación |
|-----|-------|----------------|
| < 1.2 | POBRE | El riesgo es mayor que el upside — evitar o tamaño mínimo |
| 1.2 – 1.9 | MODERADA | Asimetría aceptable — tamaño estándar |
| 2.0 – 2.9 | BUENA | Buena relación riesgo/beneficio — tamaño normal |
| 3.0+ | EXCELENTE | Alta asimetría — tamaño ampliado si conviction lo acompaña |

### Qué invalida la idea

El bloque "Qué invalida la idea" lista las condiciones que convertirían la señal de positiva a negativa. Son los stops lógicos del trade, no solo técnicos. Leerlos antes de ejecutar: si alguna de esas condiciones ya existe, el setup es más débil de lo que el score sugiere.

---

## Rotation Engine

### Opciones internas vs externas

El Rotation Engine dice si conviene más aumentar una posición existente (MU, NVDA) o abrir una nueva (AVGO, TSM). La lógica:

- Si el candidato nuevo supera en `score × conviction × asimetría` a las opciones internas → abrir nuevo
- Si la opción interna tiene mejor señal combinada → aumentar lo existente
- Si ninguno cumple los umbrales mínimos → cash hasta mejor señal

### "Capital queda en cash"

Que el Rotation Engine diga "capital a cash" no es una señal de alarma. Significa que en este momento no hay candidatos con suficiente calidad para justificar desplegar ese capital. Es una decisión de preservación de capital, no de inacción por falta de análisis.

---

## Information Coefficient (IC)

El IC aparece en el reporte de análisis cuando hay suficiente historia (≥5 observaciones con outcome real). Se muestra para horizontes de 5, 10 y 20 días.

Un **IC > 0** indica que el sistema tiene poder predictivo direccional — cuando predice alza, el activo tiende a subir en ese horizonte. Un IC cercano a 0 significa que el score no tiene ventaja estadística todavía.

> Los primeros meses no habrá datos de IC porque los outcomes (retornos reales a 5/10/20 días) se rellenan con delay. Es normal — el sistema empieza a aprender desde el primer uso.

---

## Contexto macro Argentina

### CCL y MEP

El sistema descarga el CCL (Contado con Liquidación) y el MEP (Mercado Electrónico de Pagos) en tiempo real desde dolarapi.com. La diferencia entre ambos indica el estado de las restricciones cambiarias. Un CCL muy por encima del MEP señala tensión en el mercado de capitales.

### Riesgo País

El Riesgo País (EMBI Argentina) se descarga de argentinadatos.com. El sistema usa este valor para ajustar el score de activos con alta exposición local (GGAL, YPF, MELI). Por encima de 800 pb el sistema considera el contexto "crítico".

### Impacto en el score

Para activos con exposición argentina directa, el macro score penaliza cuando:

- **CCL > $1,450** → cash ARS se derrite, penalización en activos en pesos
- **Riesgo País > 800** → señal de fuga de capital
- **Reservas BCRA < $28,000M** → riesgo de restricciones

---

## Limitaciones del sistema

> ⚠️ El sistema usa datos de yfinance para análisis técnico. Algunos tickers de Cocos Capital no tienen equivalente exacto en yfinance (BRKB requiere BRK-B, YPFD es local). Estos están en la `YFINANCE_BLACKLIST` y se excluyen del análisis técnico pero no del universo de mercado.

> ⚠️ El sentiment score via RSS es una proxy imperfecta. Los feeds de Yahoo Finance pueden estar desactualizados para tickers de baja cobertura mediática. Para los tickers más líquidos (NVDA, AAPL, CVX) funciona bien; para tickers latinoamericanos la cobertura es limitada.

> ⚠️ El optimizer Black-Litterman asume que los retornos históricos son representativos del futuro. En períodos de alta incertidumbre macro (VIX > 30), los pesos óptimos históricos pueden no reflejar la realidad actual. Por eso existe el Risk Gate — para desactivar el optimizer en esas condiciones.

---

> Sistema cuantitativo multicapa — no es asesoramiento financiero.
