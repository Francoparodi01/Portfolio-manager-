# Analytics v2 en Telegram

Comando `/analytics`, alias `/analytics_v2`, botón **Auditoría → Analytics v2**.
Consulta 180 días y entrega un resumen HTML más `quantia_analytics_v2_180d.zip`.
No dispara scraping, sincronización de atribuciones, órdenes ni cambios en datos.

El puente `scripts/run_analytics_v2.py` captura PostgreSQL en una transacción
`READ ONLY / REPEATABLE READ`. El motor y la CLI original siguen siendo offline.
La captura se limita al chat que invoca el comando. Sólo el propietario configurado
en modo single-user puede incluir filas históricas sin owner, y únicamente cuando
no existan otros propietarios en decisiones, fills o atribuciones. En multiusuario
las filas sin owner quedan fuera. Un esquema incompatible o un volumen mayor a
20.000 filas por fuente provoca un error explícito.

## Qué calcula

- Bot: `execution_plan / EXECUTED`, scope primary o planner_audit.
- Manual: fills/movimientos primarios, sin vínculo confirmado con planes seguidos.
  Si faltan las tablas para verificar vínculos, no se clasifica como manual puro.
- Seguido: atribuciones existentes; las ambiguas se cuentan y quedan fuera del EV.
- Radar: `radar / radar_audit`, separado del bot y de la ejecución real.
- Episodios conservadores por instrumento, fuente y dirección. Una repetición no
  aumenta episodios; un cambio BUY/SELL abre otro. El outcome se toma de la primera
  recomendación, sin elegir retrospectivamente la mejor. No se infieren cierres
  ni expiraciones; puede unir reaperturas que no tengan evidencia de cierre.
  La ventana de 180 días introduce censura a izquierda: no se prueba que el primer
  episodio observado haya comenzado realmente al inicio de esa ventana.
- Outcomes direccionales guardados en base decimal, sin reinvertir el signo SELL.
  Prioriza executable_outcome y exige basis canonical_cocos. Esto verifica la
  fuente de precios, **no** la integridad histórica PIT de la ventana/revisión.
- EV y distribución por episodio, concentración y sensibilidad 0/75/150/250/400 bps.
  Son escenarios de un costo round-trip por outcome, no fees reales.
- Bootstrap descriptivo por bloques de 20 fechas UTC, 5.000 remuestreos, seed 1729.
  Conserva la sección cruzada de cada fecha; sin 40 fechas deja el intervalo null
  y guarda su motivo. No acredita independencia entre episodios ni entre activos.

## Límites visibles

Toda esta captura se etiqueta `OBSERVATIONAL_NOT_PIT_VALIDATED`. Un outcome
registrado no equivale a un outcome canónico `MATURE`; los ausentes quedan
`UNAVAILABLE` con motivo, sin tratarlos como cero ni asumir `PENDING`.
`n_raw` cuenta recomendaciones enlazadas a episodios con outcome observado;
`n_recommendations` cuenta todas; `n_episodes` incluye episodios sin outcome;
`n_observed` es el denominador del EV. `n_effective` permanece null.

Economic PnL Net queda `INCOMPLETE` hasta reconciliar NAV, flujos y costos.
Matching bot/humano, IC contra benchmark y carteras shadow necesitan ventanas
exactas y evidencia adicional. Gates: `OBSERVE`; Swaps `DISABLED_SHADOW` dentro
de esta auditoría. Estos estados no cambian ninguna autoridad operativa existente.
No se publican retornos nominales mezclados como PnL ni se declara edge confirmado.

## Archivo y reproducción

El ZIP contiene `capture.json`, `report.json`, `report.html`, `metrics.csv`,
`episodes.csv`, `episode_links.csv`, `outcomes.csv` y `manifest.json`. La captura
incluye las filas utilizadas y el esquema encontrado; contiene información privada
de la cuenta. Los archivos temporales se eliminan tras el envío, también ante error.
La copia recibida en Telegram permite auditar los inputs originales sin reconsultar
tablas mutables. La manifest registra versiones, hash de inputs/código, commit base,
seed, cutoff, versiones de Python/NumPy/pandas/Pydantic, plataforma y hashes SHA256 de cada archivo. El hash del código identifica cambios
locales adicionales al commit base. No hay fixtures sintéticos en el flujo del bot.

Para repetir el cálculo offline con el mismo código y entorno numérico:

```python
import json, os
from src.analysis.analytics_v2_live import summarize, write_archive
manifest = json.load(open("manifest.json", encoding="utf-8"))
os.environ["QUANTIA_ANALYTICS_CODE_COMMIT"] = manifest["code_commit"]
capture = json.load(open("capture.json", encoding="utf-8"))
write_archive(summarize(capture), "replay.zip")
```

Misma captura, código y entorno numérico producen la misma manifest y tablas. Cambiar
plataforma o librerías puede cambiar los últimos decimales y genera otro run ID.
El contenedor ZIP
puede tener timestamps de archivo diferentes. Una nueva consulta live genera otra
captura/cutoff: no debe usarse para sobrescribir la evidencia recibida anteriormente.

```powershell
python -m pytest tests/test_analytics_v2_telegram.py tests/test_analytics_v2.py
```

Docker instala `requirements-analytics.txt` explícitamente. Después de desplegar,
el inicio del bot registra `/analytics` en la lista de comandos de Telegram.
