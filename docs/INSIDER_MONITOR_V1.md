# Quantia — Monitor de operaciones públicas v1

**Estado:** implementación inicial de SEC Form 4. Sin ejecución operativa ni cambios en las decisiones de inversión. La ruta /insiders es solo de lectura.

## Alcance real

- SEC EDGAR: formularios **4 / 4/A**, únicamente movimientos **no derivados** con códigos P (compra) y S (venta). **P/S pueden ser operaciones privadas o de mercado abierto**.
- Cruce obligatorio de issuerCik con el CIK de la empresa monitoreada: evita atribuirle a una empresa operaciones sobre otras compañías de las que es accionista.
- Registra por separado **fecha operada** (occurred_on), **fecha presentada** (filed_on) y **primera captura por Quantia** (first_seen_at). Las enmiendas tienen accession distinto; no se recalcula el pasado ni se deduce conocimiento previo.
- Guarda URL oficial, SHA-256 del XML, accionista/cargo, cantidad y monto estimado si hay precio.
- Los eventos son *evidencia descriptiva* y **no** modifican Decision Engine, optimizer, radar, planner, órdenes ni riesgo.
- Congreso de EE. UU.: **no integrado en v1**. House y Senate deben procesarse por separado. No mezclar los casos ni simular cobertura.

## Ejecución local (PowerShell, desde la raíz del repo)

Primero configurar un User-Agent identificable exigido por la SEC, con una dirección de contacto válida (usar la propia):

~~~powershell
$env:SEC_USER_AGENT = "Quantia nombre@dominio.com"
python scripts/run_public_disclosure_monitor.py --tickers AMD,NVDA,MU
~~~

El comando anterior **solo consulta y devuelve JSON**; no crea tablas ni guarda resultados.

Para alimentar PostgreSQL y mostrar resultados en /insiders:

~~~powershell
python scripts/run_public_disclosure_monitor.py --tickers AMD,NVDA,MU --persist
~~~

Para ampliar a emisores de la cartera ya enlazados mediante issuer_registry (solo los US con CIK válido):

~~~powershell
python scripts/run_public_disclosure_monitor.py --portfolio --persist
~~~

El proceso precisa la misma DATABASE_URL/configuración que Quantia para persistir. El comando puede repetirse periódicamente mediante el scheduler existente o programador de tareas **después de un smoke test real**. Esta rama **no agrega por defecto un job automático** ni llama a la SEC en el despliegue.

## Consultar

- Frontend autenticado: /insiders, filtro por ticker y compra/venta, enlace al formulario original.
- API autenticada: GET /api/public-disclosures?limit=120&ticker=AMD; si aún no hay tabla, retorna state=NOT_INITIALIZED, nunca resultados inventados.
- No se rellenan valores faltantes con cero, y no se equipara fecha del trade con fecha de publicación.

## Validación y límites

- Prueba offline: python -m pytest -q tests/test_public_disclosures.py.
- Pendiente antes de habilitar captura programada: ejecutar el script contra SEC real, comprobar al menos dos formularios originales, cobertura efectiva, rate limits y persistencia idempotente en PostgreSQL.
- El escaneo se limita a 50 tickers, 1–365 días de filing date y un máximo explícito de formularios por ticker. Se consulta filings.recent; históricos paginados no se incluyen.
- Algunas presentaciones Form 4 son tardías y pueden incluir transacciones bajo planes 10b5-1; el dato **no equivale a convicción propia ni asegura rentabilidad**.
- Para congresistas, los PTRs se reportan con retrasos legales, por lo que no podrán ser usados como feed de ejecución en tiempo real.

Fuentes oficiales:
- [SEC — códigos de operación](https://www.sec.gov/edgar/searchedgar/ownershipformcodes.html)
- [SEC — acceso responsable a EDGAR](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data)
- [House — disclosures](https://disclosures-clerk.house.gov/FinancialDisclosure)
- [Senate — disclosures](https://www.ethics.senate.gov/public/index.cfm/financialdisclosure)
