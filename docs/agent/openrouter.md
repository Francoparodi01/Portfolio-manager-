# OpenRouter para el agente analítico

`QUANTIA_AGENT_PROVIDER=openrouter` conecta el controlador (incluido el agente
grounded), el router semántico y la síntesis con la API de OpenRouter. El contrato
de decisiones JSON, las herramientas registradas, los límites de lectura y la
validación de evidencia siguen en Quantia.

Configuración para Nemotron 3 Ultra:

```dotenv
QUANTIA_AGENT_PROVIDER=openrouter
QUANTIA_AGENT_MODEL=nvidia/nemotron-3-ultra-550b-a55b
QUANTIA_LLM_ROUTER=nvidia/nemotron-3-ultra-550b-a55b
QUANTIA_LLM_REASONING=nvidia/nemotron-3-ultra-550b-a55b
QUANTIA_LLM_SYNTHESIS=nvidia/nemotron-3-ultra-550b-a55b
QUANTIA_LLM_VERIFIER=nvidia/nemotron-3-ultra-550b-a55b
QUANTIA_AGENT_MODEL_TIMEOUT_SECONDS=90
QUANTIA_ROUTER_TIMEOUT_SECONDS=45
QUANTIA_HARNESS_SYNTHESIS_TIMEOUT_SECONDS=90
QUANTIA_OPENROUTER_REASONING_EFFORT=low
QUANTIA_OPENROUTER_MAX_TOKENS=4096
```

Guardar la clave de inferencia en `OPENROUTER_API_KEY` o en la variable existente
`openrouter`, dentro del `.env` ignorado. La primera tiene prioridad. Una
Management API Key permite administrar claves pero no generar respuestas. El
modelo de esta configuración es el endpoint pago y requiere saldo habilitado.

El endpoint es exclusivamente `https://openrouter.ai/api/v1/chat/completions`.
Se exige soporte de parámetros JSON; no se envían opciones específicas de
Ollama. El presupuesto de salida incluye razonamiento, que no se publica ni
registra. Respuestas truncadas, errores de proveedor y contenido vacío se
rechazan. No hay cambio automático a otro modelo ante errores.

Si el router o sintetizador fallan, conservan sus fallbacks determinísticos
existentes. Eso no cuenta como una validación exitosa del modelo remoto.

Para volver al modelo local: seleccionar `QUANTIA_AGENT_PROVIDER=ollama` y
restaurar los nombres de modelos locales de todos los roles. Las llamadas de
otros componentes cuantitativos a Ollama quedan fuera de esta configuración.

Antes de activar: comprobar una llamada JSON real, la elección de una herramienta
permitida y la síntesis con fuentes, además de las pruebas de contrato y guards.

Referencias: [salidas estructuradas](https://openrouter.ai/docs/guides/features/structured-outputs),
[claves administrativas](https://openrouter.ai/docs/guides/overview/auth/management-api-keys),
[presupuesto de razonamiento](https://openrouter.ai/docs/guides/best-practices/reasoning-tokens).
