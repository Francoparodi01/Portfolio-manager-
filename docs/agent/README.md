# Guía de documentación para el agente de Quantia

Revisión: 2026-10-22. Alcance: índice de contratos y fuentes verificadas en código.

El agente recibe `AGENTS.md`, `quantia-semantics.md` y `quantia-data-model.md`
en cada ejecución. El resto se consulta con `search_quantia_docs`. Buscar por el
tema concreto y terminar cuando la fuente alcance para responder.

| Tema | Documento principal | Buscar |
|---|---|---|
| Significados, estados, métricas y horizontes | [Semántica](quantia-semantics.md) | `outcome horizonte DVA` |
| Herramientas, tablas permitidas y fuente correcta | [Catálogo](quantia-data-model.md) | `herramientas ledger acceso` |
| Los cuatro shadow | [Módulos shadow](shadow-modules.md) | `Calibración Shadow V3 Learning Shadow` |
| Imágenes del análisis técnico por ticker | [Gráficos ticker](ticker-technical.md) | `imágenes gráficos ticker RSI MACD` |
| Chats, recomendaciones importadas y medición histórica | [Evidencia histórica](historical-evidence.md) | `Telegram ledger recomendaciones históricas` |
| Agente, herramientas y auditoría | [Arquitectura grounded](grounded-agent-architecture.md) | `grounded autoridad herramientas` |
| OpenRouter, Nemotron y Ollama | [Proveedor del agente](openrouter.md) | `OpenRouter Nemotron proveedor` |
| DVA y contrafactual | [Decision Lab](../decision-lab.md) | `Decision Lab PLAN HOLD` |

## Qué fuente responde cada tipo de pregunta
Para «cómo funciona», usar el contrato y los archivos de implementación citados.
Para «qué está activo», usar configuración/estado del runtime cuando una herramienta
lo exponga; un documento de diseño no demuestra un despliegue.
Para «qué resultado dio», usar el reporte o medición con corte, cohorte y horizonte.
Un replay retrospectivo se identifica como tal aunque esté dentro de una guía.

## Forma de respuesta
Dar primero el resultado o explicación que pidió el usuario. Agregar la fuente y
la limitación material. No transformar una consulta de arquitectura en una lectura
de cartera. Si no hay acceso a una fuente, describir exactamente qué falta; no
afirmar que todo el historial se perdió o que debe comenzarse de cero.

## Mantenimiento
Actualizar la fecha cuando se verifique un cambio contra código o runtime. Cada
guía debe declarar su alcance, citar implementación y distinguir contratos de
resultados fechados. Evitar claves, IDs personales y cifras operativas «actuales»
sin fecha. Las mejoras documentales no autorizan cambios en estrategias o capital.
