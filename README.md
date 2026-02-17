# Copiloto Inteligente de Inversiones -- Cocos

Sistema automatizado, seguro y escalable para el análisis estratégico de
inversiones de largo plazo.

------------------------------------------------------------------------

## 🎯 Objetivo

Transformar los datos operativos del broker Cocos en información
estratégica para la toma de decisiones financieras disciplinadas,
priorizando:

-   Crecimiento sostenible
-   Control de riesgo
-   Escenarios probabilísticos
-   Auditabilidad total

Este sistema **no ejecuta órdenes**, no realiza trading automático y no
expone credenciales.

------------------------------------------------------------------------

## 🧠 Filosofía del Proyecto

-   Métricas robustas \> predicciones puntuales
-   Escenarios probabilísticos \> certezas falsas
-   Largo plazo \> especulación
-   Arquitectura modular \> scripts sueltos

------------------------------------------------------------------------

## 🏗 Arquitectura General

Collector (solo lectura)\
→ Raw Storage (HTML + JSON crudo)\
→ Normalización\
→ TimescaleDB (series temporales)\
→ Analyzer (riesgo + escenarios)\
→ Reporter (semanal)\
→ Docker always-on

------------------------------------------------------------------------

## 📦 Estructura del Proyecto

    copiloto/
    │
    ├── analysis/
    │   ├── advanced_analyzer.py
    │   ├── monte_carlo.py
    │
    ├── data/
    │   ├── db_connection.py
    │   ├── raw_storage.py
    │   ├── portfolio_repository.py
    │
    ├── db/
    │   └── schema.sql
    │
    ├── collector/
    │   ├── cocos_scraper.py
    │   └── cocos_collector.py
    │
    ├── reporter/
    │   └── weekly_reporter.py
    │
    ├── docker-compose.yml
    ├── Dockerfile
    └── main.py

------------------------------------------------------------------------

## ⚙ Requerimientos Funcionales

-   Obtención automática de posiciones y valuación.
-   Persistencia de snapshots históricos.
-   Cálculo de:
    -   Volatilidad
    -   Drawdown
    -   Concentración (HHI)
    -   Correlaciones
-   Proyecciones probabilísticas a 4 y 12 semanas.
-   Sugerencias de rebalanceo (solo recomendación).
-   Reporte semanal automatizado.

------------------------------------------------------------------------

## 🔐 Seguridad

-   Acceso al broker en modo solo lectura.
-   Credenciales gestionadas mediante Docker Secrets.
-   No se almacenan passwords en código.
-   Persistencia del dato crudo para auditoría.

------------------------------------------------------------------------

## 🗄 Base de Datos

Motor: PostgreSQL + TimescaleDB

Tablas principales:

-   `portfolio_snapshot`
-   `positions_snapshot`
-   `raw_storage`
-   `activity`
-   `prices_daily`

Uso de hypertables para manejo eficiente de series temporales.

------------------------------------------------------------------------

## 📊 Estrategia de Análisis

El sistema utiliza:

-   Ventanas móviles
-   Simulación Monte Carlo
-   Distribuciones empíricas de retornos
-   Escenarios base y estrés
-   Recalibración semanal

No utiliza modelos predictivos de alta frecuencia ni promesas de
accuracy.

------------------------------------------------------------------------

## 🐳 Infraestructura

El sistema corre en contenedores Docker:

-   Servicio DB (TimescaleDB)
-   Servicio App (Collector + Analyzer + Reporter)

Modo always-on con reinicio automático.

------------------------------------------------------------------------

## 🚀 Instalación

### 1️⃣ Clonar repositorio

    git clone <repo>
    cd copiloto

### 2️⃣ Configurar secrets

Crear carpeta `secrets/` con:

    db_password.txt
    cocos_email.txt
    cocos_password.txt

### 3️⃣ Levantar sistema

    docker-compose up -d

------------------------------------------------------------------------

## 🔄 Workflow Operativo

### Diario

-   Snapshot automático (fin de jornada)
-   Actualización de métricas

### Semanal

-   Generación de reporte
-   Evaluación manual
-   Decisiones estratégicas

------------------------------------------------------------------------

## 📉 Qué NO es este sistema

-   No es un bot de trading.
-   No ejecuta órdenes.
-   No promete predicciones mágicas.
-   No usa deep learning.
-   No busca timing de mercado.

Es una herramienta de disciplina financiera.

------------------------------------------------------------------------

## 👨‍💻 Autor

Propuesta técnica elaborada desde la perspectiva de Ingeniería en
Sistemas e Ingeniería de Requerimientos.
