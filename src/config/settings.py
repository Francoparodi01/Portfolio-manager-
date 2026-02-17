"""
Configuración del Copiloto de Inversiones Cocos
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# ====================
# FIX PARA WINDOWS: Configurar UTF-8
# ====================
if sys.platform == 'win32':
    # Configurar consola para UTF-8
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

# ====================
# CREDENCIALES COCOS
# ====================
# IMPORTANTE: No subir este archivo a Git con las credenciales reales
# Usar variables de entorno en producción

COCOS_EMAIL = os.getenv('COCOS_EMAIL', 'tu_email@example.com')
COCOS_PASSWORD = os.getenv('COCOS_PASSWORD', 'tu_password')
COCOS_TOTP_SECRET = os.getenv('COCOS_TOTP_SECRET', None)  # Opcional para 2FA

# ====================
# RUTAS DEL PROYECTO
# ====================
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
EXPORTS_DIR = BASE_DIR / 'exports'
DB_PATH = DATA_DIR / 'cocos_portfolio.db'

# Crear directorios si no existen
DATA_DIR.mkdir(exist_ok=True)
EXPORTS_DIR.mkdir(exist_ok=True)

# ====================
# CONFIGURACIÓN DE RECOLECCIÓN
# ====================
# Horario de mercado argentino (10:00 - 17:00)
MARKET_OPEN_HOUR = 10
MARKET_CLOSE_HOUR = 17

# Frecuencia de snapshots (en minutos)
SNAPSHOT_INTERVAL_MINUTES = 30  # Cada 30 minutos durante mercado abierto

# ====================
# CONFIGURACIÓN DE ANÁLISIS
# ====================
# Ventana de cálculo para métricas (en días)
VOLATILITY_WINDOW_DAYS = 30
PERFORMANCE_WINDOW_DAYS = 90

# Umbrales de alertas
MAX_POSITION_CONCENTRATION = 0.40  # 40% máximo por posición
MAX_DRAWDOWN_ALERT = 0.15  # Alerta si cae más de 15%

# ====================
# CONFIGURACIÓN DE REPORTES
# ====================
REPORT_FREQUENCY = 'weekly'  # 'daily', 'weekly', 'monthly'
EXCEL_TEMPLATE_NAME = 'reporte_portfolio'

# ====================
# LOGGING
# ====================
LOG_LEVEL = 'INFO'  # 'DEBUG', 'INFO', 'WARNING', 'ERROR'
LOG_FILE = DATA_DIR / 'cocos_copilot.log'


# Ejecutar navegador en modo headless
HEADLESS_BROWSER = True

# Carpeta donde guardar raw snapshots (filesystem audit)
RAW_DATA_PATH = "data/raw"

# URL conexión PostgreSQL (Timescale)
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/copiloto_inversiones"