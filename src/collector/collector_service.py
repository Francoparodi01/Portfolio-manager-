"""
Collector Service con soporte Telegram MFA
"""
import logging
import os
from datetime import datetime
from typing import Dict, Optional

from .cocos_scraper import CocosScraper
from .raw_storage import RawStorage
from ..collector.data.normalizer import DataNormalizer
from ..collector.data.repository import PortfolioRepository

logger = logging.getLogger(__name__)


class CollectorService:
    """
    Servicio de recolección con Telegram MFA
    """
    
    def __init__(
        self,
        database_url: str,
        raw_data_path: str,
        cocos_email: str,
        cocos_password: str,
        telegram_bot_token: str = None,
        telegram_chat_id: str = None,
        headless: bool = False
    ):
        self.cocos_email = cocos_email
        self.cocos_password = cocos_password
        
        self.scraper = CocosScraper(
            headless=headless,
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id
        )
        
        self.raw_storage = RawStorage(raw_data_path)
        self.normalizer = DataNormalizer()
        self.repository = PortfolioRepository(database_url)
    
    def collect_snapshot(self) -> bool:
        """
        Ejecuta recolección completa
        """
        logger.info("="*60)
        logger.info("INICIANDO RECOLECCIÓN DE SNAPSHOT")
        logger.info("="*60)
        
        try:
            # PASO 1: Login con Telegram MFA
            logger.info("PASO 1/4: Login con MFA via Telegram...")
            
            if not self.scraper.login_with_telegram_mfa(
                self.cocos_email,
                self.cocos_password
            ):
                logger.error("Login falló")
                return False
            
            # PASO 2: Scraping
            logger.info("PASO 2/4: Ejecutando web scraping...")
            
            raw_data = self.scraper.scrape_portfolio()
            
            if not raw_data or not raw_data.get('Posiciones'):
                logger.error("No se obtuvieron datos del portfolio")
                return False
            
            logger.info(f"✓ Scraping exitoso: ${raw_data.get('ValorTotal', 0):,.2f}")
            
            # PASO 3: Guardar Raw
            logger.info("PASO 3/4: Guardando raw data...")
            
            raw_snapshot_id = self.raw_storage.save(
                data=raw_data,
                source='cocos_scraper',
                metadata={
                    'scraper_version': '2.0',
                    'timestamp': raw_data.get('timestamp')
                }
            )
            
            logger.info(f"✓ Raw data guardado: ID {raw_snapshot_id}")
            
            # PASO 4: Normalizar y Persistir
            logger.info("PASO 4/4: Normalizando y persistiendo...")
            
            normalized = self.normalizer.normalize_portfolio(
                raw_data=raw_data,
                raw_snapshot_id=raw_snapshot_id
            )
            
            portfolio_id = self.repository.save_portfolio_snapshot(
                snapshot_data=normalized['portfolio'],
                positions_data=normalized['positions'],
                raw_snapshot_id=raw_snapshot_id
            )
            
            logger.info(f"✓ Snapshot guardado: Portfolio ID {portfolio_id}")
            
            # Notificar éxito
            if self.scraper.telegram_enabled:
                self.scraper.send_telegram_message(
                    f"✅ <b>Snapshot exitoso</b>\n\n"
                    f"Valor: ${normalized['portfolio']['total_value']:,.2f}\n"
                    f"Posiciones: {len(normalized['positions'])}\n"
                    f"ID: {portfolio_id}"
                )
            
            logger.info("")
            logger.info("="*60)
            logger.info("RECOLECCIÓN COMPLETADA EXITOSAMENTE")
            logger.info("="*60)
            
            return True
            
        except Exception as e:
            logger.error(f"Error durante recolección: {e}", exc_info=True)
            
            if self.scraper.telegram_enabled:
                self.scraper.send_telegram_message(f"❌ Error: {e}")
            
            return False
            
        finally:
            self.scraper.close()
            self.repository.close()