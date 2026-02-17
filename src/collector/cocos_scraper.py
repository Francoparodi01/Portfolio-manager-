"""
Cocos Scraper - Extrae datos del DOM (no usa API)
Funciona con trusted device, no necesita TOTP
"""
import logging
import time
import re
from typing import Dict, Optional
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import requests

logger = logging.getLogger(__name__)


class CocosScraper:
    """
    Scraper con soporte para MFA via Telegram
    """
    
    def __init__(self, headless: bool = False, telegram_bot_token: str = None, telegram_chat_id: str = None):
        self.driver = None
        self.headless = headless
        self.logged_in = False
        
        # Telegram
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.telegram_enabled = bool(telegram_bot_token and telegram_chat_id)
    
    def setup_driver(self):
        """Configura Chrome"""
        try:
            options = Options()
            
            if self.headless:
                options.add_argument('--headless')
            
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
            
            logger.info("Instalando ChromeDriver...")
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            logger.info("[OK] Chrome inicializado")
            
        except Exception as e:
            logger.error(f"[ERROR] Error setup: {e}")
            raise
    
    def send_telegram_message(self, message: str) -> bool:
        """
        Envía mensaje por Telegram
        
        Args:
            message: Texto a enviar
            
        Returns:
            bool: True si se envió correctamente
        """
        if not self.telegram_enabled:
            return False
        
        try:
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            data = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            
            response = requests.post(url, data=data, timeout=10)
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Error enviando mensaje Telegram: {e}")
            return False
    
    def get_telegram_mfa_code(self, timeout: int = 120) -> Optional[str]:
        """
        Espera código MFA via Telegram
        
        Args:
            timeout: Segundos máximo de espera
            
        Returns:
            str: Código MFA de 6 dígitos
        """
        if not self.telegram_enabled:
            logger.warning("Telegram no configurado")
            return None
        
        # Enviar solicitud
        self.send_telegram_message(
            "🔐 <b>CÓDIGO MFA REQUERIDO</b>\n\n"
            "Por favor envía el código de 6 dígitos que recibiste.\n\n"
            f"Tienes {timeout//60} minutos."
        )
        
        logger.info(f"Esperando código MFA por Telegram (timeout: {timeout}s)...")
        
        # Obtener último update_id
        url = f"https://api.telegram.org/bot{self.telegram_bot_token}/getUpdates"
        response = requests.get(url)
        data = response.json()
        
        last_update_id = 0
        if data['ok'] and data['result']:
            last_update_id = data['result'][-1]['update_id']
        
        # Polling por nuevos mensajes
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Obtener updates nuevos
                params = {'offset': last_update_id + 1, 'timeout': 10}
                response = requests.get(url, params=params, timeout=15)
                data = response.json()
                
                if data['ok'] and data['result']:
                    for update in data['result']:
                        last_update_id = update['update_id']
                        
                        if 'message' in update and 'text' in update['message']:
                            text = update['message']['text'].strip()
                            
                            # Buscar código de 6 dígitos
                            code_match = re.search(r'\b(\d{6})\b', text)
                            
                            if code_match:
                                code = code_match.group(1)
                                logger.info(f"[OK] Código MFA recibido: {code}")
                                
                                self.send_telegram_message(
                                    f"✅ Código <code>{code}</code> recibido.\n\n"
                                    "Intentando login..."
                                )
                                
                                return code
                
                time.sleep(2)  # Esperar antes de siguiente poll
                
            except Exception as e:
                logger.debug(f"Error en polling: {e}")
                time.sleep(2)
        
        logger.error("Timeout esperando código MFA")
        self.send_telegram_message("⏱️ Timeout - No se recibió código a tiempo")
        
        return None
    
    def login_with_telegram_mfa(self, email: str, password: str) -> bool:
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        try:
            if not self.driver:
                self.setup_driver()

            wait = WebDriverWait(self.driver, 40)

            logger.info("Navegando a Cocos...")
            self.driver.get("https://app.cocos.capital/login")

            # ===== LOGIN =====
            email_field = wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "input[type='email'], input[name='email']")
                )
            )
            email_field.clear()
            email_field.send_keys(email)

            password_field = wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "input[type='password']")
                )
            )
            password_field.clear()
            password_field.send_keys(password)

            login_button = wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button[type='submit'], button")
                )
            )
            login_button.click()
            logger.info("✓ Credenciales enviadas")

            # ===== ESPERAR CAMBIO DE ESTADO =====
            wait.until(
                EC.invisibility_of_element_located(
                    (By.CSS_SELECTOR, "input[type='password']")
                )
            )

            logger.info("Pantalla cambió después del submit")

            # ===== TRUSTED DEVICE =====
            if "capital-portfolio" in self.driver.current_url:
                logger.info("[OK] Login exitoso sin MFA")
                self.logged_in = True
                return True

            logger.info("MFA requerido")

            if not self.telegram_enabled:
                logger.error("Telegram no configurado")
                return False

            # ===== PEDIR CÓDIGO TELEGRAM =====
            mfa_code = self.get_telegram_mfa_code(timeout=120)
            if not mfa_code:
                return False

            # ===== FUNCIÓN PARA BUSCAR INPUTS =====
            def find_mfa_inputs():
                return self.driver.find_elements(
                    By.CSS_SELECTOR,
                    "input[type='tel'], input[inputmode='numeric'], input[autocomplete='one-time-code'], input"
                )

            # Volver al DOM principal
            self.driver.switch_to.default_content()

            # 1️⃣ Buscar en DOM principal
            mfa_inputs = find_mfa_inputs()

            # 2️⃣ Si no hay, buscar en todos los iframes
            if not mfa_inputs:
                logger.info("No encontrados en DOM principal, buscando en iframes...")
                iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
                logger.info(f"Iframes encontrados: {len(iframes)}")

                for i, frame in enumerate(iframes):
                    try:
                        self.driver.switch_to.default_content()
                        self.driver.switch_to.frame(frame)

                        mfa_inputs = find_mfa_inputs()

                        if mfa_inputs:
                            logger.info(f"Inputs MFA encontrados en iframe {i}")
                            break
                    except Exception:
                        continue

            # 3️⃣ Validación final
            if not mfa_inputs:
                logger.error("No se encontraron inputs MFA en ningún contexto")
                return False

            logger.info(f"Inputs MFA encontrados: {len(mfa_inputs)}")

            # ===== INGRESAR CÓDIGO =====
            if len(mfa_inputs) == 1:
                mfa_inputs[0].click()
                mfa_inputs[0].clear()
                mfa_inputs[0].send_keys(mfa_code)

            elif len(mfa_inputs) >= 6:
                for i, digit in enumerate(mfa_code[:6]):
                    mfa_inputs[i].click()
                    mfa_inputs[i].send_keys(digit)

            else:
                logger.error("Estructura de MFA no reconocida")
                return False

            # ===== SUBMIT MFA =====
            submit_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit'], button"))
            )
            submit_button.click()

            # ===== CONFIRMAR LOGIN =====
            wait.until(lambda d: "capital-portfolio" in d.current_url)

            logger.info("[OK] Login exitoso con MFA")
            self.logged_in = True

            if self.telegram_enabled:
                self.send_telegram_message("✅ Login exitoso con MFA")

            return True

        except Exception as e:
            logger.error(f"Error en login_with_telegram_mfa: {e}", exc_info=True)

            if self.telegram_enabled:
                self.send_telegram_message(f"❌ Error en login: {e}")

            return False


    
    def scrape_portfolio(self) -> Optional[Dict]:
        """
        Extrae portfolio - VERSIÓN FIXED para líneas separadas
        """
        if not self.logged_in:
            logger.error("[ERROR] Debes hacer login primero")
            return None
        
        try:
            logger.info("Navegando al portfolio...")
            self.driver.get("https://app.cocos.capital/capital-portfolio")
            time.sleep(4)
            
            portfolio = {
                'timestamp': datetime.now().isoformat(),
                'ValorTotal': 0,
                'Posiciones': [],
                'Moneda': 'ARS'
            }
            
            # VALOR TOTAL
            try:
                page_text = self.driver.find_element(By.TAG_NAME, 'body').text
                header_section = page_text.split('Tenencia valorizada')[0] if 'Tenencia valorizada' in page_text else page_text[:500]
                total_matches = re.findall(r'\$\s*([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]{2})', header_section)
                
                if total_matches:
                    valores_header = []
                    for match in total_matches:
                        valor_str = match.replace('.', '').replace(',', '.')
                        try:
                            val = float(valor_str)
                            valores_header.append(val)
                        except:
                            continue
                    
                    if valores_header:
                        portfolio['ValorTotal'] = max(valores_header)
                        logger.info(f"[OK] Valor total: ${portfolio['ValorTotal']:,.2f}")
                
            except Exception as e:
                logger.debug(f"Error extrayendo valor total: {e}")
            
            # INSTRUMENTOS - Estrategia: buscar datos RAW y reconstruir
            logger.info("Extrayendo instrumentos...")
            
            try:
                full_text = self.driver.find_element(By.TAG_NAME, 'body').text
                
                # Buscar la data RAW completa (viene en el JSON del debug)
                # Está entre "Cedears" y el final
                if 'CVX' in full_text:
                    # Método 1: Extraer directamente con RegEx todo junto
                    # Patrón: CVX\nChevron\n23\n$16.730,00\n$384.790,00\n43,52%
                    
                    pattern = r'(CVX|GOOGL|TSLA|NVDA|AAPL|MSFT)\n([A-Za-z]+)\n(\d+)\n\$\s*([0-9.,]+)\n\$\s*([0-9.,]+)\n([0-9]+,[0-9]+)%'
                    
                    matches = re.findall(pattern, full_text)
                    
                    logger.info(f"Encontrados {len(matches)} instrumentos con RegEx")
                    
                    for match in matches:
                        ticker, nombre, cantidad, precio_str, valuacion_str, pnl_str = match
                        
                        # Convertir valores
                        cantidad = int(cantidad)
                        precio = float(precio_str.replace('.', '').replace(',', '.'))
                        valuacion = float(valuacion_str.replace('.', '').replace(',', '.'))
                        pnl_percent = float(pnl_str.replace(',', '.'))
                        
                        posicion = {
                            'Ticker': ticker,
                            'Nombre': nombre,
                            'Cantidad': cantidad,
                            'PrecioActual': precio,
                            'Valuacion': valuacion,
                            'GananciaPorcentaje': pnl_percent,
                            'Moneda': 'USD'
                        }
                        
                        portfolio['Posiciones'].append(posicion)
                        logger.info(f"  {ticker} ({nombre}): {cantidad} @ ${precio:,.2f} = ${valuacion:,.2f} ({pnl_percent:+.2f}%)")
                
                # Método 2: Si el RegEx no funciona, buscar manualmente
                if len(portfolio['Posiciones']) == 0:
                    logger.info("Método alternativo: búsqueda manual...")
                    
                    lines = full_text.split('\n')
                    
                    # Buscar índices de tickers conocidos
                    tickers = ['CVX', 'GOOGL', 'TSLA', 'NVDA', 'AAPL', 'MSFT']
                    
                    for i, line in enumerate(lines):
                        if line.strip() in tickers:
                            ticker = line.strip()
                            
                            try:
                                # Las siguientes líneas tienen: nombre, cantidad, precio, valuación
                                nombre = lines[i+1].strip() if i+1 < len(lines) else ''
                                cantidad_str = lines[i+2].strip() if i+2 < len(lines) else '0'
                                precio_str = lines[i+3].strip() if i+3 < len(lines) else '$0'
                                valuacion_str = lines[i+4].strip() if i+4 < len(lines) else '$0'
                                
                                # Buscar el porcentaje en las siguientes líneas
                                pnl_percent = 0
                                for j in range(i+5, min(i+15, len(lines))):
                                    if '%' in lines[j] and ',' in lines[j]:
                                        pnl_match = re.search(r'([+-]?\d+,\d+)%', lines[j])
                                        if pnl_match:
                                            pnl_percent = float(pnl_match.group(1).replace(',', '.'))
                                            break
                                
                                # Convertir
                                cantidad = int(cantidad_str) if cantidad_str.isdigit() else 0
                                precio = float(precio_str.replace('$','').strip().replace('.','').replace(',','.'))
                                valuacion = float(valuacion_str.replace('$','').strip().replace('.','').replace(',','.'))
                                
                                if valuacion > 0:
                                    posicion = {
                                        'Ticker': ticker,
                                        'Nombre': nombre,
                                        'Cantidad': cantidad,
                                        'PrecioActual': precio,
                                        'Valuacion': valuacion,
                                        'GananciaPorcentaje': pnl_percent,
                                        'Moneda': 'USD'
                                    }
                                    
                                    portfolio['Posiciones'].append(posicion)
                                    logger.info(f"  {ticker} ({nombre}): {cantidad} @ ${precio:,.2f} = ${valuacion:,.2f}")
                            
                            except Exception as e:
                                logger.debug(f"Error procesando {ticker}: {e}")
                                continue
                
            except Exception as e:
                logger.error(f"Error extrayendo instrumentos: {e}")
                import traceback
                traceback.print_exc()
            
            # VALIDACIÓN FINAL
            if portfolio['ValorTotal'] == 0 and portfolio['Posiciones']:
                portfolio['ValorTotal'] = sum(p['Valuacion'] for p in portfolio['Posiciones'])
                logger.info(f"[OK] Valor total calculado: ${portfolio['ValorTotal']:,.2f}")
            
            if portfolio['ValorTotal'] > 0 or len(portfolio['Posiciones']) > 0:
                logger.info(f"\n[OK] Portfolio scrapeado:")
                logger.info(f"  Valor Total: ${portfolio['ValorTotal']:,.2f}")
                logger.info(f"  Instrumentos: {len(portfolio['Posiciones'])}")
                return portfolio
            else:
                logger.warning("[WARNING] No se extrajeron datos")
                
                screenshot_path = f"debug_screenshot_{int(time.time())}.png"
                self.driver.save_screenshot(screenshot_path)
                logger.info(f"Screenshot: {screenshot_path}")
                
                with open(f"debug_html_{int(time.time())}.html", 'w', encoding='utf-8') as f:
                    f.write(self.driver.page_source)
                
                return portfolio
                
        except Exception as e:
            logger.error(f"[ERROR] {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def inspect_dom(self) -> Dict:
        """
        Inspecciona el DOM para ayudar a identificar selectores
        Útil para debugging
        
        Returns:
            Dict con información del DOM
        """
        try:
            info = {
                'url': self.driver.current_url,
                'title': self.driver.title,
                'page_source_length': len(self.driver.page_source),
                'all_text': self.driver.find_element(By.TAG_NAME, 'body').text[:500],
                'class_names': [],
                'ids': [],
                'data_testids': []
            }
            
            # Recolectar clases
            elements_with_class = self.driver.find_elements(By.CSS_SELECTOR, '[class]')
            for elem in elements_with_class[:50]:
                classes = elem.get_attribute('class')
                if classes:
                    info['class_names'].extend(classes.split())
            
            # Recolectar IDs
            elements_with_id = self.driver.find_elements(By.CSS_SELECTOR, '[id]')
            for elem in elements_with_id[:50]:
                elem_id = elem.get_attribute('id')
                if elem_id:
                    info['ids'].append(elem_id)
            
            # Recolectar data-testid
            elements_with_testid = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid]')
            for elem in elements_with_testid:
                testid = elem.get_attribute('data-testid')
                if testid:
                    info['data_testids'].append(testid)
            
            # Deduplicar
            info['class_names'] = list(set(info['class_names']))[:20]
            info['ids'] = list(set(info['ids']))
            info['data_testids'] = list(set(info['data_testids']))
            
            return info
            
        except Exception as e:
            logger.error(f"Error inspeccionando DOM: {e}")
            return {}
    
    def keep_open(self):
        """Mantiene el navegador abierto"""
        print("\n[INFO] Navegador abierto - presiona Enter para cerrar")
        input()
    
    def close(self):
        """Cierra el navegador"""
        if self.driver:
            self.driver.quit()
            logger.info("[OK] Navegador cerrado")


# ====================
# EJEMPLO DE USO
# ====================
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    import json
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    load_dotenv()
    
    print("=" * 60)
    print("COCOS SCRAPER - Extracción desde DOM")
    print("=" * 60)
    print("\nEsta solución:")
    print("✓ NO necesita TOTP seed")
    print("✓ NO usa API OAuth")
    print("✓ Funciona con trusted device")
    print("✓ Extrae datos del navegador directamente")
    print("=" * 60)
    
    email = os.getenv('COCOS_EMAIL')
    password = os.getenv('COCOS_PASSWORD')
    
    scraper = CocosScraper(headless=False)
    
    try:
        # Login manual
        if scraper.login_manual(email, password):
            print("\n✅ Login exitoso!\n")
            
            # Inspeccionar DOM (útil para debugging)
            print("Inspeccionando estructura del DOM...")
            dom_info = scraper.inspect_dom()
            
            print("\n📋 Información del DOM:")
            print(f"URL: {dom_info.get('url')}")
            print(f"Title: {dom_info.get('title')}")
            print(f"\nPrimeras clases CSS: {dom_info.get('class_names', [])[:5]}")
            print(f"IDs encontrados: {dom_info.get('ids', [])[:5]}")
            print(f"Data-testids: {dom_info.get('data_testids', [])[:5]}")
            
            print(f"\nTexto visible (primeros 200 chars):")
            print(dom_info.get('all_text', '')[:200])
            
            # Scrapear portfolio
            print("\n\nScrapeando portfolio...")
            portfolio = scraper.scrape_portfolio()
            
            if portfolio:
                print("\n" + "=" * 60)
                print("📊 PORTFOLIO SCRAPEADO")
                print("=" * 60)
                print(json.dumps(portfolio, indent=2, ensure_ascii=False))
                print("=" * 60)
                
                if portfolio['ValorTotal'] == 0 and len(portfolio['Posiciones']) == 0:
                    print("\n⚠️  No se pudieron extraer datos automáticamente")
                    print("\nPara ayudarte a arreglar esto:")
                    print("1. Con el navegador abierto, usa F12 → Elements")
                    print("2. Busca el elemento que muestra el valor total")
                    print("3. Click derecho → Copy → Copy selector")
                    print("4. Pégame ese selector y lo agrego al código")
                    
                    scraper.keep_open()
            else:
                print("\n❌ Error scrapeando portfolio")
        else:
            print("\n❌ Login falló")
    
    except KeyboardInterrupt:
        print("\n\nCancelado")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        scraper.close()
