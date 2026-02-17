# scripts/get_telegram_chat_id.py
"""
Script para obtener tu Chat ID de Telegram
"""
import requests
import sys

def get_chat_id(bot_token):
    """Obtiene el chat_id del último mensaje"""
    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    
    print("\n" + "="*60)
    print("OBTENIENDO CHAT ID")
    print("="*60)
    print("\n1. Envía un mensaje a tu bot en Telegram")
    print("2. Presiona Enter aquí\n")
    
    input("Presiona Enter después de enviar mensaje al bot... ")
    
    response = requests.get(url)
    data = response.json()
    
    if data['ok'] and data['result']:
        chat_id = data['result'][-1]['message']['chat']['id']
        print(f"\n✅ Tu Chat ID es: {chat_id}")
        print(f"\nAgregalo a tu .env:")
        print(f"TELEGRAM_CHAT_ID={chat_id}")
        return chat_id
    else:
        print("\n❌ No se encontraron mensajes")
        print("Asegúrate de haber enviado un mensaje al bot primero")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python get_telegram_chat_id.py <BOT_TOKEN>")
        sys.exit(1)
    
    bot_token = sys.argv[1]
    get_chat_id(bot_token)