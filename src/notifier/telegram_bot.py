from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
import asyncio

class TelegramMFAHandler:

    def __init__(self, token):
        self.token = token
        self._code_future = None

    async def _handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if self._code_future and not self._code_future.done():
            self._code_future.set_result(update.message.text)

    async def wait_for_code(self):
        self._code_future = asyncio.get_event_loop().create_future()

        app = ApplicationBuilder().token(self.token).build()
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), self._handle_message))

        await app.initialize()
        await app.start()

        print("Esperando código MFA por Telegram...")
        code = await self._code_future

        await app.stop()
        return code
