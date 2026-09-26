import asyncio, hashlib, json, logging, zipfile
from pathlib import Path
from types import SimpleNamespace
from scripts.telegram_bot import action_analytics_v2, audit_keyboard, CALLBACK_ALIASES
from src.core.config import get_config
from src.core.telegram_format import validate_telegram_html
logging.disable(logging.CRITICAL)
owner = int(get_config().scraper.telegram_chat_id)
class LocalSink:
    def __init__(self):
        self.messages = []; self.documents = []; self.temp_paths = []
    async def send_message(self, **kwargs):
        assert kwargs['chat_id'] == owner
        assert validate_telegram_html(kwargs['text'])[0]
        self.messages.append(kwargs['text'])
    async def send_document(self, **kwargs):
        assert kwargs['chat_id'] == owner
        data = kwargs['document'].read()
        self.temp_paths.append(Path(kwargs['document'].name))
        target = Path('/verification/handler-verified.zip')
        target.write_bytes(data)
        with zipfile.ZipFile(target) as z:
            manifest = json.loads(z.read('manifest.json'))
            for name, expected in manifest['output_hashes'].items():
                assert hashlib.sha256(z.read(name)).hexdigest() == expected
        self.documents.append(kwargs['filename'])
sink=LocalSink()
asyncio.run(action_analytics_v2(SimpleNamespace(bot=sink), owner))
assert len(sink.messages)==1 and len(sink.documents)==1
assert all(not p.parent.exists() for p in sink.temp_paths)
assert any(b.callback_data == 'analytics_v2' for row in audit_keyboard().inline_keyboard for b in row)
assert CALLBACK_ALIASES['analytics_v2'] == 'analytics_v2'
print(json.dumps({'handler_live_db':'PASS','html':'VALID','attachment_hashes':'VALID','temp_cleanup':'PASS','outbound_telegram_messages':0,'documents':sink.documents}))
