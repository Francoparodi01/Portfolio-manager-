"""Optional read-only view under the existing monitor authentication middleware."""
import os
from aiohttp import web
from src.analysis.qeef.store import latest_evaluation


async def qeef_view(request):
    # API token protects one configured account. Never allow a query parameter
    # to select another owner merely because it exists in the database.
    configured = os.environ.get('TELEGRAM_CHAT_ID')
    try:
        owner = int(configured or '0')
        requested = int(request.query.get('owner_chat_id', str(owner)))
    except ValueError:
        raise web.HTTPBadRequest(reason='invalid owner')
    if not owner:
        raise web.HTTPServiceUnavailable(reason='configured owner required')
    if requested != owner:
        raise web.HTTPForbidden(reason='owner mismatch')
    async with request.app['pool'].acquire() as conn:
        payload = await latest_evaluation(conn, owner=owner)
    return web.json_response(payload)
