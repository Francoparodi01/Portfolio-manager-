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


async def prospective_view(request):
    from datetime import datetime
    from src.analysis.qeef.monitor import report,markdown
    try:
        owner=int(os.environ.get('TELEGRAM_CHAT_ID','0'))
        requested=int(request.query.get('owner_chat_id',str(owner)))
    except ValueError:
        raise web.HTTPBadRequest(reason='invalid owner')
    if not owner: raise web.HTTPServiceUnavailable(reason='configured owner required')
    if requested!=owner: raise web.HTTPForbidden(reason='owner mismatch')
    start=os.environ.get('QEEF_CAPTURE_START_AT')
    async with request.app['pool'].acquire() as conn:
        payload=await report(conn,owner=owner,start=datetime.fromisoformat(start) if start else None)
    if request.query.get('format')=='markdown':
        return web.Response(text=markdown(payload),content_type='text/markdown')
    return web.json_response(payload)
