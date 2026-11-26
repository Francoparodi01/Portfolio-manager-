// Executed by Playwright CLI in an already authenticated, user-visible session.
// Public Cocos web client 2026-10-16: movements service module 3360,
// response/detail schemas 3363. Only account reads and ticket downloads.
async (page) => {
  const cfg = /* AUDIT_CONFIG */ {};
  const result = {schema_version: 'cocos-activity-capture-v1', started_at: new Date().toISOString(),
    status: 'PARTIAL', list_complete: false, details_complete: false, tickets_complete: false,
    account_verification: 'NOT_VERIFIED', pages: [], details: [], tickets: [], errors: []};
  const finish = (status, reason) => {
    result.status = status;
    if (reason) result.errors.push(reason);
    result.finished_at = new Date().toISOString();
    return result;
  };
  if (!page.url().startsWith('https://app.cocos.capital/') || page.url().includes('/sign-in')) {
    return finish('SOURCE_BLOCKED', 'AUTHENTICATED_BROWSER_REQUIRED');
  }
  const body = await page.locator('body').innerText();
  const active = body.match(/Cuenta\s*N[°ºo]?\s*(\d+)/i);
  if (!active || active[1] !== cfg.expected_account) {
    return finish('SOURCE_BLOCKED', 'ACTIVE_ACCOUNT_NOT_VERIFIED');
  }
  result.account_verification = 'ACTIVE_UI_ACCOUNT_MATCH';
  result.account = cfg.expected_account; // Private artifact only.
  let request, firstResponse;
  try {
    const isList = r => {
      const u = new URL(r.url());
      return u.origin === 'https://api.cocos.capital' && u.pathname === '/api/movements' && r.method() === 'GET';
    };
    const reqPromise = page.waitForRequest(isList, {timeout: 30000});
    const responsePromise = page.waitForResponse(r => isList(r.request()), {timeout: 30000});
    // Observe the same request that the Activity UI uses; credentials stay in memory.
    const [pair] = await Promise.all([Promise.all([reqPromise, responsePromise]),
      page.goto('https://app.cocos.capital/activity', {waitUntil: 'domcontentloaded'})]);
    [request, firstResponse] = pair;
  } catch (_) {
    return finish('SOURCE_BLOCKED', 'ACTIVITY_RESPONSE_UNAVAILABLE');
  }
  if (firstResponse.status() !== 200) {
    return finish('SOURCE_BLOCKED', 'ACTIVITY_HTTP_' + firstResponse.status());
  }
  const forbidden = new Set(['host', 'cookie', 'origin', 'referer', 'user-agent', 'content-length',
    'accept-encoding', 'connection', 'priority', 'if-none-match', 'if-modified-since']);
  const headers = Object.fromEntries(Object.entries(await request.allHeaders())
    .filter(([key]) => !forbidden.has(key.toLowerCase()) && !key.toLowerCase().startsWith('sec-') && !key.startsWith(':')));
  if (headers['x-account-id'] && String(headers['x-account-id']) !== cfg.expected_account) {
    return finish('SOURCE_BLOCKED', 'REQUEST_ACCOUNT_MISMATCH');
  }
  const allowed = new Set(['CASH_MOVEMENT', 'CASH_IN', 'CASH_OUT', 'ORDER', 'ORDER_MEP',
    'SETTLEMENT_EXT_ESP', 'SETTLEMENT', 'RIGHT_AND_COMMISSION_FOR_CROSS_CURRENCY', 'FX']);
  const read = async (url, binary = false) => {
    const u = new URL(url);
    if (u.origin !== 'https://api.cocos.capital' || !/^\/api\/(movements(?:$|\?|\/details\/)|v1\/wallet\/tickets\/[0-9]+\/download$)/.test(u.pathname)) {
      throw new Error('Read URL outside audit allowlist');
    }
    return await page.evaluate(async ({url, headers, binary}) => {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 20000);
      try {
        // The observed web client authenticates with a bearer/account header.
        // The API does not grant Access-Control-Allow-Credentials for cookies.
        const response = await fetch(url, {method: 'GET', credentials: 'omit', headers, signal: controller.signal});
        const item = {url, observed_at: new Date().toISOString(), status: response.status,
          content_type: response.headers.get('content-type')};
        if (binary && response.ok) {
          const bytes = new Uint8Array(await response.arrayBuffer());
          if (bytes.length > 15000000) throw new Error('Ticket exceeds capture bound');
          let encoded = '';
          for (let i = 0; i < bytes.length; i += 32768) encoded += String.fromCharCode(...bytes.subarray(i, i + 32768));
          item.base64 = btoa(encoded);
        } else item.body = await response.text(); // Preserve the original decimal text.
        return item;
      } finally { clearTimeout(timeout); }
    }, {url, headers, binary});
  };
  const identities = new Map(), ticketIds = new Set();
  for (const [identifier, id] of cfg.identities || []) {
    if (!allowed.has(identifier) || !id) return finish('SOURCE_BLOCKED', 'INVALID_DETAIL_BATCH');
    identities.set(JSON.stringify([identifier, String(id)]), [identifier, String(id)]);
  }
  for (const id of cfg.ticket_ids || []) {
    if (!/^[1-9][0-9]*$/.test(String(id))) return finish('SOURCE_BLOCKED', 'INVALID_TICKET_BATCH');
    ticketIds.add(String(id));
  }
  let offset = 0;
  try {
    for (let p = 0; (!cfg.stage || cfg.stage === 'list') && p < cfg.max_pages; p++) {
      const url = `https://api.cocos.capital/api/movements?sort_by=execution_date&offset=${offset}&limit=100`;
      const response = await read(url);
      result.pages.push(response);
      if (response.status !== 200) return finish('SOURCE_BLOCKED', 'LIST_HTTP_' + response.status);
      let data;
      try { data = JSON.parse(response.body); } catch (_) { return finish('PARTIAL', 'LIST_NOT_JSON'); }
      if (!data.pagination || data.pagination.offset !== offset || !Number.isSafeInteger(data.pagination.limit) ||
          data.pagination.limit <= 0 || typeof data.pagination.lastPage !== 'boolean' || !Array.isArray(data.data)) {
        return finish('PARTIAL', 'PAGINATION_SCHEMA_NOT_VERIFIED');
      }
      for (const group of data.data) {
        if (!Array.isArray(group.movements)) return finish('PARTIAL', 'MOVEMENT_SCHEMA_NOT_VERIFIED');
        for (const movement of group.movements) {
          const id = movement.identifierId;
          if (!allowed.has(movement.identifier) || id === null || id === undefined || id === '' ||
              (typeof id === 'number' && !Number.isSafeInteger(id))) {
            result.errors.push('MOVEMENT_WITHOUT_SAFE_DETAIL_ID');
            continue;
          }
          identities.set(JSON.stringify([movement.identifier, String(id)]), [movement.identifier, String(id)]);
          if (Number.isSafeInteger(movement.idTicket) && movement.idTicket > 0) ticketIds.add(String(movement.idTicket));
        }
      }
      if (data.pagination.lastPage) { result.list_complete = true; break; }
      offset += data.pagination.limit;
    }
    if (!cfg.stage || cfg.stage === 'list') {
      if (!result.list_complete) result.errors.push('PAGE_LIMIT_REACHED');
      if (cfg.stage === 'list') return finish(result.list_complete && !result.errors.length ? 'BATCH_CAPTURED' : 'PARTIAL');
    }
    let n = 0;
    for (const [identifier, id] of identities.values()) {
      if (n++ >= cfg.max_details) { result.errors.push('DETAIL_LIMIT_REACHED'); break; }
      const response = await read(`https://api.cocos.capital/api/movements/details/${identifier}/${encodeURIComponent(id)}`);
      result.details.push({identifier, identifier_id: id, ...response});
      if (response.status === 401 || response.status === 403 || response.status === 429) {
        return finish('SOURCE_BLOCKED', 'DETAIL_HTTP_' + response.status);
      }
      if (response.status !== 200) { result.errors.push('DETAIL_UNAVAILABLE'); continue; }
      const value = JSON.parse(response.body).data;
      if (!value || typeof value !== 'object') { result.errors.push('DETAIL_SCHEMA_NOT_VERIFIED'); continue; }
      if (value.idAccount !== undefined && String(value.idAccount) !== cfg.expected_account) {
        return finish('SOURCE_BLOCKED', 'DETAIL_ACCOUNT_MISMATCH');
      }
      if (Number.isSafeInteger(value.idTicket) && value.idTicket > 0) ticketIds.add(String(value.idTicket));
      await page.waitForTimeout(100);
    }
    result.details_complete = result.details.length === identities.size && result.details.every(r => r.status === 200) &&
      !result.errors.some(e => e.includes('SCHEMA') || e.includes('DETAIL_ID'));
    if (cfg.download_tickets && cfg.stage !== 'details') {
      for (const id of ticketIds) {
        const response = await read(`https://api.cocos.capital/api/v1/wallet/tickets/${id}/download`, true);
        result.tickets.push({ticket_id: id, ...response});
        if (response.status === 401 || response.status === 403 || response.status === 429) {
          return finish('SOURCE_BLOCKED', 'TICKET_HTTP_' + response.status);
        }
        if (response.status !== 200) result.errors.push('TICKET_UNAVAILABLE');
        await page.waitForTimeout(100);
      }
      result.tickets_complete = result.tickets.length === ticketIds.size && result.tickets.every(r => r.status === 200);
    }
    if (result.pages.length) {
      const check = await read(result.pages[0].url);
      result.first_page_recheck = check;
      result.first_page_stable = check.status === 200 && check.body === result.pages[0].body;
      if (!result.first_page_stable) result.errors.push('FIRST_PAGE_CHANGED_DURING_CAPTURE');
    }
    if (cfg.stage === 'check') {
      result.first_page_recheck = await read('https://api.cocos.capital/api/movements?sort_by=execution_date&offset=0&limit=100');
      if (result.first_page_recheck.status !== 200) result.errors.push('RECHECK_UNAVAILABLE');
    }
    if (cfg.stage) return finish(!result.errors.length ? 'BATCH_CAPTURED' : 'PARTIAL');
    return finish(result.list_complete && result.details_complete && result.tickets_complete && !result.errors.length ? 'CAPTURED' : 'PARTIAL');
  } catch (error) {
    let diagnostic = String(error.message || error.name);
    for (const value of Object.values(headers)) {
      if (typeof value === 'string' && value.length > 6) diagnostic = diagnostic.split(value).join('[REDACTED]');
    }
    result.diagnostic = diagnostic.slice(0, 600); // Private capture, not published in summary.
    return finish('PARTIAL', 'READ_FAILED_' + error.name); // Do not leak headers/tokens.
  }
}
