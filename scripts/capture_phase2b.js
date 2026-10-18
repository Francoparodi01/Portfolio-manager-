// Read-only Cocos routes observed in the authenticated UI on 2026-10-17.
async (page) => {
  const cfg = /* AUDIT_CONFIG */ {};
  const result = {schema_version: 'phase2b-browser-batch-v1', started_at: new Date().toISOString(),
    account: cfg.expected_account, account_verification: 'NOT_VERIFIED', records: [], errors: []};
  const finish = status => ({...result, status, finished_at: new Date().toISOString()});
  if (!page.url().startsWith('https://app.cocos.capital/')) return finish('SOURCE_BLOCKED');
  const account = (await page.locator('body').innerText()).match(/Cuenta\s*N[°ºo]?\s*(\d+)/i);
  if (!account || account[1] !== cfg.expected_account) return finish('SOURCE_BLOCKED');
  result.account_verification = 'ACTIVE_UI_ACCOUNT_MATCH';
  const observed = page.waitForRequest(r => r.method() === 'GET' &&
    new URL(r.url()).origin === 'https://api.cocos.capital' &&
    new URL(r.url()).pathname === '/api/v2/portfolio', {timeout: 30000});
  await page.goto('https://app.cocos.capital/portfolio?tab=value', {waitUntil: 'domcontentloaded'});
  const request = await observed;
  const forbidden = new Set(['host','cookie','origin','referer','user-agent','content-length',
    'accept-encoding','connection','priority','if-none-match','if-modified-since']);
  const headers = Object.fromEntries(Object.entries(await request.allHeaders()).filter(([k]) =>
    !forbidden.has(k.toLowerCase()) && !k.toLowerCase().startsWith('sec-') && !k.startsWith(':')));
  if (headers['x-account-id'] && headers['x-account-id'] !== cfg.expected_account) return finish('SOURCE_BLOCKED');
  const allowed = url => {
    const u = new URL(url);
    if (u.origin !== 'https://api.cocos.capital') return false;
    const p = u.pathname, q = u.searchParams;
    if (p === '/api/orders') return !u.search;
    if (p === '/api/v2/portfolio') return u.search === '?period=MAX&domain=BROKER&include=costBasis,allocation,cash';
    if (p === '/api/portfolio/history') return ['ARS','USD'].includes(q.get('currency')) &&
      q.get('period') === 'MAX' && q.get('metric') === 'balance' && [...q.keys()].length === 3;
    if (p === '/api/portfolio/balance') return ['ARS','USD'].includes(q.get('currency')) &&
      q.get('period') === 'MAX' && [...q.keys()].length === 2;
    if (p === '/api/portfolio/reports/holdings') return ['CSV','PDF'].includes(q.get('type')) &&
      /^\d{8}$/.test(q.get('date')) && [...q.keys()].length === 2;
    return /^\/api\/v1\/wallet\/tickets\/[1-9][0-9]*\/download$/.test(p) && !u.search;
  };
  if (!Array.isArray(cfg.requests) || cfg.requests.length > 8 || cfg.requests.some(r => !allowed(r.url))) {
    result.errors.push('REQUEST_OUTSIDE_ALLOWLIST'); return finish('SOURCE_BLOCKED');
  }
  for (const item of cfg.requests) {
    try {
      const response = await page.evaluate(async ({url, headers, binary}) => {
        const controller = new AbortController(), timer = setTimeout(() => controller.abort(), 30000);
        try {
          const r = await fetch(url, {method: 'GET', credentials: 'omit', headers, signal: controller.signal});
          const out = {url, observed_at: new Date().toISOString(), status: r.status,
            content_type: r.headers.get('content-type')};
          if (r.ok && binary) {
            const bytes = new Uint8Array(await r.arrayBuffer());
            if (bytes.length > 15000000) throw new Error('Response size bound exceeded');
            let text = '';
            for (let i=0; i<bytes.length; i+=32768) text += String.fromCharCode(...bytes.subarray(i,i+32768));
            out.base64 = btoa(text);
          } else out.body = await r.text();
          return out;
        } finally { clearTimeout(timer); }
      }, {url: item.url, headers, binary: item.binary});
      result.records.push({key: item.key, ...response});
      if ([401,403,429].includes(response.status)) return finish('SOURCE_BLOCKED');
      if (response.status !== 200) result.errors.push('HTTP_'+response.status);
      await page.waitForTimeout(150);
    } catch (_) { result.errors.push('READ_FAILED'); return finish('PARTIAL'); }
  }
  return finish(result.errors.length ? 'PARTIAL' : 'CAPTURED');
}
