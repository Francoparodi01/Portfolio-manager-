// Browser-independent protocol checks; no network and no authentication data.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const code = fs.readFileSync(path.join(__dirname, '../scripts/capture_cocos_activity.js'), 'utf8');
const makeExecutor = extra => eval('(' + code.replace('/* AUDIT_CONFIG */ {}', JSON.stringify({
  expected_account: '9', max_pages: 3, max_details: 10, download_tickets: true, ...extra
})) + ')');
const execute = makeExecutor({});
const list = JSON.stringify({pagination: {offset: 0, limit: 100, lastPage: true},
  data: [{date: '2026-10-09', movements: [{identifier: 'SETTLEMENT', identifierId: 77, idTicket: 77}]}]});
function browser(mode) {
  const requests = [];
  const request = {url: () => 'https://api.cocos.capital/api/movements?offset=0&limit=30',
    method: () => 'GET', allHeaders: async () => ({authorization: 'FIXTURE_ONLY', cookie: 'MUST_NOT_FORWARD',
      ':authority': 'MUST_NOT_FORWARD', 'if-none-match': 'MUST_NOT_FORWARD'})};
  return {requests, url: () => mode === 'signin' ? 'https://app.cocos.capital/sign-in' : 'https://app.cocos.capital/activity',
    locator: () => ({innerText: async () => mode === 'wrong-account' ? 'Cuenta N° 8' : 'Cuenta N° 9'}),
    waitForRequest: async predicate => { assert(predicate(request)); return request; },
    waitForResponse: async predicate => { const response = {request: () => request, status: () => 200}; assert(predicate(response)); return response; },
    goto: async () => {}, waitForTimeout: async () => {},
    evaluate: async (fn, args) => {
      requests.push(args.url);
      assert.equal(args.headers.cookie, undefined);
      assert.equal(args.headers[':authority'], undefined);
      assert.equal(args.headers['if-none-match'], undefined);
      assert.equal(args.headers.authorization, 'FIXTURE_ONLY');
      const savedFetch = global.fetch;
      global.fetch = async (url, options) => {
        assert.equal(options.method, 'GET');
        assert.equal(options.credentials, 'omit');
        const status = mode === 'denied' ? 403 : 200;
        const body = status === 403 ? 'Access denied' : url.includes('/details/') ?
          JSON.stringify({data: {idAccount: mode === 'detail-account-conflict' ? 8 : 9, idTicket: 77}}) : list;
        return {status, ok: status === 200, headers: new Map([['content-type', args.binary ? 'application/pdf' : 'application/json']]),
          text: async () => body, arrayBuffer: async () => new TextEncoder().encode('%PDF-1.7 fixture').buffer};
      };
      try { return await fn(args); } finally { global.fetch = savedFetch; }
    }};
}
(async () => {
  for (const mode of ['signin', 'wrong-account']) {
    const page = browser(mode), result = await execute(page);
    assert.equal(result.status, 'SOURCE_BLOCKED'); assert.equal(page.requests.length, 0);
  }
  const denied = browser('denied'), blocked = await execute(denied);
  assert.equal(blocked.status, 'SOURCE_BLOCKED'); assert.equal(denied.requests.length, 1);
  assert.equal(blocked.details.length, 0);
  const conflicting = await execute(browser('detail-account-conflict'));
  assert.equal(conflicting.status, 'SOURCE_BLOCKED'); assert(conflicting.errors.includes('DETAIL_ACCOUNT_MISMATCH'));
  const complete = await execute(browser('complete'));
  assert.equal(complete.status, 'CAPTURED'); assert.equal(complete.pages.length, 1);
  assert.equal(complete.details.length, 1); assert.equal(complete.tickets.length, 1);
  assert.equal(complete.first_page_stable, true); assert(!JSON.stringify(complete).includes('FIXTURE_ONLY'));
  const listOnly = await makeExecutor({stage: 'list'})(browser('complete'));
  assert.equal(listOnly.status, 'BATCH_CAPTURED'); assert.equal(listOnly.pages.length, 1); assert.equal(listOnly.details.length, 0);
  const detailOnly = await makeExecutor({stage: 'details', identities: [['SETTLEMENT', '77']]})(browser('complete'));
  assert.equal(detailOnly.status, 'BATCH_CAPTURED'); assert.equal(detailOnly.details.length, 1); assert.equal(detailOnly.tickets.length, 0);
  const ticketOnly = await makeExecutor({stage: 'tickets', ticket_ids: ['77']})(browser('complete'));
  assert.equal(ticketOnly.status, 'BATCH_CAPTURED'); assert.equal(ticketOnly.tickets.length, 1); assert.equal(ticketOnly.pages.length, 0);
  const recheck = await makeExecutor({stage: 'check'})(browser('complete'));
  assert.equal(recheck.first_page_recheck.status, 200);
  console.log('9 isolated browser protocol scenarios passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
