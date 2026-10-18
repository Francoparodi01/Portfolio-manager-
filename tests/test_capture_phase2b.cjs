// Isolated read protocol fixtures. No broker or database access.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const source=fs.readFileSync(path.join(__dirname,'../scripts/capture_phase2b.js'),'utf8');
const run=(page,requests)=>eval('('+source.replace('/* AUDIT_CONFIG */ {}',JSON.stringify({expected_account:'9',requests}))+')')(page);
const overview={key:'orders-current',url:'https://api.cocos.capital/api/orders',binary:false};
function page(mode='ok') {
  const calls=[];
  const request={method:()=> 'GET',url:()=> 'https://api.cocos.capital/api/v2/portfolio',
    allHeaders:async()=>({authorization:'SYNTHETIC_TOKEN',cookie:'PRIVATE_COOKIE',':authority':'PRIVATE_HOST'})};
  return {calls,url:()=> 'https://app.cocos.capital/portfolio?tab=value',
    locator:()=>({innerText:async()=>mode==='wrong-owner'?'Cuenta N° 8':'Cuenta N° 9'}),
    waitForRequest:async predicate=>{assert(predicate(request));return request;},goto:async()=>{},waitForTimeout:async()=>{},
    evaluate:async(fn,args)=>{
      calls.push(args.url);assert.equal(args.headers.cookie,undefined);assert.equal(args.headers[':authority'],undefined);
      const saved=global.fetch;global.fetch=async(url,opts)=>{
        assert.equal(opts.method,'GET');assert.equal(opts.credentials,'omit');
        return {status:mode==='denied'?403:200,ok:mode!=='denied',headers:new Map(),
          text:async()=>'{"data":[]}',arrayBuffer:async()=>new TextEncoder().encode('%PDF-fixture').buffer};};
      try{return await fn(args);}finally{global.fetch=saved;}
    }};
}
(async()=>{
  let p=page(),r=await run(p,[overview]);assert.equal(r.status,'CAPTURED');assert.equal(r.records.length,1);
  assert(!JSON.stringify(r).includes('SYNTHETIC_TOKEN'));
  p=page('wrong-owner');r=await run(p,[overview]);assert.equal(r.status,'SOURCE_BLOCKED');assert.equal(p.calls.length,0);
  p=page();r=await run(p,[{...overview,url:'https://api.cocos.capital/api/v2/orders/77'}]);assert.equal(r.status,'SOURCE_BLOCKED');assert.equal(p.calls.length,0);
  p=page('denied');r=await run(p,[overview,overview]);assert.equal(r.status,'SOURCE_BLOCKED');assert.equal(p.calls.length,1);
  p=page();r=await run(p,[{key:'holdings-20261001.pdf',url:'https://api.cocos.capital/api/portfolio/reports/holdings?type=PDF&date=20261001',binary:true}]);
  assert.equal(r.status,'CAPTURED');assert(Buffer.from(r.records[0].base64,'base64').toString().startsWith('%PDF-'));
  p=page();r=await run(p,[{...overview,url:'https://outside.invalid/api/orders'}]);assert.equal(r.status,'SOURCE_BLOCKED');assert.equal(p.calls.length,0);
  console.log('6 isolated Phase 2B browser protocol scenarios passed');
})().catch(e=>{console.error(e);process.exitCode=1;});
