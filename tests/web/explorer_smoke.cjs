// Run with node tests/web/explorer_smoke.cjs. No server or production DB needed.
const vm = require('node:vm');
const fs = require('node:fs');
const assert = require('node:assert/strict');
const elements = new Map();
function element() { return {textContent:'',value:'',children:[],style:{},hidden:false,append(...a){this.children.push(...a)},replaceChildren(){this.children=[]},addEventListener(){},setPointerCapture(){},getBoundingClientRect(){return {left:0,top:0,width:1200,height:800}},click(){this.onclick?.()}}; }
const context = new Proxy({createRadialGradient:()=>({addColorStop(){}}),measureText:t=>({width:t.length*6})}, {get:(o,k)=>o[k]??(()=>{}),set:(o,k,v)=>(o[k]=v,true)});
const get = s => {if(!elements.has(s))elements.set(s,element());return elements.get(s)};
get('#graph').getContext=()=>context;
const requests=[];
vm.runInNewContext(fs.readFileSync('web/static/explorer.js','utf8'),{
 document:{querySelector:get,createElement:element},devicePixelRatio:1,AbortController,URLSearchParams,
 requestAnimationFrame:fn=>{const timer=setTimeout(()=>fn(Date.now()),16);timer.unref();return timer},setTimeout,
 ResizeObserver:class {constructor(fn){this.fn=fn}observe(){this.fn()}},
 fetch:async(url,{signal})=>{if(signal.aborted)throw Object.assign(new Error(),{name:'AbortError'});requests.push(url);if(url.startsWith('/graph/relations'))return {ok:true,json:async()=>({edges:[{source:42,target:43,kind:'reply',weight:1,message_id:1}]})};const q=new URL(url,'http://localhost').searchParams;let rows,more=false;const offset=Number(q.get('offset'));if(!q.has('chat'))rows=[{id:-100123,title:'Test chat'}];else if(!q.has('user'))rows=offset?[]:[{id:42,title:'Alice'},{id:43,title:'Bob'}];else {rows=offset? [{id:2,title:'Second message'}]:[{id:1,title:'First message'}];more=!offset}return {ok:true,json:async()=>({rows,more,offset:offset+100})}}
});
const settle=()=>new Promise(r=>setTimeout(r,25));
async function search(text){const input=get('#graph-search');input.value=text;input.oninput({target:input});const buttons=get('#graph-results').children;assert.equal(buttons.length,1);buttons[0].onclick();await settle()}
(async()=>{
 await settle();assert.match(get('#graph-summary').textContent,/1 чатів/);
 await search('Test chat');assert.match(get('#graph-summary').textContent,/2 авторів/);
 await search('Alice');assert.match(get('#graph-summary').textContent,/1 повідомлень/);
 // Other authors must be absent from the focused level, not merely dimmed.
 get('#graph-search').oninput({target:{value:'Bob'}});assert.equal(get('#graph-results').children.length,0);
 await get('#graph-all').onclick();await settle();assert.match(get('#graph-summary').textContent,/2 повідомлень · завантажено все/);
 await search('First message');assert.equal(get('#graph-detail').hidden,false);
 assert.ok(get('#graph-card').children.some(n=>n.href==='https://t.me/c/123/1'));
 get('#graph-close').onclick();assert.equal(get('#graph-detail').hidden,true);
 get('#graph-back').onclick();assert.match(get('#graph-summary').textContent,/2 авторів/);
 await search('Bob');assert.ok(requests.some(url=>url.includes('user=43')));
 console.log('PASS: focus, hidden siblings, load all, back, message links and close');
})().catch(e=>{console.error(e);process.exitCode=1});
