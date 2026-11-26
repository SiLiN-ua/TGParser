(() => {
 const root=document.getElementById('graph'); if(!root)return;
 const rows=JSON.parse(root.dataset.rows), ns='http://www.w3.org/2000/svg';
 const svg=document.createElementNS(ns,'svg'); svg.setAttribute('viewBox','0 0 1100 700'); svg.setAttribute('width','100%'); svg.setAttribute('height','700');root.append(svg);
 const g=document.createElementNS(ns,'g');svg.append(g);let scale=1,x=0,y=0,drag;
 const transform=()=>g.setAttribute('transform',`translate(${x},${y}) scale(${scale})`);
 svg.addEventListener('wheel',e=>{e.preventDefault();scale=Math.max(.3,Math.min(4,scale*(e.deltaY>0?.9:1.1)));transform()},{passive:false});
 svg.onpointerdown=e=>{if(e.target.closest('circle'))return;drag=[e.clientX,e.clientY,x,y];svg.setPointerCapture(e.pointerId)};svg.onpointermove=e=>{if(drag){x=drag[2]+e.clientX-drag[0];y=drag[3]+e.clientY-drag[1];transform()}};svg.onpointerup=()=>drag=null;svg.onpointercancel=()=>drag=null;
 const edges=rows.map(r=>r.kind==='publication'?{...r,a:'u'+r.sender_user_id,b:'c'+r.chat_id,al:r.name,bl:r.title||String(r.chat_id)}:{...r,a:'u'+r.source,b:'u'+r.target,al:r.source_label,bl:r.target_label});
 const nodes=new Map();edges.forEach(r=>{nodes.set(r.a,{id:r.a.slice(1),label:r.al,type:'user'});nodes.set(r.b,{id:r.b.slice(1),label:r.bl,type:r.b[0]==='c'?'chat':'user'})});
 const all=[...nodes.values()];const columns=Math.max(1,Math.ceil(Math.sqrt(all.length*1.6)));all.forEach((n,i)=>{n.x=90+(i%columns)*900/Math.max(1,columns-1);n.y=55+Math.floor(i/columns)*580/Math.max(1,Math.ceil(all.length/columns)-1)});
 function el(tag,attrs){const n=document.createElementNS(ns,tag);Object.entries(attrs).forEach(([k,v])=>n.setAttribute(k,v));g.append(n);return n}
 edges.forEach(r=>{const a=nodes.get(r.a),b=nodes.get(r.b);const l=el('line',{x1:a.x,y1:a.y,x2:b.x,y2:b.y,stroke:({reply:'#b69bff',forward:'#edbb74',publication:'#405779'})[r.kind],'stroke-width':2});const t=document.createElementNS(ns,'title');t.textContent=`${r.al} → ${r.bl}: ${r.kind}, ${r.n} сообщений`;l.append(t)});
 all.forEach(n=>{const c=el('circle',{cx:n.x,cy:n.y,r:n.type==='chat'?18:9,fill:n.type==='chat'?'#67dcc0':'#8caaff',tabindex:0,role:'button','aria-label':n.label});const t=el('text',{x:n.x+15,y:n.y+4,fill:'#dce7fa','font-size':13});t.textContent=n.label.slice(0,25);const select=()=>{const d=document.getElementById('graph-detail');d.replaceChildren();const h=document.createElement('h2');h.textContent=n.label;d.append(h);rows.filter(r=>n.type==='chat'?r.chat_id===n.id:r.sender_user_id===n.id).forEach(r=>{const p=document.createElement('p'),a=document.createElement('a');a.href=`/messages/${r.chat_id}/${r.message_id}`;a.textContent=`${r.name} · ${r.title||r.chat_id} · ${r.n} повідомлень · приклад`;p.append(a);d.append(p)});if(n.type==='user'){const a=document.createElement('a');a.href='/users/'+n.id;a.textContent='Відкрити профіль';d.append(a)}};c.onclick=select;c.onkeydown=e=>{if(e.key==='Enter')select()}});
 // Replace node handlers with typed evidence inspection for every edge type.
 [...g.querySelectorAll('circle')].forEach((circle,i)=>{
  const n=all[i];const select=()=>{
   const d=document.getElementById('graph-detail');d.replaceChildren();const h=document.createElement('h2');h.textContent=n.label;d.append(h);
   const key=(n.type==='chat'?'c':'u')+n.id;
   edges.filter(e=>e.a===key||e.b===key).forEach(e=>{
    const p=document.createElement('p');p.textContent=`${e.al} → ${e.bl} · ${{reply:'відповідь',forward:'пересилання з',publication:'публікація'}[e.kind]} · ${e.n} повідомлень`;
    const a=document.createElement('a');a.href=`/messages/${e.chat_id}/${e.message_id}`;a.textContent='Повідомлення-доказ';p.append(document.createElement('br'),a);
    if(String(e.chat_id).startsWith('-100')){const t=document.createElement('a');t.href=`https://t.me/c/${String(e.chat_id).slice(4)}/${e.message_id}`;t.target='_blank';t.rel='noopener noreferrer';t.textContent=' · Telegram';p.append(t)}d.append(p);
   });
   if(n.type==='user'){const a=document.createElement('a');a.href='/graph?user='+n.id;a.textContent='Сфокусувати граф на людині';d.append(a)}
   [...g.querySelectorAll('line')].forEach((l,j)=>l.setAttribute('opacity',edges[j].a===key||edges[j].b===key?'1':'.08'));
  };circle.onclick=select;circle.onkeydown=e=>{if(e.key==='Enter')select()};
 });
 if(!rows.length)root.textContent='Зв’язків не знайдено.';
})();
