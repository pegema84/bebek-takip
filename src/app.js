/* ---------- veri (Firebase ile ortak) ---------- */
import {initializeApp} from 'firebase/app';
import {initializeFirestore,getFirestore,persistentLocalCache,persistentMultipleTabManager,collection,doc,query,where,onSnapshot,writeBatch} from 'firebase/firestore';
import {getAuth,onAuthStateChanged,signInAnonymously} from 'firebase/auth';

const FB={
  apiKey:"AIzaSyDSJmbd5EUHrPWwCCLvBCmvpwFspabA6SY",
  authDomain:"bebek-takip-2ab30.firebaseapp.com",
  projectId:"bebek-takip-2ab30",
  storageBucket:"bebek-takip-2ab30.firebasestorage.app",
  messagingSenderId:"386088030461",
  appId:"1:386088030461:web:563d1e025a90fe2aab6e94"
};
const VERSION='1.6',BUILD='16 Kasım 2026';
const FAM_KEY='bebektakip.family';
const COLS=['events','meds','growth','vax','timers','meta','appts','notes'];
let S={baby:{name:'',birth:''},events:[],meds:[],vax:{},growth:[],timers:{},appts:[],notes:[]};
let FID='',fdb=null,fauth=null,ready=false,syncError='',authFail=false,unsubs=[],lastUid='',lastErrMsg='',reauthN=0;
const shadow=new Map(),got=new Set(),pend={};
let memFam='';
function getFamily(){try{return localStorage.getItem(FAM_KEY)||memFam;}catch(e){return memFam;}}
function setFamily(c){memFam=c;try{if(c)localStorage.setItem(FAM_KEY,c);else localStorage.removeItem(FAM_KEY);}catch(e){}}
function newCode(){const al='abcdefghjkmnpqrstuvwxyz23456789',a=new Uint8Array(24);crypto.getRandomValues(a);return Array.from(a,x=>al[x%al.length]).join('');}
const cleanCode=v=>String(v||'').toLowerCase().replace(/[^a-z0-9]/g,'');
const showCode=c=>c.replace(/(.{4})/g,'$1-').replace(/-$/,'');
function stable(o){const r={};Object.keys(o).sort().forEach(k=>{if(o[k]!==undefined)r[k]=o[k];});return JSON.stringify(r);}
function currentMap(){
  const m=new Map();
  S.events.forEach(e=>m.set('events/'+e.id,e));
  S.meds.forEach(e=>m.set('meds/'+e.id,e));
  S.growth.forEach(e=>m.set('growth/'+e.id,e));
  S.appts.forEach(e=>m.set('appts/'+e.id,e));
  S.notes.forEach(e=>m.set('notes/'+e.id,e));
  Object.entries(S.vax).forEach(([k,v])=>m.set('vax/'+k,{d:v}));
  Object.entries(S.timers).forEach(([k,v])=>m.set('timers/'+k,v));
  if(S.baby.name||S.baby.birth)m.set('meta/baby',S.baby);
  return m;
}
function save(){
  if(!FID||!fdb)return;
  const cur=currentMap(),ops=[];
  cur.forEach((v,k)=>{const x=stable(v);if(shadow.get(k)!==x){ops.push(['set',k,v]);shadow.set(k,x);}});
  [...shadow.keys()].forEach(k=>{if(!cur.has(k)){ops.push(['del',k]);shadow.delete(k);}});
  for(let i=0;i<ops.length;i+=400){
    const b=writeBatch(fdb);
    ops.slice(i,i+400).forEach(([t,k,v])=>{const [c,id]=k.split('/'),r=doc(fdb,'families',FID,c,id);if(t==='set')b.set(r,JSON.parse(JSON.stringify(v)));else b.delete(r);});
    b.commit().catch(e=>{syncError=e&&e.code||'hata';lastErrMsg=e&&e.message||'';if(syncError==='permission-denied')onDenied();render();});
  }
}
function applySnap(c,snap){
  [...shadow.keys()].forEach(k=>{if(k.startsWith(c+'/'))shadow.delete(k);});
  const docs=snap.docs.map(d=>({id:d.id,data:d.data()}));
  docs.forEach(x=>shadow.set(c+'/'+x.id,stable(x.data)));
  if(c==='events')S.events=docs.map(x=>Object.assign({},x.data,{id:x.id}));
  else if(c==='meds')S.meds=docs.map(x=>Object.assign({},x.data,{id:x.id}));
  else if(c==='growth')S.growth=docs.map(x=>Object.assign({},x.data,{id:x.id}));
  else if(c==='appts')S.appts=docs.map(x=>Object.assign({},x.data,{id:x.id}));
  else if(c==='notes')S.notes=docs.map(x=>Object.assign({},x.data,{id:x.id}));
  else if(c==='vax'){S.vax={};docs.forEach(x=>{S.vax[x.id]=x.data.d;});}
  else if(c==='timers'){S.timers={};docs.forEach(x=>{S.timers[x.id]=x.data;});}
  else if(c==='meta'){const b=docs.find(x=>x.id==='baby');S.baby=Object.assign({name:'',birth:''},b?b.data:{});}
  pend[c]=snap.metadata.hasPendingWrites;syncError='';
  got.add(c);if(got.size===COLS.length)ready=true;
  render();
}
function relisten(){
  unsubs.forEach(f=>{try{f();}catch(e){}});unsubs=[];
  const d=new Date();d.setHours(0,0,0,0);const since=+d-90*864e5;
  COLS.forEach(c=>{
    const base=collection(fdb,'families',FID,c);
    const q=c==='events'?query(base,where('t','>=',since)):base;
    unsubs.push(onSnapshot(q,{includeMetadataChanges:true},snap=>applySnap(c,snap),err=>{syncError=err&&err.code||'hata';lastErrMsg=err&&err.message||'';if(syncError==='permission-denied')onDenied();render();}));
  });
}
function reauth(){
  if(!fauth)return;
  fauth.signOut().then(()=>signInAnonymously(fauth)).catch(()=>{authFail=true;render();});
}
function onDenied(){if(reauthN<2){reauthN++;reauth();}}
function startSync(){
  const app=initializeApp(FB);
  try{fdb=initializeFirestore(app,{localCache:persistentLocalCache({tabManager:persistentMultipleTabManager()})});}
  catch(e){fdb=getFirestore(app);}
  fauth=getAuth(app);
  const signIn=()=>signInAnonymously(fauth).then(()=>{authFail=false;},e=>{authFail=true;lastErrMsg=e&&e.code||'';render();});
  onAuthStateChanged(fauth,u=>{
    if(u){
      authFail=false;
      if(u.uid!==lastUid){const prev=lastUid;lastUid=u.uid;relisten();if(prev)setTimeout(()=>{shadow.clear();save();},2500);}
    }else signIn();
  });
  addEventListener('online',()=>{if(!fauth.currentUser)signIn();render();});
  addEventListener('offline',render);
  setTimeout(()=>{if(!ready){ready=true;render();}},3500);
}
function syncStatus(){
  if(!FID)return '';
  if(syncError==='permission-denied')return '🔴 erişim reddedildi';
  if(syncError)return '🔴 senkron hatası';
  if(authFail&&!navigator.onLine)return '🟠 internet yok';
  if(!navigator.onLine)return '🟠 çevrimdışı';
  if(Object.values(pend).some(Boolean))return '🟠 gönderiliyor';
  return got.size===COLS.length?'🟢 senkron':'⏳ bağlanıyor';
}

/* ---------- yardımcılar ---------- */
const $=s=>document.querySelector(s);
const uid=()=>Math.random().toString(36).slice(2,9);
const pad=n=>String(n).padStart(2,'0');
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const hm=t=>{const d=new Date(t);return pad(d.getHours())+':'+pad(d.getMinutes());};
const dayKey=t=>{const d=new Date(t);return d.getFullYear()+'-'+pad(d.getMonth()+1)+'-'+pad(d.getDate());};
const todayStr=()=>dayKey(Date.now());
const ago=ms=>{const m=Math.floor(ms/60000);if(m<1)return 'şimdi';if(m<60)return m+' dk önce';const h=Math.floor(m/60),r=m%60;if(h<24)return h+' sa'+(r?' '+r+' dk':'')+' önce';return Math.floor(h/24)+' gün önce';};
const dur=ms=>{const m=Math.max(1,Math.round(ms/60000));return m<60?m+' dk':Math.floor(m/60)+' sa'+(m%60?' '+(m%60)+' dk':'');};
const mmss=ms=>{const s=Math.max(0,Math.floor(ms/1000)),h=Math.floor(s/3600),m=Math.floor(s%3600/60);return (h?h+':'+pad(m):m)+':'+pad(s%60);};
const fmtDate=d=>new Date(d).toLocaleDateString('tr-TR',{day:'numeric',month:'long',year:'numeric'});
const fmtShort=d=>new Date(d).toLocaleDateString('tr-TR',{day:'numeric',month:'short'});
const num=v=>{const n=parseFloat(String(v).replace(',','.'));return isFinite(n)?n:NaN;};
function parseDate(s){const [y,m,d]=s.split('-').map(Number);return new Date(y,m-1,d);}
function addMonths(d,n){const r=new Date(d.getFullYear(),d.getMonth()+n,1);const last=new Date(r.getFullYear(),r.getMonth()+1,0).getDate();r.setDate(Math.min(d.getDate(),last));return r;}
let viewDay='';
const vday=()=>viewDay||todayStr();
function tAt(tid,did){
  const dv=$('#'+did).value,tv=$('#'+tid).value;
  const d=dv?parseDate(dv):new Date();
  if(tv){const [h,m]=tv.split(':').map(Number);d.setHours(h,m,0,0);}else if(dv&&dv!==todayStr())d.setHours(12,0,0,0);else{const n=new Date();d.setHours(n.getHours(),n.getMinutes(),0,0);}
  const t=d.getTime();
  if(t>Date.now()+5*60000){flash('Gelecek bir tarih/saat girilemez');return null;}
  return t;
}
const dtRow=t=>`<div class="two"><div><label>Tarih</label><input type="date" id="f-date" value="${t?dayKey(t):vday()}" max="${todayStr()}"></div><div><label>Saat</label><input type="time" id="f-time" value="${t?hm(t):nowHM()}"></div></div>`;
function tFrom(id){const v=$('#'+id).value;if(!v)return Date.now();const [h,m]=v.split(':').map(Number);const d=new Date();d.setHours(h,m,0,0);if(d.getTime()>Date.now()+5*60000)d.setDate(d.getDate()-1);return d.getTime();}
function ageText(){
  if(!S.baby.birth)return 'Doğum tarihi girilmedi';
  const b=parseDate(S.baby.birth),n=new Date();
  let m=(n.getFullYear()-b.getFullYear())*12+n.getMonth()-b.getMonth(),d=n.getDate()-b.getDate();
  if(d<0){m--;d+=new Date(n.getFullYear(),n.getMonth(),0).getDate();}
  if(m<0)return 'Henüz doğmadı';
  return (m?m+' ay ':'')+d+' gün';
}
let toastT;
function flash(msg){const t=$('#toast');t.textContent=msg;t.hidden=false;clearTimeout(toastT);toastT=setTimeout(()=>t.hidden=true,2200);}

/* ---------- aşı takvimi (T.C. Sağlık Bakanlığı, güncel) ---------- */
const VAX=[
 {m:0, l:'Doğumda',   v:['Hepatit B 1. doz']},
 {m:2, l:'2. ay sonu',v:['BCG (verem)','KPA (pnömokok) 1. doz','Altılı karma 1. doz']},
 {m:4, l:'4. ay sonu',v:['KPA 2. doz','Altılı karma 2. doz']},
 {m:6, l:'6. ay sonu',v:['Altılı karma 3. doz','OPA (oral çocuk felci) 1. doz']},
 {m:9, l:'9. ay sonu',v:['KKK ek doz'],note:'Bakanlık kartında ek doz olarak geçiyor. Aile hekimiyle teyit et.'},
 {m:12,l:'12. ay sonu',v:['KPA pekiştirme','KKK 1. doz','Suçiçeği 1. doz']},
 {m:18,l:'18. ay sonu',v:['Altılı karma pekiştirme','OPA 2. doz','Hepatit A 1. doz']},
 {m:24,l:'24. ay sonu',v:['Hepatit A 2. doz']},
 {m:48,l:'48. ay (4 yaş)',v:['KKK 2. doz','DaBT-İPA pekiştirme','Suçiçeği 2. doz']}
];
/* doktorun önerdiği takvim dışı (özel) aşılar */
const VOPT=[
 {k:'o2', m:2, l:'Özel · 2. ay',v:['Rotarix (rotavirüs) 1. doz'],info:'100. günden önce başlanmalı (son gün 24 Aralık 2026). Doktorun planı: 2. ve 4. ay ya da 3. ve 5. ay. Hangisini seçtiysen yapıldığı tarihi gir.'},
 {k:'o3', m:3, l:'Özel · 3. ay',v:['Bexsero (menenjit) 1. doz','Nimenrix (menenjit) 1. doz'],info:'Reçetede Bexsero ve Nimenrix için 3., 5. ve 15. ay yazıyor.'},
 {k:'o4', m:4, l:'Özel · 4. ay',v:['Rotarix (rotavirüs) 2. doz']},
 {k:'o5', m:5, l:'Özel · 5. ay',v:['Bexsero (menenjit) 2. doz','Nimenrix (menenjit) 2. doz']},
 {k:'o8', m:8, l:'Özel · en geç 8. ay',v:['Beyfortus (RSV)'],note:'Doktor ilk 8 ay içinde yazdı. Tam zamanı doktora sor, sonra tarihi gir.'},
 {k:'o13',m:13,l:'Özel · grip (tahmini)',v:['Grip aşısı'],note:'Doktor önerdi, zamanı belli değil. Tarih tahminidir, doktora sor.'},
 {k:'o15',m:15,l:'Özel · 15. ay',v:['Bexsero (menenjit) 3. doz','Nimenrix (menenjit) 3. doz']}
];
const ALLV=[...VAX,...VOPT].sort((a,b)=>a.m-b.m);
const gk=g=>g.k||g.m;

/* ---------- ekranlar ---------- */
let tab='today';
const TABS=[['today','🏠','Bugün'],['med','💊','İlaç'],['vax','💉','Aşı'],['growth','📏','Büyüme'],['info','📖','Bilgi']];

function evInfo(e){
  switch(e.type){
    case 'breast':return {ic:'🤱',t:'Emzirme'+(e.side?' · '+e.side:''),s:e.dur?dur(e.dur):''};
    case 'bottle':return {ic:'🍼',t:'Biberon · '+e.ml+' ml',s:e.kind||''};
    case 'diaper':return {ic:e.kind==='wet'?'💧':e.kind==='dirty'?'💩':'💧',t:{wet:'Islak bez',dirty:'Kirli bez',both:'Islak + kirli bez'}[e.kind]||'Bez',s:e.color||''};
    case 'sleep':return {ic:'😴',t:'Uyku',s:e.dur?dur(e.dur):''};
    case 'med':return {ic:'💊',t:e.name||'İlaç',s:e.dose||''};
  }
  return {ic:'•',t:e.type,s:''};
}

function alertList(){
  const out=[],now=Date.now();
  if(S.baby.birth){
    const items=vaxItems(),today=parseDate(todayStr());
    ALLV.forEach(g=>{
      if(g.note)return;
      const u=items.filter(i=>i.g===g&&!i.done);if(!u.length)return;
      const d=Math.round((u[0].due-today)/864e5);
      if(d<=14)out.push({tab:'vax',ic:'💉',t:g.l+': '+u.map(i=>i.name).join(', '),tag:d<0?(-d)+' gün gecikti':d===0?'Bugün':d+' gün kaldı',cls:d<0?'bad':'warn',d});
    });
  }
  S.appts.forEach(a=>{
    const d=(a.t-Date.now())/864e5;
    if(d>-0.05&&d<=2){const dk=Math.round((parseDate(dayKey(a.t))-parseDate(todayStr()))/864e5);
      out.push({tab:'info',topic:'appt',ic:'🩺',t:(a.title||'Muayene')+' · '+hm(a.t)+(a.place?' · '+a.place:''),tag:dk<=0?'Bugün':dk===1?'Yarın':dk+' gün kaldı',cls:'warn',d:dk-0.9});}
  });
  const late=medSlots().filter(s=>!s.ev&&s.due<now);
  if(late.length)out.push({tab:'med',ic:'💊',t:late.length+' ilaç dozu gecikti',tag:late[0].m.name+(late.length>1?' +'+(late.length-1):''),cls:'warn',d:-0.5});
  return out.sort((a,b)=>a.d-b.d).slice(0,4);
}

/* ---------- takvime ekleme (.ics) ---------- */
const icsEsc=t=>String(t).replace(/\\/g,'\\\\').replace(/;/g,'\;').replace(/,/g,'\\,').replace(/\n/g,'\\n');
function icsFold(l){const e=new TextEncoder();if(e.encode(l).length<=74)return l;let out='',cur='';for(const ch of l){if(e.encode(cur+ch).length>73){out+=cur+'\r\n ';cur=ch;}else cur+=ch;}return out+cur;}
const icsDate=d=>d.getFullYear()+pad(d.getMonth()+1)+pad(d.getDate());
const icsStamp=()=>new Date().toISOString().replace(/[-:]/g,'').replace(/\.\d+/,'');
function icsCal(evs){return ['BEGIN:VCALENDAR','VERSION:2.0','PRODID:-//Bebek Takip//TR','CALSCALE:GREGORIAN','METHOD:PUBLISH'].concat(...evs,['END:VCALENDAR']).map(icsFold).join('\r\n')+'\r\n';}
function vaxICS(){
  const items=vaxItems(),today=parseDate(todayStr()),tom=new Date(today.getFullYear(),today.getMonth(),today.getDate()+1),evs=[],nm=S.baby.name||'Bebek';
  ALLV.forEach(g=>{
    const u=items.filter(i=>i.g===g&&!i.done);if(!u.length)return;
    let d=u[0].due;const late=d<today;if(d<tom)d=tom;
    const end=new Date(d.getFullYear(),d.getMonth(),d.getDate()+1);
    evs.push(['BEGIN:VEVENT','UID:vax-'+gk(g)+'@bebek-takip','DTSTAMP:'+icsStamp(),'DTSTART;VALUE=DATE:'+icsDate(d),'DTEND;VALUE=DATE:'+icsDate(end),
      'SUMMARY:'+icsEsc('💉 '+nm+(g.k?' özel aşı: ':' aşı zamanı: ')+g.l),
      'DESCRIPTION:'+icsEsc(u.map(i=>i.name).join(', ')+(late?' (gecikmiş, aile hekimini ara)':'')+((g.note||g.info)?' - '+(g.note||g.info):'')),
      'BEGIN:VALARM','ACTION:DISPLAY','DESCRIPTION:'+icsEsc('Aşı zamanı yaklaşıyor'),'TRIGGER:-PT15H','END:VALARM',
      'BEGIN:VALARM','ACTION:DISPLAY','DESCRIPTION:'+icsEsc('Bugün aşı günü'),'TRIGGER:PT9H','END:VALARM','END:VEVENT']);
  });
  return evs;
}
function medICS(){
  const evs=[],now=new Date();
  S.meds.filter(m=>!m.off).forEach(m=>(m.times||[]).forEach(t=>{
    const [h,mi]=t.split(':').map(Number),st=new Date(now.getFullYear(),now.getMonth(),now.getDate(),h,mi);
    if(st<now)st.setDate(st.getDate()+1);
    const en=new Date(+st+10*60000),f=d=>icsDate(d)+'T'+pad(d.getHours())+pad(d.getMinutes())+'00';
    evs.push(['BEGIN:VEVENT','UID:med-'+m.id+'-'+t.replace(':','')+'@bebek-takip','DTSTAMP:'+icsStamp(),'DTSTART:'+f(st),'DTEND:'+f(en),'RRULE:FREQ=DAILY',
      'SUMMARY:'+icsEsc('💊 '+m.name+(m.dose?' · '+m.dose:'')),'DESCRIPTION:'+icsEsc('Bebek Takip ilaç hatırlatması'),
      'BEGIN:VALARM','ACTION:DISPLAY','DESCRIPTION:'+icsEsc(m.name),'TRIGGER:PT0S','END:VALARM','END:VEVENT']);
  }));
  return evs;
}
function saveICS(name,evs){
  if(!evs.length)return flash('Takvime eklenecek bir şey yok');
  const url=URL.createObjectURL(new Blob([icsCal(evs)],{type:'text/calendar;charset=utf-8'}));
  const ios=/iP(hone|ad|od)/.test(navigator.userAgent)||(navigator.platform==='MacIntel'&&navigator.maxTouchPoints>1);
  if(ios){location.href=url;return;}
  const a=document.createElement('a');a.href=url;a.download=name;document.body.appendChild(a);a.click();a.remove();
  setTimeout(()=>URL.revokeObjectURL(url),30000);
  flash('Dosya indi. Açıp takvime ekle');
}

let avgN=7;
const f1=x=>(Math.round(x*10)/10).toString().replace('.',',');
function avgCard(){
  const tk=todayStr(),t0=parseDate(tk),start=new Date(t0.getFullYear(),t0.getMonth(),t0.getDate()-avgN);
  const evs=S.events.filter(e=>e.type!=='med'&&e.t>=start&&e.t<t0);
  const days=new Set(evs.map(e=>dayKey(e.t))).size;
  let h=`<h2>Günlük ortalama · son ${avgN} gün</h2><div class="card"><div class="row" style="margin-bottom:10px">${[1,3,7,14].map(n=>`<button class="btn sm${avgN===n?' pri':''}" style="flex:1;padding:0 4px" data-act="avgN" data-v="${n}">${n} gün</button>`).join('')}</div>`;
  if(!days)return h+`<div class="mute" style="font-size:14px">Tamamlanmış gün kaydı olunca ortalama burada görünür (bugün hariç).</div></div>`;
  const br=evs.filter(e=>e.type==='breast'),bo=evs.filter(e=>e.type==='bottle'),di=evs.filter(e=>e.type==='diaper'),sl=evs.filter(e=>e.type==='sleep');
  const sum=(a,k)=>a.reduce((x,e)=>x+(e[k]||0),0);
  const fe=evs.filter(e=>e.type==='breast'||e.type==='bottle').map(e=>e.t).sort((a,b)=>a-b);
  let gaps=[];for(let i=1;i<fe.length;i++){const g=fe[i]-fe[i-1];if(g<=12*36e5)gaps.push(g);}
  const wet=di.filter(e=>e.kind!=='dirty').length,dirty=di.filter(e=>e.kind!=='wet').length;
  const cell=(t,b,sm)=>`<div class="stat"><span>${t}</span><b>${b}</b>${sm?`<span>${sm}</span>`:''}</div>`;
  h+=`<div class="stats" style="grid-template-columns:1fr 1fr">`+
    cell('🤱 Emzirme',f1(br.length/days)+' kez',br.length?'ort. '+dur(sum(br,'dur')/br.length):'')+
    cell('🍼 Biberon',f1(bo.length/days)+' kez · '+Math.round(sum(bo,'ml')/days)+' ml',bo.length?'ort. '+Math.round(sum(bo,'ml')/bo.length)+' ml / öğün':'')+
    cell('🧷 Bez',f1(wet/days)+' ıslak · '+f1(dirty/days)+' kirli','günde')+
    cell('😴 Uyku',sl.length?dur(sum(sl,'dur')/days):'—',sl.length?f1(sl.length/days)+' uyku · ort. '+dur(sum(sl,'dur')/sl.length):'')+
    `</div>`;
  if(gaps.length)h+=`<div class="stat" style="margin-top:8px"><span>Ortalama beslenme aralığı</span><b>${dur(gaps.reduce((a,b)=>a+b,0)/gaps.length)}</b></div>`;
  h+=`<div class="foot" style="margin-top:8px">Kayıt girilen ${days} gün üzerinden, bugün hariç. Kayıt girmediğin günler hesaba katılmaz.</div></div>`;
  return h;
}
function vToday(){
  const now=Date.now(),tk=todayStr();
  const all=[...S.events].sort((a,b)=>b.t-a.t);
  const vd=vday(),isT=vd===tk,today=all.filter(e=>dayKey(e.t)===vd);
  let h='';
  if(!S.baby.birth)h+=`<div class="card hint"><b>Başlayalım</b><p>Bebeğin adını ve doğum tarihini gir. Aşı takvimi buna göre hesaplanır.</p><button class="btn pri" data-act="gear">Bilgileri gir</button></div>`;
  for(const a of alertList())h+=`<button class="item" data-act="tab" data-v="${a.tab}"${a.topic?` data-topic="${a.topic}"`:''} style="border-color:var(--${a.cls==='bad'?'bad':'warn'})"><span class="ic">${a.ic}</span><span class="tx">${esc(a.t)}</span><span class="tag ${a.cls}">${esc(a.tag)}</span></button>`;
  for(const k of Object.keys(S.timers)){
    const t=S.timers[k];
    const lab=k==='breast'?'🤱 Emzirme'+(t.side?' · '+esc(t.side):''):'😴 Uyku';
    h+=`<div class="card timer"><div><div class="tl">${lab}</div><div class="tm" data-start="${t.start}">${mmss(now-t.start)}</div></div><div class="row"><button class="btn pri" data-act="stopTimer" data-k="${k}">Bitir</button><button class="btn ghost sm" data-act="cancelTimer" data-k="${k}">İptal</button></div></div>`;
  }
  h+=`<div class="qgrid">
    <button class="q feed" data-act="qBreast"><span>🤱</span>Emzirme</button>
    <button class="q bottle" data-act="qBottle"><span>🍼</span>Biberon</button>
    <button class="q diaper" data-act="qDiaper"><span>🧷</span>Bez</button>
    <button class="q sleep" data-act="qSleep"><span>😴</span>Uyku</button></div>`;
  const lastFeed=all.find(e=>e.type==='breast'||e.type==='bottle');
  const lastD=all.find(e=>e.type==='diaper');
  const lastDirty=all.find(e=>e.type==='diaper'&&e.kind!=='wet');
  h+=`<div class="stats">
    <div class="stat"><span>Son beslenme</span><b>${lastFeed?ago(now-lastFeed.t):'—'}</b></div>
    <div class="stat"><span>Son bez</span><b>${lastD?ago(now-lastD.t):'—'}</b></div>
    <div class="stat"><span>Son kaka</span><b>${lastDirty?ago(now-lastDirty.t):'—'}</b></div></div>`;
  const dl=isT?'Bugün':vd===dayKey(Date.now()-864e5)?'Dün · '+fmtShort(parseDate(vd)):fmtDate(parseDate(vd));
  h+=`<div class="row" style="align-items:center;gap:8px;margin:14px 0 8px"><button class="btn sm" data-act="dayGo" data-v="-1" aria-label="Önceki gün">‹</button><label class="btn sm" style="flex:1;position:relative;margin:0;display:flex;align-items:center;justify-content:center">${dl}<input type="date" value="${vd}" max="${tk}" data-chg="dayPick" style="position:absolute;inset:0;opacity:0;width:100%"></label><button class="btn sm" data-act="dayGo" data-v="1" ${isT?'disabled':''} aria-label="Sonraki gün">›</button></div>`;
  const br=today.filter(e=>e.type==='breast'),bo=today.filter(e=>e.type==='bottle'),di=today.filter(e=>e.type==='diaper'),sl=today.filter(e=>e.type==='sleep');
  const ml=bo.reduce((s,e)=>s+(e.ml||0),0),wet=di.filter(e=>e.kind!=='dirty').length,dirty=di.filter(e=>e.kind!=='wet').length,slp=sl.reduce((s,e)=>s+(e.dur||0),0);
  h+=`<div class="card"><div class="sum">
    <div><b>${br.length}</b><span>emzirme</span></div>
    <div><b>${ml}</b><span>ml biberon</span></div>
    <div><b>${wet}/${dirty}</b><span>ıslak/kirli</span></div>
    <div><b>${slp?dur(slp).replace(' sa','s').replace(' dk','dk'):'0'}</b><span>uyku</span></div></div></div>`;
  h+=`<h2>${isT?'Bugünün':'Günün'} akışı</h2>`;
  if(!today.length)h+=`<div class="card mute">${isT?'Bugün henüz kayıt yok. Yukarıdaki düğmelerle başla.':'Bu günde kayıt yok.'}</div>`;
  for(const e of today){const i=evInfo(e);h+=`<button class="item" data-act="openEv" data-id="${e.id}"><span class="ic">${i.ic}</span><span class="tx">${esc(i.t)}${i.s?`<small>${esc(i.s)}</small>`:''}</span><span class="tm2">${hm(e.t)}</span></button>`;}
  h+=avgCard();
  return h;
}

function medSlots(){
  const tk=todayStr(),out=[];
  S.meds.filter(m=>!m.off).forEach(m=>(m.times||[]).forEach(t=>{
    const slot=tk+' '+t,ev=S.events.find(e=>e.type==='med'&&e.medId===m.id&&e.slot===slot);
    const [h,mi]=t.split(':').map(Number),due=new Date();due.setHours(h,mi,0,0);
    out.push({m,t,slot,ev,due:due.getTime()});
  }));
  return out.sort((a,b)=>a.due-b.due);
}
function vMed(){
  const now=Date.now();let h='';
  const sl=medSlots();
  h+=`<h2>Bugünkü dozlar</h2>`;
  if(!sl.length)h+=`<div class="card mute">Saatli ilaç yok. Aşağıdan ekleyebilirsin.</div>`;
  for(const s of sl){
    const late=!s.ev&&s.due<now;
    const st=s.ev?`<span class="tag ok">${hm(s.ev.t)} verildi</span>`:late?`<span class="tag warn">Gecikti</span>`:`<span class="tag mute">Bekliyor</span>`;
    h+=`<button class="item" data-act="medSlot" data-id="${s.m.id}" data-slot="${esc(s.slot)}"><span class="check ${s.ev?'on':late?'late':''}">${s.ev?'✓':''}</span><span class="tx">${esc(s.m.name)}<small>${esc(s.m.dose||'')} · ${s.t}</small></span>${st}</button>`;
  }
  const prn=S.meds.filter(m=>!m.off&&!(m.times||[]).length);
  if(prn.length){
    h+=`<h2>Gerektiğinde</h2>`;
    for(const m of prn){
      const last=[...S.events].filter(e=>e.type==='med'&&e.medId===m.id).sort((a,b)=>b.t-a.t)[0];
      h+=`<div class="item"><span class="ic">💊</span><span class="tx">${esc(m.name)}<small>${esc(m.dose||'')}${last?' · son: '+ago(now-last.t):''}</small></span><button class="btn sm pri" data-act="medNow" data-id="${m.id}">Verdim</button></div>`;
    }
  }
  h+=`<h2>İlaçlarım</h2>`;
  for(const m of S.meds)h+=`<button class="item" data-act="openMed" data-id="${m.id}"><span class="ic">${m.off?'⏸':'💊'}</span><span class="tx">${esc(m.name)}<small>${esc(m.dose||'')}${(m.times||[]).length?' · '+m.times.join(', '):' · gerektiğinde'}${m.off?' · kapalı':''}</small></span><span class="mute">›</span></button>`;
  h+=`<button class="btn block pri" data-act="openMed" data-id="">+ İlaç / vitamin ekle</button>`;
  h+=`<button class="btn block" style="margin-top:12px" data-act="icsMed">📅 İlaç saatlerini takvime ekle</button><div class="foot">Takvimde her gün tekrarlanır ve alarm çalar. Bir ilacı bırakırsan o etkinliği takvimden de silmen gerekir. Geciken dozlar burada ve Bugün ekranında turuncu görünür.</div>`;
  return h;
}

function vaxItems(){
  const b=S.baby.birth?parseDate(S.baby.birth):null,out=[];
  ALLV.forEach(g=>g.v.forEach((name,i)=>{const id=gk(g)+'-'+i;out.push({id,g,name,done:S.vax[id]||null,due:b?addMonths(b,g.m):null});}));
  return out;
}
function vVax(){
  if(!S.baby.birth)return `<div class="card hint"><b>Doğum tarihi gerekli</b><p>Aşı zamanlarını hesaplamak için bebeğin doğum tarihini gir.</p><button class="btn pri" data-act="gear">Bilgileri gir</button></div>`;
  const items=vaxItems(),today=parseDate(todayStr());
  const dd=d=>Math.round((d-today)/864e5);
  let h='';
  const nextG=ALLV.find(g=>items.some(i=>i.g===g&&!i.done));
  if(nextG){
    const it=items.filter(i=>i.g===nextG&&!i.done),d=dd(it[0].due);
    h+=`<div class="card next"><div class="mute" style="font-size:13px">Sıradaki aşılar</div><b>${nextG.l}</b> · ${fmtDate(it[0].due)}<div class="${d<0?'bad':d<=14?'warn':'mute'}" style="font-weight:600;margin-top:2px">${d<0?-d+' gün gecikti':d===0?'Bugün':d+' gün kaldı'}</div><div style="margin-top:6px">${it.map(i=>esc(i.name)).join(' · ')}</div></div>`;
  }else h+=`<div class="card next"><b>Takvimdeki tüm aşılar tamam 🎉</b></div>`;
  let optHead=false;
  for(const g of [...VAX,...VOPT]){
    if(g.k&&!optHead){optHead=true;h+=`<h2 style="margin-top:22px">🩺 Doktorun önerdiği özel aşılar</h2><div class="foot" style="margin-top:-4px">Bunlar ulusal takvimde yok, doktorun reçetesine göre eklendi.</div>`;}
    const gi=items.filter(i=>i.g===g);
    h+=`<h2>${g.l} · ${fmtShort(gi[0].due)} ${gi[0].due.getFullYear()}</h2>`;
    if(g.note||g.info)h+=`<div class="foot" style="margin-top:-4px">${g.note||g.info}</div>`;
    for(const i of gi){
      const d=dd(i.due);
      const st=i.done?`<span class="tag ok">${fmtShort(parseDate(i.done))}</span>`:d<0?`<span class="tag bad">${-d} gün gecikti</span>`:d<=14?`<span class="tag warn">${d===0?'Bugün':d+' gün kaldı'}</span>`:`<span class="tag mute">${d>60?Math.round(d/30)+' ay':d+' gün'}</span>`;
      h+=`<button class="item" data-act="openVax" data-id="${i.id}"><span class="check ${i.done?'on':''}">${i.done?'✓':''}</span><span class="tx">${esc(i.name)}${i.g.k?' <span class="tag warn" style="font-size:11px">özel</span>':''}</span>${st}</button>`;
    }
  }
  h+=`<button class="btn block" style="margin-top:14px" data-act="icsVax">📅 Aşı hatırlatmalarını takvime ekle</button>`;
  h+=`<div class="foot">Takvim, T.C. Sağlık Bakanlığı ulusal çocukluk dönemi aşı takvimine göre hazırlandı (altılı karma aşı Nisan 2025'ten beri geçerli). Tarihler yaklaşıktır, kesin randevuyu aile hekimin verir.</div>`;
  return h;
}

/* ---------- bilgi menüsü ---------- */
let infoTopic='';
const MILES=[
 ['Sosyal Gülümseme','Siz gülünce veya konuşunca gülümser.',2,2,'2. ay'],
 ['Başını Dik Tutma','Kucakta desteklenirken başını kontrol eder.',2,4,'2–4. ay'],
 ['Kahkaha Atma','Oyun sırasında kıkırdamaya başlar.',4,4,'4. ay'],
 ['Oyuncaklara Uzanma','Yakınındaki nesnelere uzanır.',4,4,'4. ay'],
 ['Dönme','Yüzüstünden sırtüstüne veya tersine döner.',4,6,'4–6. ay'],
 ['Desteksiz Oturma','Uzun süre tek başına oturur.',6,9,'6–9. ay'],
 ['Nesneyi Elden Ele Geçirme','Oyuncağı bir elinden diğerine aktarır.',6,6,'6. ay'],
 ['İsmine Tepki Verme','Adı söylenince dönüp bakar.',9,9,'9. ay'],
 ['“Ma-ma”, “Ba-ba” Heceleme','Ses dizilerini tekrar eder.',9,9,'9. ay'],
 ['Emekleme','Elleri ve dizleri üzerinde ilerler.',7,10,'7–10. ay'],
 ['Bay Bay Yapma','Biri giderken el sallar.',9,12,'9–12. ay'],
 ['Tutunarak Ayağa Kalkma','Mobilyalardan destek alır.',9,12,'9–12. ay'],
 ['Küçük Nesneleri Tutma','Baş ve işaret parmağını kullanır.',9,12,'9–12. ay'],
 ['İlk Adımlar','Tek başına birkaç adım atar.',12,15,'12–15. ay'],
 ['İlk Anlamlı Kelimeler','“Anne”, “baba” gibi kelimeleri anlamlı kullanır.',12,12,'12. ay']
];
function bodyMiles(){
  const am=S.baby.birth?(Date.now()-parseDate(S.baby.birth))/864e5/30.4375:null;
  let h=`<div class="foot" style="margin:0 4px 10px">Her bebek kendi hızında gelişir; aylar ortalama değerlerdir.${am!==null?' Bebeğinin şu anki dönemine denk gelenler <b>Şimdi</b> ile işaretli.':''}</div>`;
  MILES.forEach((m,i)=>{
    const now=am!==null&&am>=m[2]-.5&&am<=m[3]+.5;
    h+=`<div class="item" style="${now?'border-color:var(--pri)':''}"><span class="ic" style="font-weight:700;color:var(--mute);font-size:16px">${i+1}</span><span class="tx"><b>${esc(m[0])}</b><small>${esc(m[1])}</small></span>${now?'<span class="tag ok" style="margin-right:6px">Şimdi</span>':''}<span class="tag mute">${m[4]}</span></div>`;
  });
  return h;
}
const dtLong=t=>new Date(t).toLocaleDateString('tr-TR',{weekday:'long',day:'numeric',month:'long',year:'numeric'});
function vAppt(){
  const now=Date.now(),all=[...S.appts].sort((a,b)=>a.t-b.t),up=all.filter(a=>a.t>=now-36e5),past=all.filter(a=>a.t<now-36e5).reverse();
  let h=`<button class="btn pri block" data-act="openAppt" data-id="">+ Randevu ekle</button>`;
  const row=(a,old)=>{const dk=Math.round((parseDate(dayKey(a.t))-parseDate(todayStr()))/864e5);
    const tg=old?'':`<span class="tag ${dk<=2?'warn':'mute'}">${dk<=0?'Bugün':dk===1?'Yarın':dk+' gün'}</span>`;
    return `<button class="item" data-act="openAppt" data-id="${a.id}"><span class="ic">🩺</span><span class="tx"><b>${esc(a.title||'Muayene')}</b><small>${esc(dtLong(a.t))} · ${hm(a.t)}${a.place?' · '+esc(a.place):''}</small></span>${tg}</button>`;};
  h+=`<h2>Yaklaşan</h2>`;
  if(!up.length)h+=`<div class="card mute">Yaklaşan randevu yok.</div>`;
  up.forEach(a=>h+=row(a,false));
  if(up.length)h+=`<button class="btn block" style="margin-top:6px" data-act="icsAppts">📅 Yaklaşan randevuları takvime ekle</button><div class="foot">Takvimde bir gün önce ve 2 saat önce alarm çalar.</div>`;
  if(past.length){h+=`<h2>Geçmiş</h2>`;past.forEach(a=>h+=row(a,true));}
  return h;
}
function openAppt(id){
  const a=S.appts.find(x=>x.id===id)||{},d=a.t?new Date(a.t):null;
  openSheet(`<h3>🩺 ${id?'Randevuyu düzenle':'Randevu ekle'}</h3>
  <div class="two"><div><label>Tarih</label><input type="date" id="a-date" value="${d?dayKey(a.t):''}"></div><div><label>Saat</label><input type="time" id="a-time" value="${d?hm(a.t):'10:00'}"></div></div>
  <label>Ne için</label><input type="text" id="a-title" value="${esc(a.title||'Muayene')}">
  <div class="chips">${['Muayene','Kontrol','Aşı','Tahlil'].map(n=>`<button data-act="apptTitle" data-v="${n}">${n}</button>`).join('')}</div>
  <label>Doktor / yer</label><input type="text" id="a-place" value="${esc(a.place||'')}" placeholder="Dr. ... / hastane">
  <label>Not</label><textarea id="a-note" rows="3" placeholder="Götürülecekler, sorulacaklar">${esc(a.note||'')}</textarea>
  <button class="btn pri block" style="margin-top:16px" data-act="saveAppt" data-id="${id||''}">Kaydet</button>
  ${id?`<button class="btn block" style="margin-top:10px" data-act="icsAppt" data-id="${id}">📅 Takvime ekle</button>${delBlock('delAppt',id)}`:''}`);
}
function apptICS(list){
  const nm=S.baby.name||'Bebek',f=x=>icsDate(x)+'T'+pad(x.getHours())+pad(x.getMinutes())+'00';
  return list.map(a=>{const st=new Date(a.t),en=new Date(a.t+45*60000);
    const e=['BEGIN:VEVENT','UID:appt-'+a.id+'@bebek-takip','DTSTAMP:'+icsStamp(),'DTSTART:'+f(st),'DTEND:'+f(en),'SUMMARY:'+icsEsc('🩺 '+nm+' '+(a.title||'Muayene'))];
    if(a.place)e.push('LOCATION:'+icsEsc(a.place));
    if(a.note)e.push('DESCRIPTION:'+icsEsc(a.note));
    e.push('BEGIN:VALARM','ACTION:DISPLAY','DESCRIPTION:'+icsEsc('Yarın randevu var'),'TRIGGER:-P1D','END:VALARM','BEGIN:VALARM','ACTION:DISPLAY','DESCRIPTION:'+icsEsc('Randevuya 2 saat kaldı'),'TRIGGER:-PT2H','END:VALARM','END:VEVENT');
    return e;});
}
function vNotes(){
  const all=[...S.notes].sort((a,b)=>(b.d||'').localeCompare(a.d||'')||(b.c||0)-(a.c||0));
  let h=`<button class="btn pri block" data-act="openNote" data-id="">+ Not ekle</button><div class="foot">Doktorun tavsiyelerini buraya yaz, ikiniz de görürsünüz.</div>`;
  if(!all.length)h+=`<div class="card mute">Henüz not yok.</div>`;
  all.forEach(n=>{const sn=(n.text||'').replace(/\s+/g,' ').slice(0,110);
    h+=`<button class="item" data-act="openNote" data-id="${n.id}"><span class="ic">📝</span><span class="tx"><b>${esc(n.title||sn.slice(0,40)||'Not')}</b><small>${n.d?esc(fmtDate(parseDate(n.d))):''}${n.title&&sn?' · '+esc(sn):''}</small></span></button>`;});
  return h;
}
function openNote(id){
  const n=S.notes.find(x=>x.id===id)||{};
  openSheet(`<h3>📝 ${id?'Notu düzenle':'Doktor notu ekle'}</h3>
  <label>Tarih</label><input type="date" id="n-date" value="${n.d||todayStr()}">
  <label>Başlık (isteğe bağlı)</label><input type="text" id="n-title" value="${esc(n.title||'')}" placeholder="Beslenme, ilaç, uyku…">
  <label>Not</label><textarea id="n-text" rows="9" placeholder="Doktorun söyledikleri">${esc(n.text||'')}</textarea>
  <button class="btn pri block" style="margin-top:16px" data-act="saveNote" data-id="${id||''}">Kaydet</button>
  ${id?delBlock('delNote',id):''}`);
}
const INFO=[
 {id:'appt',ic:'🩺',t:'Muayene randevuları',sub:()=>{const n=S.appts.filter(a=>a.t>=Date.now()-36e5).sort((a,b)=>a.t-b.t)[0];return n?'Sıradaki: '+fmtShort(n.t)+' '+hm(n.t):'Randevu ekle, takvime aktar';},body:vAppt},
 {id:'notes',ic:'📝',t:'Doktor notları',sub:()=>S.notes.length?S.notes.length+' not':'Doktorun tavsiyeleri',body:vNotes},
 {id:'miles',ic:'🧸',t:'Bebekler bunları ne zaman yapar?',sub:'Gelişim basamakları, ay ay',body:bodyMiles}
];
function vInfo(){
  const t=INFO.find(x=>x.id===infoTopic);
  if(t)return `<button class="btn sm ghost" data-act="infoBack" style="margin-bottom:6px">← Bilgi menüsü</button><h2 style="margin-top:8px">${t.ic} ${esc(t.t)}</h2>`+t.body();
  let h=`<h2>📖 Bilgi menüsü</h2>`;
  for(const x of INFO)h+=`<button class="item" data-act="infoOpen" data-id="${x.id}"><span class="ic">${x.ic}</span><span class="tx">${esc(x.t)}<small>${esc(typeof x.sub==='function'?x.sub():x.sub)}</small></span><span class="tag mute">›</span></button>`;
  return h;
}
function vGrowth(){
  const g=[...S.growth].sort((a,b)=>a.d.localeCompare(b.d));
  let h=`<button class="btn block pri" data-act="openGrowth" data-id="">+ Ölçüm ekle</button><h2>Kilo (kg)</h2>`;
  const w=g.filter(x=>x.kg);
  if(w.length<2)h+=`<div class="card mute">Grafik için en az iki kilo ölçümü gerekli.</div>`;
  else{
    const W=320,H=140,p=26,xs=w.map(x=>+parseDate(x.d)),x0=Math.min(...xs),x1=Math.max(...xs)||1,ys=w.map(x=>x.kg),y0=Math.min(...ys)-.2,y1=Math.max(...ys)+.2;
    const X=v=>p+(x1===x0?0.5:(v-x0)/(x1-x0))*(W-2*p),Y=v=>H-p-((v-y0)/(y1-y0))*(H-2*p);
    const pts=w.map(x=>X(+parseDate(x.d))+','+Y(x.kg)).join(' ');
    h+=`<div class="card"><svg viewBox="0 0 ${W} ${H}" width="100%" role="img" aria-label="Kilo grafiği"><polyline points="${pts}" fill="none" stroke="var(--pri)" stroke-width="2.5" stroke-linejoin="round"/>${w.map(x=>`<circle cx="${X(+parseDate(x.d))}" cy="${Y(x.kg)}" r="4" fill="var(--pri)"/><text x="${X(+parseDate(x.d))}" y="${Y(x.kg)-9}" text-anchor="middle">${x.kg}</text>`).join('')}<text x="${p}" y="${H-6}" text-anchor="start">${fmtShort(x0)}</text><text x="${W-p}" y="${H-6}" text-anchor="end">${fmtShort(x1)}</text></svg></div>`;
  }
  h+=`<h2>Ölçümler</h2>`;
  if(!g.length)h+=`<div class="card mute">Henüz ölçüm yok.</div>`;
  for(const x of [...g].reverse())h+=`<button class="item" data-act="openGrowth" data-id="${x.id}"><span class="ic">📏</span><span class="tx">${fmtDate(parseDate(x.d))}<small>${[x.kg?x.kg+' kg':'',x.cm?x.cm+' cm boy':'',x.head?x.head+' cm baş':''].filter(Boolean).join(' · ')}</small></span><span class="mute">›</span></button>`;
  return h;
}

function vWelcome(){
  return `<div class="card hint"><b>Hoş geldin 👋</b><p>Bebeğin kayıtları, sadece sizin bildiğiniz gizli bir aile kodu ile saklanır. Eşinle aynı kodu kullanarak ikiniz de aynı kayıtları görürsünüz.</p><button class="btn pri block" data-act="createFam">Yeni aile oluştur</button></div>
  <div class="card"><b>Eşim zaten oluşturdu</b><label>Eşinin gönderdiği kodu yapıştır</label><input type="text" id="j-code" autocapitalize="off" autocomplete="off" placeholder="xxxx-xxxx-xxxx-..."><button class="btn block" style="margin-top:10px" data-act="joinFam">Katıl</button></div>`;
}
function render(){
  $('#babyName').textContent=(FID&&S.baby.name)||'Bebek Takip';
  $('#babyAge').textContent=FID?ageText()+' · '+syncStatus():'';
  let v;
  if(!FID)v=vWelcome();
  else if(!ready)v=`<div class="card mute">Kayıtlar yükleniyor…</div>`;
  else if(authFail&&!got.size)v=`<div class="card hint"><b>İnternet gerekli</b><p>İlk açılışta bir kez internete bağlanmak gerekiyor. Bağlanınca kendiliğinden devam eder.</p></div>`;
  else v=({today:vToday,med:vMed,vax:vVax,growth:vGrowth,info:vInfo})[tab]();
  if(syncError==='permission-denied')v=`<div class="card hint"><b>Veritabanı erişimi reddedildi</b><p>Firebase güvenlik kuralları yayınlanmamış ya da yanlış olabilir.</p></div>`+v;
  $('#view').innerHTML=v;
  $('#nav').innerHTML=FID?TABS.map(t=>`<button class="${t[0]===tab?'on':''}" data-act="tab" data-v="${t[0]}"><span>${t[1]}</span>${t[2]}</button>`).join(''):'';
  $('#nav').hidden=!FID;
}

/* ---------- alt sayfa (sheet) ---------- */
let sel={};
function openSheet(html){$('#sheetBody').innerHTML=html;$('#sheet').hidden=false;document.body.style.overflow='hidden';}
function closeSheet(){$('#sheet').hidden=true;document.body.style.overflow='';sel={};}
const nowHM=()=>hm(Date.now());
const delBlock=(act,id)=>`<div style="margin-top:18px"><button class="btn block ghost bad" data-act="askDel">Sil</button><div id="delConfirm" hidden><div class="two"><button class="btn danger" data-act="${act}" data-id="${id}">Evet, sil</button><button class="btn" data-act="close">Vazgeç</button></div></div></div>`;

function openBreast(){
  sel={side:'Sol'};
  openSheet(`<h3>🤱 Emzirme</h3>
  <div class="seg"><button class="on" data-act="seg" data-g="side" data-v="Sol">Sol</button><button data-act="seg" data-g="side" data-v="Sağ">Sağ</button></div>
  <button class="btn pri block" style="margin-top:14px" data-act="startTimer" data-k="breast">▶ Şimdi başlat</button>
  <div class="or">veya geçmiş kaydı elle gir</div>
  ${dtRow()}
  <label>Süre (dk)</label><input type="number" id="f-dur" inputmode="decimal" placeholder="15">
  <button class="btn block" style="margin-top:12px" data-act="saveBreast">Kaydet</button>`);
}
function openSleep(){
  openSheet(`<h3>😴 Uyku</h3>
  <button class="btn pri block" data-act="startTimer" data-k="sleep">▶ Uyudu, sayacı başlat</button>
  <div class="or">veya geçmiş kaydı elle gir</div>
  ${dtRow()}
  <label>Süre (dk)</label><input type="number" id="f-dur" inputmode="decimal" placeholder="45">
  <button class="btn block" style="margin-top:12px" data-act="saveSleep">Kaydet</button>`);
}
function openBottle(){
  sel={bk:'Anne sütü'};
  openSheet(`<h3>🍼 Biberon</h3>
  <div class="seg"><button class="on" data-act="seg" data-g="bk" data-v="Anne sütü">Anne sütü</button><button data-act="seg" data-g="bk" data-v="Mama">Mama</button></div>
  <label>Miktar (ml)</label><input type="number" id="f-ml" inputmode="numeric" placeholder="90">
  <div class="chips">${[30,60,90,120,150,180].map(v=>`<button data-act="ml" data-v="${v}">${v}</button>`).join('')}</div>
  ${dtRow()}
  <button class="btn pri block" style="margin-top:16px" data-act="saveBottle">Kaydet</button>`);
}
function openDiaper(){
  sel={};
  openSheet(`<h3>🧷 Bez</h3>
  <div class="seg"><button data-act="seg" data-g="dk" data-v="wet">💧 Islak</button><button data-act="seg" data-g="dk" data-v="dirty">💩 Kirli</button><button data-act="seg" data-g="dk" data-v="both">💧💩 İkisi</button></div>
  <div id="colorRow" hidden><label>Renk (isteğe bağlı)</label><div class="chips">${['Sarı','Yeşil','Kahverengi','Koyu'].map(c=>`<button data-act="col" data-v="${c}">${c}</button>`).join('')}</div></div>
  ${dtRow()}
  <button class="btn pri block" style="margin-top:16px" data-act="saveDiaper">Kaydet</button>`);
}
function openEv(id){
  const e=S.events.find(x=>x.id===id);if(!e)return;const i=evInfo(e);
  sel={side:e.side||'Sol',bk:e.kind||'Anne sütü',dk:e.kind,col:e.color||''};
  const seg=(g,opts,cur)=>`<div class="seg">${opts.map(([v,l])=>`<button class="${cur===v?'on':''}" data-act="seg" data-g="${g}" data-v="${v}">${l}</button>`).join('')}</div>`;
  let f='';
  if(e.type==='breast')f=seg('side',[['Sol','Sol'],['Sağ','Sağ']],e.side||'Sol')+`<label>Süre (dk)</label><input type="number" id="f-dur" inputmode="decimal" value="${e.dur?Math.round(e.dur/60000):''}">`;
  else if(e.type==='sleep')f=`<label>Süre (dk)</label><input type="number" id="f-dur" inputmode="decimal" value="${e.dur?Math.round(e.dur/60000):''}">`;
  else if(e.type==='bottle')f=seg('bk',[['Anne sütü','Anne sütü'],['Mama','Mama']],e.kind||'Anne sütü')+`<label>Miktar (ml)</label><input type="number" id="f-ml" inputmode="numeric" value="${e.ml||''}">`;
  else if(e.type==='diaper')f=seg('dk',[['wet','💧 Islak'],['dirty','💩 Kirli'],['both','💧💩 İkisi']],e.kind)+`<div id="colorRow" ${e.kind==='wet'?'hidden':''}><label>Renk (isteğe bağlı)</label><div class="chips">${['Sarı','Yeşil','Kahverengi','Koyu'].map(c=>`<button class="${e.color===c?'on':''}" data-act="col" data-v="${c}">${c}</button>`).join('')}</div></div>`;
  else f=`<div class="mute">${esc(i.s||'')}</div>`;
  openSheet(`<h3>${i.ic} ${esc(i.t)} · düzenle</h3>${dtRow(e.t)}<div style="height:12px"></div>${f}
  <button class="btn pri block" style="margin-top:16px" data-act="saveEv" data-id="${id}">Kaydet</button>${delBlock('delEv',id)}<button class="btn block" style="margin-top:10px" data-act="close">Kapat</button>`);
}
function openMed(id){
  const m=S.meds.find(x=>x.id===id)||{};sel={medId:id||''};
  openSheet(`<h3>💊 ${id?'İlacı düzenle':'İlaç / vitamin ekle'}</h3>
  <label>Ad</label><input type="text" id="m-name" value="${esc(m.name||'')}" placeholder="D vitamini">
  ${id?'':`<div class="chips">${['D vitamini','Demir','Gaz damlası','Ateş düşürücü'].map(n=>`<button data-act="medName" data-v="${n}">${n}</button>`).join('')}</div>`}
  <label>Doz</label><input type="text" id="m-dose" value="${esc(m.dose||'')}" placeholder="3 damla">
  <label>Saatler (virgülle ayır, boş bırakırsan "gerektiğinde")</label><input type="text" id="m-times" value="${esc((m.times||[]).join(', '))}" placeholder="09:00, 21:00">
  ${id?`<label><input type="checkbox" id="m-off" ${m.off?'checked':''}> İlacı geçici olarak kapat</label>`:''}
  <button class="btn pri block" style="margin-top:16px" data-act="saveMed">Kaydet</button>
  ${id?delBlock('delMed',id):''}`);
}
function openVax(id){
  const it=vaxItems().find(x=>x.id===id);if(!it)return;
  openSheet(`<h3>💉 ${esc(it.name)}</h3>${it.g.k?'<div class="warn" style="font-weight:600">Özel aşı (devlet takviminde yok, ücretli)</div>':''}<div class="mute">${it.g.l} · önerilen: ${it.due?fmtDate(it.due):'—'}</div>
  <label>Yapıldığı tarih</label><input type="date" id="v-date" value="${it.done||(it.due&&dayKey(it.due)<todayStr()?dayKey(it.due):todayStr())}">
  <button class="btn pri block" style="margin-top:16px" data-act="saveVax" data-id="${id}">${it.done?'Tarihi güncelle':'Yapıldı olarak işaretle'}</button>
  ${it.done?`<button class="btn block ghost" style="margin-top:10px" data-act="undoVax" data-id="${id}">Geri al</button>`:''}`);
}
function openGrowth(id){
  const x=S.growth.find(g=>g.id===id)||{};
  openSheet(`<h3>📏 ${id?'Ölçümü düzenle':'Ölçüm ekle'}</h3>
  <label>Tarih</label><input type="date" id="g-date" value="${x.d||todayStr()}">
  <div class="two"><div><label>Kilo (kg veya gram)</label><input type="text" id="g-kg" inputmode="decimal" value="${x.kg||''}" placeholder="4.2 ya da 4200"></div><div><label>Boy (cm)</label><input type="text" id="g-cm" inputmode="decimal" value="${x.cm||''}" placeholder="55"></div></div>
  <label>Baş çevresi (cm)</label><input type="text" id="g-head" inputmode="decimal" value="${x.head||''}" placeholder="37">
  <button class="btn pri block" style="margin-top:16px" data-act="saveGrowth" data-id="${id||''}">Kaydet</button>
  ${id?delBlock('delGrowth',id):''}`);
}
function themePref(){try{const t=localStorage.getItem('bt_theme');return t==='dark'||t==='light'?t:'auto';}catch(e){return 'auto';}}
function applyTheme(){
  const t=themePref(),r=document.documentElement;
  if(t==='auto')delete r.dataset.theme;else r.dataset.theme=t;
  const dk=t==='dark'||(t==='auto'&&matchMedia('(prefers-color-scheme: dark)').matches);
  const m=document.querySelector('meta[name=theme-color]');if(m)m.content=dk?'#16151b':'#2f8478';
}
function setTheme(t){try{if(t==='auto')localStorage.removeItem('bt_theme');else localStorage.setItem('bt_theme',t);}catch(e){}applyTheme();}
function openSettings(){
  openSheet(`<h3>⚙️ Bebek bilgileri</h3>
  <label>Adı</label><input type="text" id="s-name" value="${esc(S.baby.name)}" placeholder="Bebeğin adı">
  <label>Doğum tarihi</label><input type="date" id="s-birth" value="${esc(S.baby.birth)}">
  <button class="btn pri block" style="margin-top:16px" data-act="saveSettings">Kaydet</button>
  ${FID?`<h3 style="margin-top:26px">Aile kodu</h3>
  <div class="mute" style="font-size:14px;margin-bottom:8px">Eşin bu kodla ya da aşağıdaki davet linkiyle aynı kayıtları görür. Kodu sadece eşinle paylaş.</div>
  <div class="card" style="font-family:ui-monospace,monospace;word-break:break-all;font-size:15px">${showCode(FID)}</div>
  <div class="two"><button class="btn" data-act="copyInvite">Davet linkini kopyala</button><button class="btn" data-act="copyCode">Kodu kopyala</button></div>
  <label>Başka bir aile koduna katıl</label><input type="text" id="j-code" autocapitalize="off" autocomplete="off" placeholder="xxxx-xxxx-...">
  <button class="btn block" style="margin-top:10px" data-act="joinFam">Bu koda geç</button>
  <div style="margin-top:10px"><button class="btn block ghost bad" data-act="askDel">Bu telefondan çık</button><div id="delConfirm" hidden><div class="two"><button class="btn danger" data-act="leaveFam">Evet, çık</button><button class="btn" data-act="close">Vazgeç</button></div></div></div>`:''}
  ${FID?`<h3 style="margin-top:26px">Bağlantı bilgisi</h3>
  <div class="card" style="font-size:13px;line-height:1.6;word-break:break-all">Sürüm: ${VERSION}<br>Çevrimiçi: ${navigator.onLine?'evet':'hayır'}<br>Ana ekran uygulaması: ${(matchMedia('(display-mode: standalone)').matches||navigator.standalone)?'evet':'hayır (tarayıcı)'}<br>Giriş: ${fauth&&fauth.currentUser?'var ('+esc(fauth.currentUser.uid.slice(0,6))+')':'yok'}<br>Aile kodu uzunluğu: ${FID.length}<br>Durum: ${esc(syncStatus())}<br>Hata: ${esc(syncError||'yok')} ${esc(lastErrMsg.slice(0,120))}<br>Alınan veri: ${esc([...got].join(', ')||'-')}</div>
  <button class="btn block" data-act="reconnect">Yeniden bağlan ve gönder</button>`:''}
  <h3 style="margin-top:26px">Görünüm (bu telefon)</h3>
  <div class="three" style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px">${[['auto','Otomatik'],['light','Açık'],['dark','Koyu']].map(([v,l])=>`<button class="btn${themePref()===v?' pri':''}" data-act="theme" data-v="${v}">${l}</button>`).join('')}</div>
  <h3 style="margin-top:26px">Yedek</h3>
  <div class="mute" style="font-size:14px;margin-bottom:8px">Telefon değiştirirsen ya da emin olmak istersen verileri kopyalayıp bir yere (not, e-posta) yapıştır.</div>
  <button class="btn block" data-act="backup">Yedeği göster ve kopyala</button>
  <textarea id="s-backup" style="margin-top:10px" placeholder="Yedek metni burada görünür. Geri yüklemek için buraya yapıştır."></textarea>
  <button class="btn block" style="margin-top:10px" data-act="restore">Yapıştırılan yedeği geri yükle</button>
  <button class="btn block ghost" style="margin-top:10px" data-act="close">Kapat</button>
  <div class="foot" style="text-align:center">Bebek Takip · Sürüm ${VERSION} · ${BUILD}</div>`);
}

/* ---------- eylemler ---------- */
function copyText(t){
  const ta=document.createElement('textarea');ta.value=t;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();
  let ok=false;try{ok=document.execCommand('copy');}catch(e){}
  ta.remove();
  if(ok)return flash('Kopyalandı');
  try{navigator.clipboard.writeText(t).then(()=>flash('Kopyalandı'),()=>flash('Kopyalanamadı'));}catch(e){flash('Kopyalanamadı');}
}
const H={
  tab(el){tab=el.dataset.v;if(el.dataset.topic!==undefined)infoTopic=el.dataset.topic;render();scrollTo(0,0);},
  infoOpen(el){infoTopic=el.dataset.id;render();scrollTo(0,0);},
  openAppt(el){openAppt(el.dataset.id);},
  apptTitle(el){$('#a-title').value=el.dataset.v;},
  saveAppt(el){const dv=$('#a-date').value,tv=$('#a-time').value||'10:00';if(!dv)return flash('Tarihi gir');
    const [y,mo,d]=dv.split('-').map(Number),[h,mi]=tv.split(':').map(Number),t=new Date(y,mo-1,d,h,mi).getTime();
    const o={t,title:$('#a-title').value.trim()||'Muayene',place:$('#a-place').value.trim(),note:$('#a-note').value.trim()},id=el.dataset.id;
    if(id){const a=S.appts.find(x=>x.id===id);if(a)Object.assign(a,o);}else S.appts.push(Object.assign({id:uid()},o));
    save();closeSheet();render();flash('Kaydedildi');},
  delAppt(el){S.appts=S.appts.filter(a=>a.id!==el.dataset.id);save();closeSheet();render();},
  icsAppt(el){const a=S.appts.find(x=>x.id===el.dataset.id);if(a)saveICS('bebek-randevu.ics',apptICS([a]));},
  icsAppts(){saveICS('bebek-randevular.ics',apptICS(S.appts.filter(a=>a.t>=Date.now()-36e5)));},
  openNote(el){openNote(el.dataset.id);},
  saveNote(el){const text=$('#n-text').value.trim();if(!text)return flash('Notu yaz');
    const o={d:$('#n-date').value||todayStr(),title:$('#n-title').value.trim(),text},id=el.dataset.id;
    if(id){const n=S.notes.find(x=>x.id===id);if(n)Object.assign(n,o);}else S.notes.push(Object.assign({id:uid(),c:Date.now()},o));
    save();closeSheet();render();flash('Kaydedildi');},
  delNote(el){S.notes=S.notes.filter(n=>n.id!==el.dataset.id);save();closeSheet();render();},
  infoBack(){infoTopic='';render();scrollTo(0,0);},
  gear(){openSettings();},
  avgN(el){avgN=+el.dataset.v;render();},
  theme(el){setTheme(el.dataset.v);document.querySelectorAll('[data-act=theme]').forEach(b=>b.classList.toggle('pri',b.dataset.v===el.dataset.v));},
  close(){closeSheet();},
  askDel(el){el.hidden=true;$('#delConfirm').hidden=false;},
  seg(el){sel[el.dataset.g]=el.dataset.v;el.parentElement.querySelectorAll('button').forEach(b=>b.classList.toggle('on',b===el));
    if(el.dataset.g==='dk'){const r=$('#colorRow');r.hidden=!(sel.dk==='dirty'||sel.dk==='both');}},
  col(el){sel.col=sel.col===el.dataset.v?'':el.dataset.v;el.parentElement.querySelectorAll('button').forEach(b=>b.classList.toggle('on',b.dataset.v===sel.col));},
  ml(el){$('#f-ml').value=el.dataset.v;},
  medName(el){$('#m-name').value=el.dataset.v;},
  qBreast:openBreast,qBottle:openBottle,qDiaper:openDiaper,qSleep:openSleep,
  startTimer(el){const k=el.dataset.k;S.timers[k]={start:Date.now()};if(k==='breast')S.timers[k].side=sel.side||'Sol';save();closeSheet();render();},
  stopTimer(el){const k=el.dataset.k,t=S.timers[k];if(!t)return;const e={id:uid(),type:k,t:t.start,dur:Date.now()-t.start};if(k==='breast')e.side=t.side;S.events.push(e);delete S.timers[k];save();render();flash('Kaydedildi');},
  cancelTimer(el){delete S.timers[el.dataset.k];save();render();},
  saveBreast(){const m=num($('#f-dur').value);if(!(m>0))return flash('Süreyi gir');const t=tAt('f-time','f-date');if(t===null)return;S.events.push({id:uid(),type:'breast',t,side:sel.side||'Sol',dur:m*60000});save();closeSheet();render();flash('Kaydedildi');},
  saveSleep(){const m=num($('#f-dur').value);if(!(m>0))return flash('Süreyi gir');const t=tAt('f-time','f-date');if(t===null)return;S.events.push({id:uid(),type:'sleep',t,dur:m*60000});save();closeSheet();render();flash('Kaydedildi');},
  saveBottle(){const ml=num($('#f-ml').value);if(!(ml>0))return flash('Miktarı gir');const t=tAt('f-time','f-date');if(t===null)return;S.events.push({id:uid(),type:'bottle',t,ml:Math.round(ml),kind:sel.bk||'Anne sütü'});save();closeSheet();render();flash('Kaydedildi');},
  saveDiaper(){if(!sel.dk)return flash('Islak mı kirli mi seç');const t=tAt('f-time','f-date');if(t===null)return;const e={id:uid(),type:'diaper',t,kind:sel.dk};if(sel.col&&sel.dk!=='wet')e.color=sel.col;S.events.push(e);save();closeSheet();render();flash('Kaydedildi');},
  openEv(el){openEv(el.dataset.id);},
  saveEv(el){
    const e=S.events.find(x=>x.id===el.dataset.id);if(!e)return;const t=tAt('f-time','f-date');if(t===null)return;
    if(e.type==='breast'||e.type==='sleep'){const m=num($('#f-dur').value);if(!(m>0))return flash('Süreyi gir');e.dur=m*60000;if(e.type==='breast')e.side=sel.side||'Sol';}
    if(e.type==='bottle'){const ml=num($('#f-ml').value);if(!(ml>0))return flash('Miktarı gir');e.ml=Math.round(ml);e.kind=sel.bk||'Anne sütü';}
    if(e.type==='diaper'){if(!sel.dk)return flash('Islak mı kirli mi seç');e.kind=sel.dk;if(sel.col&&sel.dk!=='wet')e.color=sel.col;else delete e.color;}
    e.t=t;save();closeSheet();render();flash('Güncellendi');
  },
  dayGo(el){const d=parseDate(vday());d.setDate(d.getDate()+(+el.dataset.v));const k=dayKey(d.getTime());viewDay=k>=todayStr()?'':k;render();},
  dayPick(el){const v=el.value;viewDay=!v||v>=todayStr()?'':v;render();},
  delEv(el){S.events=S.events.filter(e=>e.id!==el.dataset.id);save();closeSheet();render();},
  medSlot(el){const {id,slot}=el.dataset,i=S.events.findIndex(e=>e.type==='med'&&e.medId===id&&e.slot===slot);
    if(i>=0)S.events.splice(i,1);else{const m=S.meds.find(x=>x.id===id);S.events.push({id:uid(),type:'med',medId:id,name:m.name,dose:m.dose,slot,t:Date.now()});}
    save();render();},
  medNow(el){const m=S.meds.find(x=>x.id===el.dataset.id);S.events.push({id:uid(),type:'med',medId:m.id,name:m.name,dose:m.dose,t:Date.now()});save();render();flash(m.name+' kaydedildi');},
  openMed(el){openMed(el.dataset.id);},
  saveMed(){const name=$('#m-name').value.trim();if(!name)return flash('İlaç adını gir');
    const times=$('#m-times').value.split(/[\s,;]+/).filter(t=>/^\d{1,2}:\d{2}$/.test(t)).map(t=>{const [h,m]=t.split(':');return pad(+h)+':'+m;}).sort();
    let m=S.meds.find(x=>x.id===sel.medId);if(!m){m={id:uid()};S.meds.push(m);}
    m.name=name;m.dose=$('#m-dose').value.trim();m.times=times;const off=$('#m-off');m.off=off?off.checked:false;
    save();closeSheet();render();},
  delMed(el){S.meds=S.meds.filter(m=>m.id!==el.dataset.id);save();closeSheet();render();},
  openVax(el){openVax(el.dataset.id);},
  saveVax(el){S.vax[el.dataset.id]=$('#v-date').value||todayStr();save();closeSheet();render();},
  undoVax(el){delete S.vax[el.dataset.id];save();closeSheet();render();},
  openGrowth(el){openGrowth(el.dataset.id);},
  saveGrowth(el){let kg=num($('#g-kg').value);if(kg>30)kg=kg/1000;if(!isNaN(kg))kg=Math.round(kg*1000)/1000;const cm=num($('#g-cm').value),hd=num($('#g-head').value);
    if(isNaN(kg)&&isNaN(cm)&&isNaN(hd))return flash('En az bir ölçü gir');
    let x=S.growth.find(g=>g.id===el.dataset.id);if(!x){x={id:uid()};S.growth.push(x);}
    x.d=$('#g-date').value||todayStr();x.kg=isNaN(kg)?null:kg;x.cm=isNaN(cm)?null:cm;x.head=isNaN(hd)?null:hd;save();closeSheet();render();},
  delGrowth(el){S.growth=S.growth.filter(g=>g.id!==el.dataset.id);save();closeSheet();render();},
  saveSettings(){S.baby.name=$('#s-name').value.trim();S.baby.birth=$('#s-birth').value;save();closeSheet();render();},
  backup(){const ta=$('#s-backup');ta.value=JSON.stringify(S);ta.select();
    try{navigator.clipboard.writeText(ta.value).then(()=>flash('Kopyalandı'),()=>flash('Metni elle kopyala'));}catch(e){flash('Metni elle kopyala');}},
  restore(){try{const o=JSON.parse($('#s-backup').value);if(!o||!Array.isArray(o.events))throw 0;
    const mg=(a,b)=>{const ids=new Set(a.map(x=>x.id));return a.concat((b||[]).filter(x=>x&&x.id&&!ids.has(x.id)));};
    S.events=mg(S.events,o.events);S.meds=mg(S.meds,o.meds);S.appts=mg(S.appts,o.appts);S.notes=mg(S.notes,o.notes);
    S.growth=mg(S.growth,(o.growth||[]).map(g=>Object.assign({},g,{kg:g.kg>30?Math.round(g.kg)/1000:g.kg})));
    S.vax=Object.assign({},o.vax||{},S.vax);
    if(o.baby&&(o.baby.name||o.baby.birth)&&!(S.baby.name||S.baby.birth))S.baby=o.baby;
    save();closeSheet();render();flash('Geri yüklendi');}catch(e){flash('Yedek metni okunamadı');}},
  icsVax(){saveICS('bebek-asi-takvimi.ics',vaxICS());},
  icsMed(){saveICS('bebek-ilac-takvimi.ics',medICS());},
  reconnect(){reauthN=0;syncError='';lastErrMsg='';reauth();flash('Yeniden bağlanıyor…');closeSheet();render();},
  createFam(){setFamily(newCode());location.reload();},
  joinFam(){const c=cleanCode($('#j-code').value);if(c.length<20)return flash('Kod eksik ya da hatalı');setFamily(c);location.reload();},
  leaveFam(){setFamily('');location.reload();},
  copyCode(){copyText(showCode(FID));},
  copyInvite(){copyText(location.origin+location.pathname+'#f='+FID);}
};
document.addEventListener('click',e=>{
  if(e.target.id==='sheet'){closeSheet();return;}
  const el=e.target.closest('[data-act]');if(!el)return;
  const f=H[el.dataset.act];if(f)f(el,e);
});

document.addEventListener('change',e=>{const el=e.target.closest&&e.target.closest('[data-chg]');if(el&&H[el.dataset.chg])H[el.dataset.chg](el);});

/* ---------- başlat ---------- */
(function boot(){
  applyTheme();matchMedia('(prefers-color-scheme: dark)').addEventListener&&matchMedia('(prefers-color-scheme: dark)').addEventListener('change',applyTheme);
  const m=location.hash.match(/^#f=([a-z0-9-]+)/i),h=m?cleanCode(m[1]):'';
  if(h.length>=20&&!getFamily())setFamily(h);
  if(m)history.replaceState(null,'',location.pathname+location.search);
  FID=getFamily();
  if(FID)startSync();
  render();
  if('serviceWorker' in navigator&&location.protocol.indexOf('http')===0)navigator.serviceWorker.register('sw.js').catch(()=>{});
})();
addEventListener('hashchange',()=>{const m=location.hash.match(/^#f=([a-z0-9-]+)/i);if(!m)return;const h=cleanCode(m[1]);history.replaceState(null,'',location.pathname+location.search);if(h.length>=20&&!getFamily()){setFamily(h);location.reload();}});
setInterval(()=>{document.querySelectorAll('.tm').forEach(el=>el.textContent=mmss(Date.now()-(+el.dataset.start)));},1000);
setInterval(()=>{if($('#sheet').hidden&&FID&&ready&&(tab==='today'||tab==='med'))render();},60000);
