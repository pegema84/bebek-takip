/* ---------- veri (Firebase ile ortak) ---------- */
import {initializeApp} from 'firebase/app';
import {initializeFirestore,getFirestore,persistentLocalCache,persistentMultipleTabManager,collection,doc,query,where,onSnapshot,writeBatch,getDocs,getDocsFromCache} from 'firebase/firestore';
import {getAuth,onAuthStateChanged,signInAnonymously} from 'firebase/auth';

const FB={
  apiKey:"AIzaSyDSJmbd5EUHrPWwCCLvBCmvpwFspabA6SY",
  authDomain:"bebek-takip-2ab30.firebaseapp.com",
  projectId:"bebek-takip-2ab30",
  storageBucket:"bebek-takip-2ab30.firebasestorage.app",
  messagingSenderId:"386088030461",
  appId:"1:386088030461:web:563d1e025a90fe2aab6e94"
};
const VERSION='2.8',BUILD='22 Kasım 2026';
const FAM_KEY='bebektakip.family';
const COLS=['events','meds','growth','vax','timers','meta','appts','notes','qs'];
let S={baby:{name:'',birth:''},events:[],meds:[],vax:{},growth:[],timers:{},appts:[],notes:[],qs:[]};
let FID='',fdb=null,fauth=null,ready=false,syncError='',authFail=false,unsubs=[],lastUid='',lastErrMsg='',reauthN=0;
const shadow=new Map(),got=new Set(),pend={};
/* 90 günden eski kayıtlar: canlı dinlenmez, o güne gidilince ay ay yüklenir */
const oldEv=new Map(),oldLoaded=new Set(),oldBusy=new Set();let SINCE=0;
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
  S.qs.forEach(e=>m.set('qs/'+e.id,e));
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
  if(c==='events'){
    S.events=docs.map(x=>Object.assign({},x.data,{id:x.id}));
    const ids=new Set(docs.map(x=>x.id));
    oldEv.forEach((e,id)=>{if(ids.has(id)||e.t>=SINCE)oldEv.delete(id);else{S.events.push(e);shadow.set('events/'+id,stable(e));}});
  }
  else if(c==='meds')S.meds=docs.map(x=>Object.assign({},x.data,{id:x.id}));
  else if(c==='growth')S.growth=docs.map(x=>Object.assign({},x.data,{id:x.id}));
  else if(c==='appts')S.appts=docs.map(x=>Object.assign({},x.data,{id:x.id}));
  else if(c==='notes')S.notes=docs.map(x=>Object.assign({},x.data,{id:x.id}));
  else if(c==='qs')S.qs=docs.map(x=>Object.assign({},x.data,{id:x.id}));
  else if(c==='vax'){S.vax={};docs.forEach(x=>{S.vax[x.id]=x.data.d;});}
  else if(c==='timers'){S.timers={};docs.forEach(x=>{S.timers[x.id]=x.data;});}
  else if(c==='meta'){const b=docs.find(x=>x.id==='baby');S.baby=Object.assign({name:'',birth:''},b?b.data:{});}
  pend[c]=snap.metadata.hasPendingWrites;syncError='';
  got.add(c);if(got.size===COLS.length)ready=true;
  render();
}
function relisten(){
  unsubs.forEach(f=>{try{f();}catch(e){}});unsubs=[];
  const d=new Date();d.setHours(0,0,0,0);const since=+d-90*864e5;SINCE=since;
  COLS.forEach(c=>{
    const base=collection(fdb,'families',FID,c);
    const q=c==='events'?query(base,where('t','>=',since)):base;
    unsubs.push(onSnapshot(q,{includeMetadataChanges:true},snap=>applySnap(c,snap),err=>{syncError=err&&err.code||'hata';lastErrMsg=err&&err.message||'';if(syncError==='permission-denied')onDenied();render();}));
  });
}
function fetchQ(q){
  let done=false;
  return Promise.race([getDocs(q).then(r=>{done=true;return r;}),new Promise(r=>setTimeout(r,6000)).then(()=>done?new Promise(()=>{}):getDocsFromCache(q))]).catch(()=>getDocsFromCache(q));
}
function needOld(day){
  if(!SINCE||!fdb||+parseDate(day)>=SINCE)return false;
  const mk=day.slice(0,7);if(oldLoaded.has(mk))return false;
  if(!oldBusy.has(mk)){oldBusy.add(mk);loadOld(mk);}
  return true;
}
function loadOld(mk){
  const [y,m]=mk.split('-').map(Number),a=+new Date(y,m-1,1),b=Math.min(+new Date(y,m,1),SINCE);
  fetchQ(query(collection(fdb,'families',FID,'events'),where('t','>=',a),where('t','<',b))).then(snap=>{
    const have=new Set(S.events.map(e=>e.id));
    snap.docs.forEach(d=>{if(have.has(d.id))return;const e=Object.assign({},d.data(),{id:d.id});oldEv.set(e.id,e);S.events.push(e);shadow.set('events/'+e.id,stable(e));});
    oldLoaded.add(mk);
  }).catch(()=>{setTimeout(()=>oldBusy.delete(mk),4000);}).then(()=>{oldBusy.delete(mk);render();});
}
function keepOld(e){if(SINCE&&e.t<SINCE)oldEv.set(e.id,e);else oldEv.delete(e.id);}
function addEv(e){const w=who();if(w)e.by=w;S.events.push(e);if(SINCE&&e.t<SINCE)oldEv.set(e.id,e);return e;}
function allEvents(){
  if(!fdb)return Promise.resolve(S.events.slice());
  return fetchQ(collection(fdb,'families',FID,'events')).then(snap=>{
    const m=new Map();snap.docs.forEach(d=>m.set(d.id,Object.assign({},d.data(),{id:d.id})));
    S.events.forEach(e=>m.set(e.id,e));return [...m.values()];
  }).catch(()=>S.events.slice());
}
function who(){try{return localStorage.getItem('bt_who')||'';}catch(e){return '';}}
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
/* uyku, uyanılan güne yazılır (dün 23:00 → bugün 07:00 uykusu bugünün kaydıdır) */
const eT=e=>e.type==='sleep'?e.t+(e.dur||0):e.t;
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
/* uyku: başlangıç + bitiş saati (süre otomatik; tarih = bitiş günü, başlangıç gerekirse bir gün önce) */
const sleepRow=(st,en)=>{const endT=en||null;
  return `<div><label>Tarih (uyanma günü)</label><input type="date" id="f-date" value="${endT?dayKey(endT):vday()}" max="${todayStr()}" data-chg="sleepSum"></div>
  <div class="two" style="margin-top:4px"><div><label>Uykuya dalış</label><input type="time" id="f-time" value="${st?hm(st):''}" data-chg="sleepSum"></div><div><label>Uyanış</label><input type="time" id="f-end" value="${endT?hm(endT):(vday()===todayStr()?nowHM():'')}" data-chg="sleepSum"></div></div>
  <div id="sleepSum" class="mute" style="margin-top:8px;min-height:20px"></div>`;};
function sleepCalc(){
  const dv=$('#f-date').value,sv=$('#f-time').value,ev=$('#f-end').value;
  if(!dv||!sv||!ev)return null;
  const e=parseDate(dv),s=parseDate(dv);
  const [eh,em]=ev.split(':').map(Number),[sh,sm]=sv.split(':').map(Number);
  e.setHours(eh,em,0,0);s.setHours(sh,sm,0,0);
  if(s.getTime()>=e.getTime())s.setDate(s.getDate()-1);
  return {t:s.getTime(),end:e.getTime(),dur:e.getTime()-s.getTime()};
}
function sleepSum(){
  const el=$('#sleepSum');if(!el)return;const r=sleepCalc();
  el.textContent=r?'Süre: '+dur(r.dur)+(dayKey(r.t)!==dayKey(r.end)?' (gece, bir önceki günden)':''):'';
}
function sleepAt(){
  const r=sleepCalc();if(!r){flash('Uykuya dalış ve uyanış saatini gir');return null;}
  if(r.end>Date.now()+5*60000){flash('Gelecek bir tarih/saat girilemez');return null;}
  if(r.dur>=24*3600000){flash('Süre 24 saatten az olmalı');return null;}
  return r;
}
function tFrom(id){const v=$('#'+id).value;if(!v)return Date.now();const [h,m]=v.split(':').map(Number);const d=new Date();d.setHours(h,m,0,0);if(d.getTime()>Date.now()+5*60000)d.setDate(d.getDate()-1);return d.getTime();}
function ageText(){
  if(!S.baby.birth)return 'Doğum tarihi girilmedi';
  const b=parseDate(S.baby.birth),n=new Date();
  let m=(n.getFullYear()-b.getFullYear())*12+n.getMonth()-b.getMonth(),d=n.getDate()-b.getDate();
  if(d<0){m--;d+=new Date(n.getFullYear(),n.getMonth(),0).getDate();}
  if(m<0)return 'Henüz doğmadı';
  return (m?m+' ay ':'')+d+' gün';
}
const f1=x=>(Math.round(x*10)/10).toString().replace('.',',');
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
    case 'diaper':return {ic:e.kind==='wet'?'💧':e.kind==='dirty'?'💩':'💧',t:{wet:'Çiş bezi',dirty:'Kaka bezi',both:'Çiş + kaka bezi'}[e.kind]||'Bez',s:e.color||''};
    case 'sleep':return {ic:'😴',t:'Uyku',s:e.dur?dur(e.dur):''};
    case 'med':return {ic:'💊',t:e.name||'İlaç',s:e.dose||''};
    case 'pump':return {ic:'🥛',t:'Sağım · '+e.ml+' ml',s:''};
    case 'temp':return {ic:'🌡️',t:'Ateş · '+f1(e.c)+' °C',s:e.note||''};
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
  try{
    let f=+localStorage.getItem('bt_first');if(!f){f=now;localStorage.setItem('bt_first',String(now));}
    const lb=+localStorage.getItem('bt_backup')||f,dd=Math.floor((now-lb)/864e5);
    if(dd>=30&&S.events.length)out.push({act:'gear',tab:'',ic:'💾',t:'Yedek alma zamanı (Ayarlar > Yedek)',tag:dd+' gündür yok',cls:'warn',d:20});
  }catch(e){}
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
function avgStats(n){
  const tk=todayStr(),t0=parseDate(tk),start=new Date(t0.getFullYear(),t0.getMonth(),t0.getDate()-n);
  const evs=S.events.filter(e=>e.type!=='med'&&eT(e)>=start&&eT(e)<t0);
  const days=new Set(evs.filter(e=>e.type!=='temp').map(e=>dayKey(eT(e)))).size;
  const bo=evs.filter(e=>e.type==='bottle'||e.type==='breast'),di=evs.filter(e=>e.type==='diaper'),sl=evs.filter(e=>e.type==='sleep'),pu=evs.filter(e=>e.type==='pump'),te=evs.filter(e=>e.type==='temp');
  const sum=(a,k)=>a.reduce((x,e)=>x+(e[k]||0),0);
  const fe=bo.map(e=>e.t).sort((a,b)=>a-b);
  const gaps=[];for(let i=1;i<fe.length;i++){const g=fe[i]-fe[i-1];if(g<=12*36e5)gaps.push(g);}
  const d=days||1,bt=bo.filter(e=>e.type==='bottle');
  return {days,feedN:bo.length/d,ml:sum(bt,'ml')/d,mlPer:bt.length?sum(bt,'ml')/bt.length:0,bm:bt.filter(e=>e.kind!=='Mama').length/d,fm:bt.filter(e=>e.kind==='Mama').length/d,
    wet:di.filter(e=>e.kind!=='dirty').length/d,dirty:di.filter(e=>e.kind!=='wet').length/d,
    slN:sl.length/d,slDay:sum(sl,'dur')/d,slPer:sl.length?sum(sl,'dur')/sl.length:0,
    gap:gaps.length?gaps.reduce((a,b)=>a+b,0)/gaps.length:0,puN:pu.length/d,puMl:sum(pu,'ml')/d,
    tMax:te.length?te.reduce((a,b)=>b.c>a.c?b:a):null};
}
function avgCard(){
  const a=avgStats(avgN);
  let h=`<h2>Günlük ortalama · son ${avgN} gün</h2><div class="card"><div class="row" style="margin-bottom:10px">${[1,3,7,14].map(n=>`<button class="btn sm${avgN===n?' pri':''}" style="flex:1;padding:0 4px" data-act="avgN" data-v="${n}">${n} gün</button>`).join('')}</div>`;
  if(!a.days)return h+`<div class="mute" style="font-size:14px">Tamamlanmış gün kaydı olunca ortalama burada görünür (bugün hariç).</div></div>`;
  const cell=(t,b,sm)=>`<div class="stat"><span>${t}</span><b>${b}</b>${sm?`<span>${sm}</span>`:''}</div>`;
  h+=`<div class="stats" style="grid-template-columns:1fr 1fr">`+
    cell('🍼 Biberon',f1(a.feedN)+' kez · '+Math.round(a.ml)+' ml',a.mlPer?'ort. '+Math.round(a.mlPer)+' ml / öğün':'')+
    cell('🥛 Tür',f1(a.bm)+' anne sütü · '+f1(a.fm)+' mama','günde')+
    cell('🧷 Bez',f1(a.wet)+' çiş · '+f1(a.dirty)+' kaka','günde')+
    cell('😴 Uyku',a.slN?dur(a.slDay):'—',a.slN?f1(a.slN)+' uyku · ort. '+dur(a.slPer):'')+
    (a.gap?cell('⏱ Beslenme aralığı',dur(a.gap),'ortalama'):'')+
    `</div>`;
  h+=`<div class="foot" style="margin-top:8px">Kayıt girilen ${a.days} gün üzerinden, bugün hariç. Kayıt girmediğin günler hesaba katılmaz.</div></div>`;
  return h;
}
function vToday(){
  const now=Date.now(),tk=todayStr();
  const all=[...S.events].sort((a,b)=>b.t-a.t);
  const vd=vday(),isT=vd===tk,today=all.filter(e=>dayKey(eT(e))===vd);
  let h='';
  if(!S.baby.birth)h+=`<div class="card hint"><b>Başlayalım</b><p>Bebeğin adını ve doğum tarihini gir. Aşı takvimi buna göre hesaplanır.</p><button class="btn pri" data-act="gear">Bilgileri gir</button></div>`;
  for(const a of alertList())h+=`<button class="item" data-act="${a.act||'tab'}" data-v="${a.tab}"${a.topic?` data-topic="${a.topic}"`:''} style="border-color:var(--${a.cls==='bad'?'bad':'warn'})"><span class="ic">${a.ic}</span><span class="tx">${esc(a.t)}</span><span class="tag ${a.cls}">${esc(a.tag)}</span></button>`;
  for(const k of Object.keys(S.timers)){
    const t=S.timers[k];
    const lab=k==='breast'?'🤱 Emzirme'+(t.side?' · '+esc(t.side):''):'😴 Uyku';
    h+=`<div class="card timer"><div><div class="tl">${lab}</div><div class="tm" data-start="${t.start}">${mmss(now-t.start)}</div></div><div class="row"><button class="btn pri" data-act="stopTimer" data-k="${k}">Bitir</button><button class="btn ghost sm" data-act="cancelTimer" data-k="${k}">İptal</button></div></div>`;
  }
  h+=`<div class="qgrid">
    <button class="q bottle" data-act="qBottle" style="grid-column:span 2"><span>🍼</span>Biberon</button>
    <button class="q diaper" data-act="qDiaper"><span>🧷</span>Bez</button>
    <button class="q sleep" data-act="qSleep"><span>😴</span>Uyku</button>
    <button class="q temp" data-act="qTemp" style="grid-column:span 2"><span>🌡️</span>Ateş</button></div>`;
  const lastB=all.find(e=>e.type==='bottle');
  if(isT&&lastB)h+=`<button class="btn block" style="margin:-2px 0 12px" data-act="repeatBottle">🔁 Aynısını şimdi kaydet: ${lastB.ml} ml ${esc(lastB.kind||'')}</button>`;
  const lastT=all.find(e=>e.type==='temp');
  if(lastT&&now-lastT.t<24*36e5)h+=`<button class="item" data-act="openEv" data-id="${lastT.id}" style="${lastT.c>=38?'border-color:var(--bad)':''}"><span class="ic">🌡️</span><span class="tx">Son ateş: ${f1(lastT.c)} °C<small>${ago(now-lastT.t)}${feverMedText(now)}</small></span></button>`;
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
    <div><b>${br.length+bo.length}</b><span>beslenme</span></div>
    <div><b>${ml}</b><span>ml biberon</span></div>
    <div><b>${wet}/${dirty}</b><span>çiş/kaka</span></div>
    <div><b>${slp?dur(slp).replace(' sa','s').replace(' dk','dk'):'0'}</b><span>uyku</span></div></div></div>`;
  h+=`<h2>${isT?'Bugünün':'Günün'} akışı</h2>`;
  const oldWait=needOld(vd);
  if(oldWait)h+=`<div class="card mute">Eski kayıtlar yükleniyor…</div>`;
  else if(!today.length)h+=`<div class="card mute">${isT?'Bugün henüz kayıt yok. Yukarıdaki düğmelerle başla.':'Bu günde kayıt yok.'}</div>`;
  for(const e of today){const i=evInfo(e);h+=`<button class="item" data-act="openEv" data-id="${e.id}"><span class="ic">${i.ic}</span><span class="tx">${esc(i.t)}${(i.s||e.by)?`<small>${esc([i.s,e.by].filter(Boolean).join(' · '))}</small>`:''}</span><span class="tm2">${hm(e.t)}</span></button>`;}
  h+=avgCard();
  return h;
}

const FEVER_RE=/ateş|parol|calpol|parasetamol|paracetamol|ibufen|ibuprofen|dolven|pedifen|tylol|a-ferin/i;
function lastFeverMed(){return [...S.events].filter(e=>e.type==='med'&&FEVER_RE.test(e.name||'')).sort((a,b)=>b.t-a.t)[0];}
function feverMedText(now){const m=lastFeverMed();return m&&now-m.t<48*36e5?' · son ateş düşürücü ('+m.name+'): '+ago(now-m.t):'';}
function ageMonthsAt(t){return S.baby.birth?(t-parseDate(S.baby.birth))/864e5/30.4375:null;}
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
/* ---------- aşı takvimi resmi (her seferinde güncel veriden üretilir) ---------- */
function vaxImage(){
  const W=1200,P=56,FONT='system-ui,-apple-system,"Segoe UI",Roboto,sans-serif';
  const items=vaxItems(),today=parseDate(todayStr()),dd=d=>Math.round((d-today)/864e5);
  const fd=d=>new Date(d).toLocaleDateString('tr-TR',{day:'numeric',month:'short',year:'numeric'});
  const C={bg:'#fbf7f2',card:'#ffffff',ink:'#2b2a33',mute:'#7b7885',line:'#e6ddd3',pri:'#2f8478',ok:'#2f8478',warn:'#c9741c',bad:'#cc3d49'};
  const cv=document.createElement('canvas'),x=cv.getContext('2d');
  const fnt=(sz,w)=>`${w||400} ${sz}px ${FONT}`;
  const wrap=(t,w,f)=>{x.font=f;const out=[];let cur='';for(const wd of String(t).split(' ')){const n=cur?cur+' '+wd:wd;if(x.measureText(n).width>w&&cur){out.push(cur);cur=wd;}else cur=n;}if(cur)out.push(cur);return out;};
  const rr=(X,Y,w,h,r,fill,stroke)=>{x.beginPath();x.roundRect?x.roundRect(X,Y,w,h,r):x.rect(X,Y,w,h);if(fill){x.fillStyle=fill;x.fill();}if(stroke){x.strokeStyle=stroke;x.lineWidth=2;x.stroke();}};
  const txt=(t,X,Y,f,col,al)=>{x.font=f;x.fillStyle=col;x.textAlign=al||'left';x.textBaseline='alphabetic';x.fillText(t,X,Y);};
  const nm=S.baby.name||'Bebek',tot=items.length,dn=items.filter(i=>i.done).length,late=items.filter(i=>!i.done&&dd(i.due)<0).length;
  const paint=draw=>{
    let y=0;const D=fn=>{if(draw)fn();};
    D(()=>{x.fillStyle=C.bg;x.fillRect(0,0,W,cv.height/cv.__sc);});
    // üst bant
    D(()=>{rr(0,0,W,250,0,C.pri);});
    D(()=>{txt(nm+' · Aşı Takvimi',P,112,fnt(60,800),'#fff');});
    D(()=>{txt('Doğum: '+(S.baby.birth?fmtDate(parseDate(S.baby.birth)):'—')+'  ·  Yaş: '+ageText(),P,164,fnt(30,500),'rgba(255,255,255,.92)');});
    D(()=>{txt(dn+' / '+tot+' aşı yapıldı'+(late?'  ·  '+late+' gecikmiş':''),P,214,fnt(30,700),'#fff');});
    y=250+36;
    const section=(g)=>{
      const gi=items.filter(i=>i.g===g),gdone=gi.filter(i=>i.done).length;
      const note=g.note||g.info||'';
      const nl=note?wrap(note,W-2*P-40,fnt(24,400)):[];
      const hh=70+(nl.length?nl.length*32+10:0)+gi.length*86+16;
      D(()=>{rr(P-12,y,W-2*P+24,hh,26,C.card,C.line);});
      D(()=>{txt(g.l,P+16,y+46,fnt(36,800),g.k?C.warn:C.ink);});
      D(()=>{txt(fd(gi[0].due)+(gdone===gi.length?'  ✓':''),W-P-16,y+46,fnt(30,600),gdone===gi.length?C.ok:C.mute,'right');});
      let yy=y+70;
      if(nl.length){nl.forEach((l,k)=>D(()=>txt(l,P+16,yy+28+k*32,fnt(24,400),C.mute)));yy+=nl.length*32+10;}
      gi.forEach(i=>{
        const d=dd(i.due);
        D(()=>{x.beginPath();x.arc(P+38,yy+40,17,0,7);x.fillStyle=i.done?C.ok:C.card;x.fill();x.strokeStyle=i.done?C.ok:C.line;x.lineWidth=3;x.stroke();
          if(i.done){x.beginPath();x.moveTo(P+29,yy+40);x.lineTo(P+36,yy+48);x.lineTo(P+49,yy+31);x.strokeStyle='#fff';x.lineWidth=4;x.lineCap='round';x.lineJoin='round';x.stroke();}});
        D(()=>{txt(i.name+(g.k?'  (özel)':''),P+76,yy+38,fnt(32,600),C.ink);});
        D(()=>{txt(i.done?'Yapıldığı tarih: '+fd(parseDate(i.done)):'Önerilen: '+fd(i.due),P+76,yy+70,fnt(24,400),C.mute);});
        const st=i.done?['Yapıldı',C.ok]:d<0?[-d+' gün gecikti',C.bad]:d===0?['Bugün',C.warn]:d<=14?[d+' gün kaldı',C.warn]:[d>60?Math.round(d/30)+' ay sonra':d+' gün sonra',C.mute];
        D(()=>{txt(st[0],W-P-16,yy+46,fnt(30,700),st[1],'right');});
        yy+=86;
      });
      y+=hh+22;
    };
    for(const g of VAX)section(g);
    if(VOPT.length){
      D(()=>{txt('Doktorun önerdiği özel aşılar',P,y+40,fnt(40,800),C.warn);});
      D(()=>{txt('Ulusal takvimde yok, doktorun reçetesine göre eklendi.',P,y+80,fnt(26,400),C.mute);});
      y+=110;for(const g of VOPT)section(g);
    }
    const fl=wrap('Takvim, T.C. Sağlık Bakanlığı ulusal çocukluk dönemi aşı takvimine göre hazırlandı. Tarihler yaklaşıktır, kesin randevuyu aile hekimi verir.',W-2*P,fnt(24,400));
    fl.forEach((l,k)=>D(()=>txt(l,P,y+30+k*32,fnt(24,400),C.mute)));
    y+=fl.length*32+24;
    D(()=>{txt('Güncelleme: '+fmtDate(Date.now())+'  ·  Bebek Takip',P,y+24,fnt(24,600),C.mute);});
    return y+70;
  };
  // 1. geçiş: yükseklik ölçümü, 2. geçiş: çizim
  cv.width=W;cv.height=100;cv.__sc=1;
  const H0=paint(false);
  const sc=Math.min(2,Math.sqrt(15e6/(W*H0)));
  cv.width=Math.round(W*sc);cv.height=Math.round(H0*sc);cv.__sc=sc;
  x.scale(sc,sc);
  paint(true);
  return cv;
}
async function vaxSave(share){
  if(!S.baby.birth)return flash('Önce doğum tarihini gir');
  let cv;try{cv=vaxImage();}catch(err){return flash('Resim hazırlanamadı');}
  const blob=await new Promise(r=>cv.toBlob(r,'image/png'));if(!blob)return flash('Resim hazırlanamadı');
  const name=(S.baby.name||'bebek').toLowerCase().replace(/[^a-z0-9çğıöşü]+/gi,'-')+'-asi-takvimi-'+todayStr()+'.png';
  const file=new File([blob],name,{type:'image/png'});
  const ios=/iP(hone|ad|od)/.test(navigator.userAgent)||(navigator.platform==='MacIntel'&&navigator.maxTouchPoints>1);
  if((share||ios)&&navigator.canShare&&navigator.canShare({files:[file]})){
    try{await navigator.share({files:[file],title:(S.baby.name||'Bebek')+' aşı takvimi'});}catch(err){}return;}
  if(share)return flash('Bu cihaz paylaşımı desteklemiyor, İndir\'i kullan');
  const url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download=name;document.body.appendChild(a);a.click();a.remove();
  setTimeout(()=>URL.revokeObjectURL(url),30000);flash('Resim indirildi');
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
  h+=`<div class="two" style="margin-top:10px"><button class="btn" data-act="vaxImg">🖼️ Resim olarak indir</button><button class="btn" data-act="vaxShare">📤 Resmi paylaş</button></div>`;
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

/* Sesler ve ninni: tarayıcıda üretilir (Web Audio), dosya ve internet gerekmez */
const SND=[
 {id:'white',ic:'🌫️',t:'Beyaz gürültü',k:'n'},
 {id:'pink',ic:'🌸',t:'Pembe gürültü (yumuşak)',k:'n'},
 {id:'brown',ic:'🌊',t:'Kahverengi gürültü (derin)',k:'n'},
 {id:'rain',ic:'🌧️',t:'Yağmur',k:'n'},
 {id:'fan',ic:'🌀',t:'Fan sesi',k:'n'},
 {id:'womb',ic:'🤰',t:'Rahim sesi (kalp atışı)',k:'n'},
 {id:'car',ic:'🚗',t:'Araç içi sesi',k:'n'},
 {id:'brahms',ic:'🎵',t:'Brahms ninnisi (müzik kutusu, sözsüz)',k:'m'}
];
const MEL={brahms:{bpm:66,n:[[64,.5],[64,.5],[67,2],[64,.5],[64,.5],[67,2],[64,.5],[67,.5],[72,1],[71,2],[69,.5],[69,.5],[67,2],[62,.5],[64,.5],[65,2],[62,.5],[62,.5],[64,.5],[65,.5],[62,.5],[65,.5],[71,1],[69,2],[67,.5],[71,.5],[72,3],[0,1.5]]}};
let AC=null,SG=null,SRC=null,SEL=null,sndId='',sndVol=30,sndMin=30,sndTm=null,sndAt=0;const sndBuf={};
const sndGain=v=>Math.pow(v/100,2)*0.7;
function sndMake(id){
  const sr=AC.sampleRate,m=SND.find(x=>x.id===id);let N,d;
  if(m.k==='m'){
    const me=MEL[id],bt=60/me.bpm,tot=me.n.reduce((a,x)=>a+x[1],0)*bt;N=Math.ceil(tot*sr);const b=AC.createBuffer(1,N,sr);d=b.getChannelData(0);let t=0;
    for(const [mi,du] of me.n){if(mi){const f=440*Math.pow(2,(mi-69)/12),i0=Math.floor(t*sr),len=Math.min(Math.floor(2.2*sr),N-i0);
      for(let i=0;i<len;i++){const x=i/sr,e=Math.exp(-x*2.6);d[i0+i]+=0.32*e*(Math.sin(6.2832*f*x)+0.35*Math.sin(6.2832*f*2*x)*Math.exp(-x*3)+0.12*Math.sin(6.2832*f*3.01*x)*Math.exp(-x*6))*Math.min(1,x*400);}}
      t+=du*bt;}
    return b;
  }
  N=Math.floor(sr*8);const b=AC.createBuffer(1,N,sr);d=b.getChannelData(0);
  const TP=6.2832,rnd=()=>Math.random()*2-1,fc=f=>1-Math.exp(-TP*f/sr);
  /* tek kutuplu alçak geçiren süzgeç */
  const LP=f=>{const c=fc(f);let y=0;return x=>(y+=c*(x-y));};
  if(id==='white'){for(let i=0;i<N;i++)d[i]=rnd()*0.5;}
  else if(id==='pink'){let b0=0,b1=0,b2=0,b3=0,b4=0,b5=0,b6=0;for(let i=0;i<N;i++){const w=rnd();b0=.99886*b0+w*.0555179;b1=.99332*b1+w*.0750759;b2=.969*b2+w*.153852;b3=.8665*b3+w*.3104856;b4=.55*b4+w*.5329522;b5=-.7616*b5-w*.016898;d[i]=(b0+b1+b2+b3+b4+b5+b6+w*.5362)*0.11;b6=w*.115926;}}
  else if(id==='brown'){let last=0;for(let i=0;i<N;i++){last=(last+0.02*rnd())/1.02;d[i]=last*3.2;}}
  else if(id==='rain'){
    /* zemin: 700 Hz-7 kHz arası hışırtı + sık, rastgele küçük damlalar + uzaktan hafif gürültü */
    const l1=LP(4500),l1b=LP(4500),l2=LP(900),l2b=LP(900),l3=LP(250);
    for(let i=0;i<N;i++){const w=rnd();const hi=l1b(l1(w))-l2b(l2(w));d[i]=hi*0.9+l3(w)*0.35;}
    const nd=Math.floor(N/sr*55);
    for(let k=0;k<nd;k++){const i0=Math.floor(Math.random()*N),len=Math.floor(sr*(0.004+Math.random()*0.012)),a=(0.15+Math.random()*0.5)*(Math.random()<0.15?1.6:1),f=1200+Math.random()*2800,tone=Math.random()<0.4;
      for(let i=0;i<len;i++){const x=i/len,e=Math.exp(-x*6)*a,v=tone?Math.sin(TP*f*i/sr):rnd();d[(i0+i)%N]+=v*e*0.5;}}
  }
  else if(id==='fan'){
    /* motor + kanat sesi: sabit tonlar ve hafif dönüş titreşimi, üstüne yumuşak hava sesi */
    const l1=LP(1500),l2=LP(1500),l4=LP(200);
    for(let i=0;i<N;i++){const t=i/sr,am=1+0.12*Math.sin(TP*3*t);
      d[i]=(l2(l1(rnd()))*1.8+l4(rnd())*1.5)*am+(0.10*Math.sin(TP*60*t)+0.14*Math.sin(TP*120*t)+0.07*Math.sin(TP*240*t)+0.04*Math.sin(TP*360*t))*am;}
  }
  else if(id==='car'){
    /* araç içi: derin motor uğultusu + yol ve rüzgâr gürültüsü */
    const a1=LP(180),a2=LP(180),r1=LP(1600),r2=LP(1600);
    for(let i=0;i<N;i++){const t=i/sr,w=rnd(),wob=1+0.15*Math.sin(TP*0.5*t)+0.06*Math.sin(TP*1.25*t);
      d[i]=(a2(a1(w))*7*wob)+r2(r1(rnd()))*1.1+(0.07*Math.sin(TP*38*t)+0.045*Math.sin(TP*76*t+1)+0.025*Math.sin(TP*114*t+2))*wob;}
  }
  else if(id==='womb'){
    /* rahim: boğuk, yavaşça dalgalanan kan akışı uğultusu + belirgin bebek kalp atışı (dakikada 150) */
    const l1=LP(220),l2=LP(220),l3=LP(90);const per=0.4;
    for(let i=0;i<N;i++){const t=i/sr,ph=t%per,w=rnd();
      const flow=1+0.35*Math.sin(TP*0.5*t)+0.2*Math.sin(TP*1.25*t+1);
      let v=l2(l1(w))*5.5*flow+l3(rnd())*2.5;
      const th=(Math.sin(TP*70*ph)*Math.exp(-ph*38))*0.9+(ph>0.11?Math.sin(TP*58*(ph-0.11))*Math.exp(-(ph-0.11)*45)*0.65:0);
      d[i]=v*0.55+th*0.9;}
  }
  if(id!=='womb'&&id!=='white'){const M=Math.floor(sr*0.15);for(let i=0;i<M;i++){const a=i/M;d[N-M+i]=d[N-M+i]*(1-a)+d[i]*a;}}
  {let r=0,pk=0;for(let i=0;i<N;i++){r+=d[i]*d[i];const x=Math.abs(d[i]);if(x>pk)pk=x;}r=Math.sqrt(r/N)||1;let g=0.17/r;if(pk*g>0.9)g=0.9/pk;for(let i=0;i<N;i++)d[i]*=g;}
  return b;
}
function sndStart(id){
  const C=window.AudioContext||window.webkitAudioContext;if(!C)return flash('Bu telefon ses üretemiyor');
  if(!AC)AC=new C();AC.resume&&AC.resume();
  sndHalt();
  if(!sndBuf[id])sndBuf[id]=sndMake(id);
  SG=AC.createGain();SG.gain.value=sndGain(sndVol);
  SRC=AC.createBufferSource();SRC.buffer=sndBuf[id];SRC.loop=true;
  const m=SND.find(x=>x.id===id);if(m.k!=='m'&&id!=='womb'&&id!=='white')SRC.loopStart=0.15;
  SRC.connect(SG);
  try{const dst=AC.createMediaStreamDestination();SG.connect(dst);SEL=new Audio();SEL.srcObject=dst.stream;SEL.play().catch(()=>{try{SG.connect(AC.destination);}catch(e){}});}catch(e){SEL=null;SG.connect(AC.destination);}
  SRC.start();sndId=id;sndSetTimer();
  try{if(navigator.mediaSession){navigator.mediaSession.metadata=new MediaMetadata({title:m.t,artist:'Bebek Takip'});navigator.mediaSession.setActionHandler('pause',()=>{sndStop();render();});navigator.mediaSession.setActionHandler('stop',()=>{sndStop();render();});}}catch(e){}
}
function sndHalt(){clearTimeout(sndTm);sndTm=null;try{SRC&&SRC.stop();}catch(e){}try{SG&&SG.disconnect();}catch(e){}if(SEL){try{SEL.pause();SEL.srcObject=null;}catch(e){}SEL=null;}SRC=null;SG=null;}
function sndStop(){sndHalt();sndId='';sndAt=0;}
function sndSetTimer(){
  clearTimeout(sndTm);sndTm=null;sndAt=0;if(!sndMin||!sndId)return;
  sndAt=Date.now()+sndMin*60000;
  sndTm=setTimeout(()=>{if(SG&&AC){SG.gain.setValueAtTime(SG.gain.value,AC.currentTime);SG.gain.linearRampToValueAtTime(0,AC.currentTime+8);}setTimeout(()=>{sndStop();if(tab==='info')render();},8500);},sndMin*60000);
}
function vSnd(){
  let h=`<div class="card hint"><p style="margin:0">Sesi kısık tut, telefonu bebeğin başından uzağa koy ve uzun süre yüksek sesle dinletme.</p></div>`;
  SND.forEach(x=>{const on=sndId===x.id;h+=`<button class="item" data-act="sndPlay" data-id="${x.id}" style="${on?'border:2px solid var(--pri)':''}"><span class="ic">${x.ic}</span><span class="tx">${esc(x.t)}<small>${on?'Çalıyor · durdurmak için dokun':'Başlatmak için dokun'}</small></span><span style="font-size:22px">${on?'⏸':'▶'}</span></button>`;});
  h+=`<label style="margin-top:12px">Ses düzeyi</label><input type="range" min="5" max="100" value="${sndVol}" data-inp="sndVol" style="width:100%">`;
  h+=`<label style="margin-top:12px">Kendiliğinden kapanma</label><div class="chips">${[[0,'Kapalı'],[15,'15 dk'],[30,'30 dk'],[60,'1 saat']].map(([m,l])=>`<button class="${sndMin===m?'on':''}" data-act="sndTimer" data-v="${m}">${l}</button>`).join('')}</div>`;
  if(sndId&&sndAt)h+=`<div class="mute" style="margin-top:8px">Kapanış: ${hm(sndAt)}</div>`;
  return h;
}
const INFO=[
 {id:'doc',ic:'📋',t:'Doktor için özet',sub:()=>{return 'Ortalamalar, ölçümler, ateş, aşılar';},body:()=>vDoc()},
 {id:'appt',ic:'🩺',t:'Muayene randevuları',sub:()=>{const n=S.appts.filter(a=>a.t>=Date.now()-36e5).sort((a,b)=>a.t-b.t)[0];return n?'Sıradaki: '+fmtShort(n.t)+' '+hm(n.t):'Randevu ekle, takvime aktar';},body:vAppt},
 {id:'qs',ic:'❓',t:'Doktora sorulacaklar',sub:()=>{const n=S.qs.filter(q=>!q.done).length;return n?n+' bekleyen soru':'Aklına gelenleri yaz'},body:()=>vQs()},
 {id:'notes',ic:'📝',t:'Doktor notları',sub:()=>S.notes.length?S.notes.length+' not':'Doktorun tavsiyeleri',body:vNotes},
 {id:'snd',ic:'🎶',t:'Sesler ve ninniler',sub:()=>sndId?'Çalıyor: '+SND.find(x=>x.id===sndId).t:'Beyaz gürültü, yağmur, ninni',body:()=>vSnd()},
 {id:'miles',ic:'🧸',t:'Bebekler bunları ne zaman yapar?',sub:'Gelişim basamakları, ay ay',body:bodyMiles}
];
function vInfo(){
  const t=INFO.find(x=>x.id===infoTopic);
  if(t)return `<button class="btn sm ghost" data-act="infoBack" style="margin-bottom:6px;background:var(--pri);color:var(--priInk);border-color:var(--pri)">← Bilgi menüsü</button><h2 style="margin-top:8px">${t.ic} ${esc(t.t)}</h2>`+t.body();
  let h=`<h2>📖 Bilgi menüsü</h2>`;
  for(const x of INFO)h+=`<button class="item" data-act="infoOpen" data-id="${x.id}"><span class="ic">${x.ic}</span><span class="tx">${esc(x.t)}<small>${esc(typeof x.sub==='function'?x.sub():x.sub)}</small></span><span class="tag mute">›</span></button>`;
  return h;
}
/* DSÖ (WHO) büyüme standartları, 0-36 ay, LMS değerleri [L,M,S] */
const WHO={"wm":[[0.3487,3.3464,0.14602],[0.2297,4.4709,0.13395],[0.197,5.5675,0.12385],[0.1738,6.3762,0.11727],[0.1553,7.0023,0.11316],[0.1395,7.5105,0.1108],[0.1257,7.934,0.10958],[0.1134,8.297,0.10902],[0.1021,8.6151,0.10882],[0.0917,8.9014,0.10881],[0.082,9.1649,0.10891],[0.073,9.4122,0.10906],[0.0644,9.6479,0.10925],[0.0563,9.8749,0.10949],[0.0487,10.0953,0.10976],[0.0413,10.3108,0.11007],[0.0343,10.5228,0.11041],[0.0275,10.7319,0.11079],[0.0211,10.9385,0.11119],[0.0148,11.143,0.11164],[0.0087,11.3462,0.11211],[0.0029,11.5486,0.11261],[-0.0028,11.7504,0.11314],[-0.0083,11.9514,0.11369],[-0.0137,12.1515,0.11426],[-0.0189,12.3502,0.11485],[-0.024,12.5466,0.11544],[-0.0289,12.7401,0.11604],[-0.0337,12.9303,0.11664],[-0.0385,13.1169,0.11723],[-0.0431,13.3,0.11781],[-0.0476,13.4798,0.11839],[-0.052,13.6567,0.11896],[-0.0564,13.8309,0.11953],[-0.0606,14.0031,0.12008],[-0.0648,14.1736,0.12062],[-0.0689,14.3429,0.12116]],"wf":[[0.3809,3.2322,0.14171],[0.1714,4.1873,0.13724],[0.0962,5.1282,0.13],[0.0402,5.8458,0.12619],[-0.005,6.4237,0.12402],[-0.043,6.8985,0.12274],[-0.0756,7.297,0.12204],[-0.1039,7.6422,0.12178],[-0.1288,7.9487,0.12181],[-0.1507,8.2254,0.12199],[-0.17,8.48,0.12223],[-0.1872,8.7192,0.12247],[-0.2024,8.9481,0.12268],[-0.2158,9.1699,0.12283],[-0.2278,9.387,0.12294],[-0.2384,9.6008,0.12299],[-0.2478,9.8124,0.12303],[-0.2562,10.0226,0.12306],[-0.2637,10.2315,0.12309],[-0.2703,10.4393,0.12315],[-0.2762,10.6464,0.12323],[-0.2815,10.8534,0.12335],[-0.2862,11.0608,0.1235],[-0.2903,11.2688,0.12369],[-0.2941,11.4775,0.1239],[-0.2975,11.6864,0.12414],[-0.3005,11.8947,0.12441],[-0.3032,12.1015,0.12472],[-0.3057,12.3059,0.12506],[-0.308,12.5073,0.12545],[-0.3101,12.7055,0.12587],[-0.312,12.9006,0.12633],[-0.3138,13.093,0.12683],[-0.3155,13.2837,0.12737],[-0.3171,13.4731,0.12794],[-0.3186,13.6618,0.12855],[-0.3201,13.8503,0.12919]],"lm":[[1.0,49.8842,0.03795],[1.0,54.7244,0.03557],[1.0,58.4249,0.03424],[1.0,61.4292,0.03328],[1.0,63.886,0.03257],[1.0,65.9026,0.03204],[1.0,67.6236,0.03165],[1.0,69.1645,0.03139],[1.0,70.5994,0.03124],[1.0,71.9687,0.03117],[1.0,73.2812,0.03118],[1.0,74.5388,0.03125],[1.0,75.7488,0.03137],[1.0,76.9186,0.03154],[1.0,78.0497,0.03174],[1.0,79.1458,0.03197],[1.0,80.2113,0.03222],[1.0,81.2487,0.0325],[1.0,82.2587,0.03279],[1.0,83.2418,0.0331],[1.0,84.1996,0.03342],[1.0,85.1348,0.03376],[1.0,86.0477,0.0341],[1.0,86.941,0.03445],[1.0,87.8161,0.03479],[1.0,87.972,0.03542],[1.0,88.8065,0.03576],[1.0,89.6197,0.0361],[1.0,90.412,0.03642],[1.0,91.1828,0.03674],[1.0,91.9327,0.03704],[1.0,92.6631,0.03733],[1.0,93.3753,0.03761],[1.0,94.0711,0.03787],[1.0,94.7532,0.03812],[1.0,95.4236,0.03836],[1.0,96.0835,0.03858]],"lf":[[1.0,49.1477,0.0379],[1.0,53.6872,0.0364],[1.0,57.0673,0.03568],[1.0,59.8029,0.0352],[1.0,62.0899,0.03486],[1.0,64.0301,0.03463],[1.0,65.7311,0.03448],[1.0,67.2873,0.03441],[1.0,68.7498,0.0344],[1.0,70.1435,0.03444],[1.0,71.4818,0.03452],[1.0,72.771,0.03464],[1.0,74.015,0.03479],[1.0,75.2176,0.03496],[1.0,76.3817,0.03514],[1.0,77.5099,0.03534],[1.0,78.6055,0.03555],[1.0,79.671,0.03576],[1.0,80.7079,0.03598],[1.0,81.7182,0.0362],[1.0,82.7036,0.03643],[1.0,83.6654,0.03666],[1.0,84.604,0.03688],[1.0,85.5202,0.03711],[1.0,86.4153,0.03734],[1.0,86.5904,0.03786],[1.0,87.4462,0.03808],[1.0,88.283,0.0383],[1.0,89.1004,0.03851],[1.0,89.8991,0.03872],[1.0,90.6797,0.03893],[1.0,91.443,0.03913],[1.0,92.1906,0.03933],[1.0,92.9239,0.03952],[1.0,93.6444,0.03971],[1.0,94.3533,0.03989],[1.0,95.0515,0.04006]],"hm":[[1.0,34.4618,0.03686],[1.0,37.2759,0.03133],[1.0,39.1285,0.02997],[1.0,40.5135,0.02918],[1.0,41.6317,0.02868],[1.0,42.5576,0.02837],[1.0,43.3306,0.02817],[1.0,43.9803,0.02804],[1.0,44.53,0.02796],[1.0,44.9998,0.02792],[1.0,45.4051,0.0279],[1.0,45.7573,0.02789],[1.0,46.0661,0.02789],[1.0,46.3395,0.02789],[1.0,46.5844,0.02791],[1.0,46.806,0.02792],[1.0,47.0088,0.02795],[1.0,47.1962,0.02797],[1.0,47.3711,0.028],[1.0,47.5357,0.02803],[1.0,47.6919,0.02806],[1.0,47.8408,0.0281],[1.0,47.9833,0.02813],[1.0,48.1201,0.02817],[1.0,48.2515,0.02821],[1.0,48.3777,0.02825],[1.0,48.4989,0.0283],[1.0,48.6151,0.02834],[1.0,48.7264,0.02838],[1.0,48.8331,0.02842],[1.0,48.9351,0.02847],[1.0,49.0327,0.02851],[1.0,49.126,0.02855],[1.0,49.2153,0.02859],[1.0,49.3007,0.02863],[1.0,49.3826,0.02867],[1.0,49.4612,0.02871]],"hf":[[1.0,33.8787,0.03496],[1.0,36.5463,0.0321],[1.0,38.2521,0.03168],[1.0,39.5328,0.0314],[1.0,40.5817,0.03119],[1.0,41.459,0.03102],[1.0,42.1995,0.03087],[1.0,42.829,0.03075],[1.0,43.3671,0.03063],[1.0,43.83,0.03053],[1.0,44.2319,0.03044],[1.0,44.5844,0.03035],[1.0,44.8965,0.03027],[1.0,45.1752,0.03019],[1.0,45.4265,0.03012],[1.0,45.6551,0.03006],[1.0,45.865,0.02999],[1.0,46.0598,0.02993],[1.0,46.2424,0.02987],[1.0,46.4152,0.02982],[1.0,46.5801,0.02977],[1.0,46.7384,0.02972],[1.0,46.8913,0.02967],[1.0,47.0391,0.02962],[1.0,47.1822,0.02957],[1.0,47.3204,0.02953],[1.0,47.4536,0.02949],[1.0,47.5817,0.02945],[1.0,47.7045,0.02941],[1.0,47.8219,0.02937],[1.0,47.934,0.02933],[1.0,48.041,0.02929],[1.0,48.1432,0.02926],[1.0,48.2408,0.02922],[1.0,48.3343,0.02919],[1.0,48.4239,0.02915],[1.0,48.5099,0.02912]]};
let growthTab='w';
const GK={w:{k:'kg',t:'Kilo',u:'kg',dec:1,lo:'hafif',hi:'ağır'},l:{k:'cm',t:'Boy',u:'cm',dec:1,lo:'kısa',hi:'uzun'},h:{k:'head',t:'Baş çevresi',u:'cm',dec:1,lo:'küçük',hi:'büyük'}};
const PZ=[[-1.881,'3'],[-1.036,'15'],[0,'50'],[1.036,'85'],[1.881,'97']];
function lmsAt(key,m){const a=WHO[key+(S.baby.sex==='f'?'f':'m')];m=Math.max(0,Math.min(36,m));const i=Math.min(35,Math.floor(m)),f=m-i,p=a[i],q=a[i+1];return [p[0]+(q[0]-p[0])*f,p[1]+(q[1]-p[1])*f,p[2]+(q[2]-p[2])*f];}
const lmsVal=([L,M,S_],z)=>Math.abs(L)<1e-6?M*Math.exp(S_*z):M*Math.pow(1+L*S_*z,1/L);
const lmsZ=([L,M,S_],x)=>Math.abs(L)<1e-6?Math.log(x/M)/S_:(Math.pow(x/M,L)-1)/(L*S_);
function normCdf(z){const t=1/(1+0.2316419*Math.abs(z)),d=0.3989423*Math.exp(-z*z/2);const p=d*t*(0.3193815+t*(-0.3565638+t*(1.781478+t*(-1.821256+t*1.330274))));return z>0?1-p:p;}
function pctOf(key,ageM,x){return normCdf(lmsZ(lmsAt(key,ageM),x))*100;}
function pctText(p){return p<1?'100 yaşıt bebekten 1 tanesinden bile küçük':p>99?'100 yaşıt bebekten 99 tanesinden büyük':'100 yaşıt bebekten yaklaşık '+Math.round(p)+' tanesinden büyük';}
function growthPts(key){
  const k=GK[key].k,b=S.baby.birth?+parseDate(S.baby.birth):null;
  return S.growth.filter(x=>x[k]&&x.d).map(x=>({d:x.d,v:+x[k],m:b===null?null:(+parseDate(x.d)-b)/864e5/30.4375})).filter(x=>x.m===null||(x.m>=-0.1&&x.m<=36)).sort((a,b2)=>a.d.localeCompare(b2.d));
}
function growthChart(key){
  const g=GK[key],pts=growthPts(key),nowM=Math.max(0,ageMonthsAt(Date.now()));
  const xmax=Math.min(36,Math.max(6,Math.ceil(Math.max(nowM,...pts.map(p=>p.m))+2)));
  const W=340,H=240,pl=30,pr=26,pt=10,pb=36;
  let y0=lmsVal(lmsAt(key,0),-1.881),y1=lmsVal(lmsAt(key,xmax),1.881);
  pts.forEach(p=>{y0=Math.min(y0,p.v);y1=Math.max(y1,p.v);});
  const padY=(y1-y0)*0.06;y0-=padY;y1+=padY;
  const X=m=>pl+m/xmax*(W-pl-pr),Y=v=>H-pb-(v-y0)/(y1-y0)*(H-pt-pb);
  const curve=z=>{let d='';for(let m=0;m<=xmax+1e-9;m+=0.5)d+=(d?'L':'M')+X(m).toFixed(1)+' '+Y(lmsVal(lmsAt(key,m),z)).toFixed(1);return d;};
  let band='';for(let m=0;m<=xmax+1e-9;m+=0.5)band+=(band?'L':'M')+X(m).toFixed(1)+' '+Y(lmsVal(lmsAt(key,m),1.881)).toFixed(1);
  for(let m=xmax;m>=-1e-9;m-=0.5)band+='L'+X(m).toFixed(1)+' '+Y(lmsVal(lmsAt(key,m),-1.881)).toFixed(1);
  const xs=xmax<=12?1:xmax<=24?2:6,span=y1-y0,ys=[0.5,1,2,5,10].find(st=>span/st<=6)||10;
  let grid='';
  for(let m=0;m<=xmax;m+=xs)grid+=`<line x1="${X(m)}" y1="${pt}" x2="${X(m)}" y2="${H-pb}" stroke="var(--line)" stroke-width="1"/><text x="${X(m)}" y="${H-pb+13}" text-anchor="middle">${m}</text>`;
  for(let v=Math.ceil(y0/ys)*ys;v<=y1;v+=ys)grid+=`<line x1="${pl}" y1="${Y(v)}" x2="${W-pr}" y2="${Y(v)}" stroke="var(--line)" stroke-width="1"/><text x="${pl-4}" y="${Y(v)+3}" text-anchor="end">${String(Math.round(v*10)/10).replace('.',',')}</text>`;
  const lines=PZ.map(([z,l])=>`<path d="${curve(z)}" fill="none" stroke="var(--pri)" stroke-width="${z===0?2:1}" ${z===0?'':'stroke-dasharray="4 3" opacity=".75"'}/><text x="${W-pr+3}" y="${Y(lmsVal(lmsAt(key,xmax),z))+3}">${l}</text>`).join('');
  const baby=(pts.length>1?`<polyline points="${pts.map(p=>X(p.m).toFixed(1)+','+Y(p.v).toFixed(1)).join(' ')}" fill="none" stroke="var(--med)" stroke-width="2.5" stroke-linejoin="round"/>`:'')+pts.map(p=>`<circle cx="${X(p.m).toFixed(1)}" cy="${Y(p.v).toFixed(1)}" r="4.5" fill="var(--med)" stroke="var(--card)" stroke-width="1.5"/>`).join('');
  return `<div class="card"><svg viewBox="0 0 ${W} ${H}" width="100%" role="img" aria-label="${g.t} büyüme eğrisi"><path d="${band}Z" fill="var(--pri)" opacity=".08"/>${grid}${lines}${baby}<text x="${(pl+W-pr)/2}" y="${H-5}" text-anchor="middle">yaş (ay)</text></svg>
  <div class="mute" style="font-size:12px;margin-top:4px"><span style="color:var(--med);font-weight:700">●</span> ${esc(S.baby.name||'Bebek')} &nbsp; <span style="color:var(--pri);font-weight:700">—</span> yaşıtların ortası &nbsp; <span style="color:var(--pri)">- -</span> 100 yaşıttan 3, 15, 85 ve 97 tanesinin sınırı</div></div>`;
}
function lastPct(key){
  const pts=growthPts(key),l=pts[pts.length-1];if(!l||l.m===null)return null;
  return {d:l.d,v:l.v,p:pctOf(key,l.m,l.v)};
}
function vGrowth(){
  const g=[...S.growth].sort((a,b)=>a.d.localeCompare(b.d)),gk=GK[growthTab];
  let h=`<button class="btn block pri" data-act="openGrowth" data-id="">+ Ölçüm ekle</button>
  <div class="seg" style="margin-top:14px">${Object.keys(GK).map(k=>`<button class="${growthTab===k?'on':''}" data-act="growthTab" data-v="${k}">${GK[k].t}</button>`).join('')}</div>
  <h2>${gk.t} (${gk.u}) · ${S.baby.sex==='f'?'kız':'erkek'} bebek eğrisi</h2>`;
  if(!S.baby.birth)h+=`<div class="card mute">Büyüme eğrisi için Ayarlar'dan doğum tarihini gir.</div>`;
  else{
    h+=growthChart(growthTab);
    const l=lastPct(growthTab);
    if(l){const r=Math.round(l.p);h+=`<div class="card"><b>Son ölçüm: ${f1(l.v)} ${gk.u}</b> <span class="mute">(${fmtDate(parseDate(l.d))})</span><div style="margin-top:4px">${pctText(l.p)}</div></div>`;}
    else h+=`<div class="card mute">Henüz ${gk.t.toLowerCase()} ölçümü yok. Ölçüm ekleyince eğrinin üstünde görünür.</div>`;
    h+=`<div class="foot">Eğriler Dünya Sağlık Örgütü büyüme standartlarıdır. 3 ile 97 arasındaki değerler beklenen aralık kabul edilir. Tek bir ölçümden çok, ölçümlerin zaman içinde kendi eğrisini izlemesi önemlidir. Değerlendirmeyi doktor yapar.</div>`;
  }
  h+=`<h2>Ölçümler</h2>`;
  if(!g.length)h+=`<div class="card mute">Henüz ölçüm yok.</div>`;
  for(const x of [...g].reverse())h+=`<button class="item" data-act="openGrowth" data-id="${x.id}"><span class="ic">📏</span><span class="tx">${fmtDate(parseDate(x.d))}<small>${[x.kg?x.kg+' kg':'',x.cm?x.cm+' cm boy':'',x.head?x.head+' cm baş':''].filter(Boolean).join(' · ')}</small></span><span class="mute">›</span></button>`;
  return h;
}

/* ---------- doktor özeti ---------- */
function docData(){
  const now=Date.now(),a7=avgStats(7),a14=avgStats(14);
  const vi=S.baby.birth?vaxItems():[],done=vi.filter(i=>i.done).sort((a,b)=>String(b.done).localeCompare(String(a.done))),next=vi.filter(i=>!i.done&&i.due&&!i.g.note).sort((a,b)=>a.due-b.due);
  const nd=next[0]?next.filter(i=>+i.due===+next[0].due):[];
  const temps=S.events.filter(e=>e.type==='temp'&&now-e.t<14*864e5).sort((a,b)=>b.c-a.c);
  const appt=S.appts.filter(x=>x.t>=now-36e5).sort((a,b)=>a.t-b.t)[0];
  return {a7,a14,done,nd,temps,appt,gw:lastPct('w'),gl:lastPct('l'),gh:lastPct('h'),meds:S.meds.filter(m=>!m.off),qs:[...S.qs].sort((a,b)=>(a.done-b.done)||(a.c-b.c))};
}
function avgLines(a){
  if(!a.days)return [];
  const o=['Beslenme: günde '+f1(a.feedN)+' kez, toplam '+Math.round(a.ml)+' ml'+(a.mlPer?' (öğün başı '+Math.round(a.mlPer)+' ml)':''),
    'Tür: günde '+f1(a.bm)+' anne sütü, '+f1(a.fm)+' mama'];
  if(a.gap)o.push('Beslenme aralığı: ortalama '+dur(a.gap));
  o.push('Bez: günde '+f1(a.wet)+' çiş, '+f1(a.dirty)+' kaka');
  if(a.slN)o.push('Uyku: günde '+dur(a.slDay)+' ('+f1(a.slN)+' uyku)');
  return o;
}
function docSections(){
  const d=docData(),sec=[];
  sec.push(['Bebek',[(S.baby.name||'Bebek')+' · '+ageText()+(S.baby.birth?' (doğum: '+fmtDate(parseDate(S.baby.birth))+')':'')]]);
  const gr=[['Kilo','gw','kg'],['Boy','gl','cm'],['Baş çevresi','gh','cm']].filter(x=>d[x[1]]).map(x=>x[0]+': '+f1(d[x[1]].v)+' '+x[2]+' · '+pctText(d[x[1]].p)+' ('+fmtShort(parseDate(d[x[1]].d))+')');
  sec.push(['Son ölçümler',gr.length?gr:['Ölçüm girilmedi']]);
  sec.push(['Son 7 gün ortalaması',avgLines(d.a7).length?avgLines(d.a7):['Yeterli kayıt yok']]);
  if(d.a14.days>d.a7.days)sec.push(['Son 14 gün ortalaması',avgLines(d.a14)]);
  sec.push(['Ateş (son 14 gün)',d.temps.length?['En yüksek: '+f1(d.temps[0].c)+' °C ('+fmtShort(d.temps[0].t)+' '+hm(d.temps[0].t)+')',d.temps.length+' ölçüm, 38 °C ve üzeri: '+d.temps.filter(t=>t.c>=38).length]:['Ateş kaydı yok']]);
  sec.push(['İlaç ve vitaminler',d.meds.length?d.meds.map(m=>m.name+(m.dose?' · '+m.dose:'')+((m.times||[]).length?' · '+m.times.join(', '):' · gerektiğinde')):['Kayıtlı ilaç yok']]);
  const vx=[];if(d.done.length)vx.push('Son yapılan: '+d.done[0].name+' ('+fmtShort(parseDate(d.done[0].done))+')');
  if(d.nd.length)vx.push('Sıradaki: '+d.nd.map(i=>i.name).join(', ')+' ('+fmtDate(d.nd[0].due)+')');
  sec.push(['Aşılar',vx.length?vx:['Aşı bilgisi yok']]);
  return {d,sec};
}
function docText(){
  const {d,sec}=docSections();
  let t='BEBEK TAKİP · DOKTOR ÖZETİ ('+fmtDate(Date.now())+')\n';
  sec.forEach(([h,l])=>{t+='\n'+h.toLocaleUpperCase('tr-TR')+'\n'+l.map(x=>'- '+x).join('\n')+'\n';});
  const open=d.qs.filter(q=>!q.done);
  if(open.length)t+='\nDOKTORA SORULACAKLAR\n'+open.map(q=>'- '+q.text).join('\n')+'\n';
  return t;
}
function vQs(){
  let h=`<div class="row" style="gap:8px;margin-bottom:8px"><input type="text" id="q-text" placeholder="Aklına gelen soruyu yaz" style="flex:1"><button class="btn pri" data-act="qAdd">Ekle</button></div>`;
  const qs=[...S.qs].sort((a,b)=>(a.done-b.done)||(a.c-b.c));
  if(!qs.length)h+=`<div class="card mute">Soru yok. Aklına geldikçe ekle, muayenede buradan bakıp sorulanları işaretlersin.</div>`;
  qs.forEach(q=>{h+=`<div class="item"><button class="check ${q.done?'on':''}" data-act="qToggle" data-id="${q.id}" aria-label="Soruldu">${q.done?'✓':''}</button><span class="tx" style="${q.done?'text-decoration:line-through;color:var(--mute)':''}">${esc(q.text)}</span><button class="btn sm ghost" data-act="qDel" data-id="${q.id}" aria-label="Sil">✕</button></div>`;});
  return h;
}
function vDoc(){
  const {d,sec}=docSections();
  let h='';
  if(d.appt)h+=`<div class="card hint"><b>Sıradaki randevu</b><p style="margin-bottom:0">${esc(d.appt.title||'Muayene')} · ${esc(dtLong(d.appt.t))} ${hm(d.appt.t)}${d.appt.place?' · '+esc(d.appt.place):''}</p></div>`;
  const pend=d.qs.filter(q=>!q.done);
  h+=`<h2>❓ Doktora sorulacaklar</h2>`+(pend.length?`<div class="card" style="line-height:1.55">${pend.map(q=>`<div>• ${esc(q.text)}</div>`).join('')}</div>`:`<div class="card mute">Bekleyen soru yok.</div>`)+`<button class="btn block ghost" data-act="infoOpen" data-id="qs" style="margin-bottom:12px">Soruları düzenle</button>`;
  sec.forEach(([t,l])=>{h+=`<h2>${esc(t)}</h2><div class="card" style="line-height:1.55">${l.map(x=>`<div>${esc(x)}</div>`).join('')}</div>`;});
  h+=`<button class="btn block" data-act="docCopy">📋 Özeti metin olarak kopyala</button><div class="foot">Ortalamalar bugünü içermez ve yalnızca kayıt girilen günlerden hesaplanır.</div>`;
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

function openSleep(){
  openSheet(`<h3>😴 Uyku</h3>
  <button class="btn pri block" data-act="startTimer" data-k="sleep">▶ Uyudu, sayacı başlat</button>
  <div class="or">veya geçmiş kaydı elle gir</div>
  ${sleepRow()}
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
function openPump(){
  openSheet(`<h3>🥛 Sağım</h3>
  <label>Sağılan miktar (ml)</label><input type="number" id="f-ml" inputmode="numeric" placeholder="80">
  <div class="chips">${[30,60,90,120,150,180].map(v=>`<button data-act="ml" data-v="${v}">${v}</button>`).join('')}</div>
  ${dtRow()}
  <button class="btn pri block" style="margin-top:16px" data-act="savePump">Kaydet</button>`);
}
function tempVal(){const c=num($('#f-c').value);if(!(c>=34&&c<=43)){flash('Dereceyi gir (örn. 37,5)');return null;}return Math.round(c*10)/10;}
function tempHint(){
  const el=$('#tempHint');if(!el)return;const c=num($('#f-c').value),am=ageMonthsAt(Date.now());
  el.className=c>=38?'bad':'mute';
  el.textContent=!(c>=38)?'':am!==null&&am<3?'3 aydan küçük bebekte 38 °C ve üzeri ateşte beklemeden doktoru ara.':'38 °C ve üzeri ateş. Doktorunun önerdiği şekilde hareket et; bebek halsizse ya da ateş düşmüyorsa doktoru ara.';
}
function openTemp(){
  const m=lastFeverMed(),now=Date.now();
  openSheet(`<h3>🌡️ Ateş ölçümü</h3>
  <label>Derece (°C)</label><input type="text" id="f-c" inputmode="decimal" placeholder="37,5" data-inp="tempHint">
  <div id="tempHint" style="margin-top:8px;font-size:14px;font-weight:600"></div>
  <label>Not (isteğe bağlı)</label><input type="text" id="f-note" placeholder="Koltuk altı, aşıdan sonra…">
  ${dtRow()}
  <div class="mute" style="margin-top:10px;font-size:14px">${m?'Son ateş düşürücü: '+esc(m.name)+(m.dose?' · '+esc(m.dose):'')+' · '+ago(now-m.t)+' ('+fmtShort(m.t)+' '+hm(m.t)+')':'Verilen ateş düşürücüyü İlaç sekmesinden "Verdim" ile kaydedersen son doz saati burada görünür.'}</div>
  <button class="btn pri block" style="margin-top:16px" data-act="saveTemp">Kaydet</button>
  <button class="btn block" style="margin-top:10px" data-act="tab" data-v="med">💊 İlaç sekmesine git</button>`);
}
function openDiaper(){
  sel={};
  openSheet(`<h3>🧷 Bez</h3>
  <div class="seg"><button data-act="seg" data-g="dk" data-v="wet">💧 Çiş</button><button data-act="seg" data-g="dk" data-v="dirty">💩 Kaka</button><button data-act="seg" data-g="dk" data-v="both">💧💩 İkisi</button></div>
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
  else if(e.type==='sleep')f='';
  else if(e.type==='bottle')f=seg('bk',[['Anne sütü','Anne sütü'],['Mama','Mama']],e.kind||'Anne sütü')+`<label>Miktar (ml)</label><input type="number" id="f-ml" inputmode="numeric" value="${e.ml||''}">`;
  else if(e.type==='diaper')f=seg('dk',[['wet','💧 Çiş'],['dirty','💩 Kaka'],['both','💧💩 İkisi']],e.kind)+`<div id="colorRow" ${e.kind==='wet'?'hidden':''}><label>Renk (isteğe bağlı)</label><div class="chips">${['Sarı','Yeşil','Kahverengi','Koyu'].map(c=>`<button class="${e.color===c?'on':''}" data-act="col" data-v="${c}">${c}</button>`).join('')}</div></div>`;
  else if(e.type==='pump')f=`<label>Sağılan miktar (ml)</label><input type="number" id="f-ml" inputmode="numeric" value="${e.ml||''}">`;
  else if(e.type==='temp')f=`<label>Derece (°C)</label><input type="text" id="f-c" inputmode="decimal" value="${f1(e.c)}" data-inp="tempHint"><div id="tempHint" style="margin-top:8px;font-size:14px;font-weight:600"></div><label>Not</label><input type="text" id="f-note" value="${esc(e.note||'')}">`;
  else f=`<div class="mute">${esc(i.s||'')}</div>`;
  if(e.by)f+=`<div class="mute" style="margin-top:10px;font-size:13px">Kaydı giren: ${esc(e.by)}</div>`;
  const sl=e.type==='sleep';
  openSheet(`<h3>${i.ic} ${esc(i.t)} · düzenle</h3>${sl?sleepRow(e.t,e.t+(e.dur||0)):dtRow(e.t)}<div style="height:12px"></div>${f}
  <button class="btn pri block" style="margin-top:16px" data-act="saveEv" data-id="${id}">Kaydet</button>${delBlock('delEv',id)}<button class="btn block" style="margin-top:10px" data-act="close">Kapat</button>`);
  if(sl)sleepSum();
  tempHint();
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
  sel={sex:S.baby.sex||'m'};
  openSheet(`<h3>⚙️ Bebek bilgileri</h3>
  <label>Adı</label><input type="text" id="s-name" value="${esc(S.baby.name)}" placeholder="Bebeğin adı">
  <label>Doğum tarihi</label><input type="date" id="s-birth" value="${esc(S.baby.birth)}">
  <label>Cinsiyet (büyüme eğrisi için)</label><div class="seg"><button class="${sel.sex==='m'?'on':''}" data-act="seg" data-g="sex" data-v="m">Erkek</button><button class="${sel.sex==='f'?'on':''}" data-act="seg" data-g="sex" data-v="f">Kız</button></div>
  <label>Bu telefonu kullanan (kayıtlarda görünür)</label><input type="text" id="s-who" value="${esc(who())}" placeholder="Anne, Baba…" maxlength="20">
  <div class="chips">${['Anne','Baba'].map(n=>`<button data-act="whoName" data-v="${n}">${n}</button>`).join('')}</div>
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
  <div class="mute" style="font-size:14px;margin-bottom:8px">Yedek, bütün kayıtları tek dosyada toplar. Ayda bir alıp e-postana ya da Drive'a atman yeterli. ${lastBackupText()}</div>
  <button class="btn pri block" data-act="backupFile">💾 Yedeği dosya olarak indir</button>
  <label class="btn block" style="margin-top:10px;display:flex;align-items:center;justify-content:center">📂 Dosyadan geri yükle<input type="file" accept=".json,application/json,text/plain" data-chg="restoreFile" hidden></label>
  <div class="or">veya metin olarak</div>
  <button class="btn block" data-act="backup">Yedeği göster ve kopyala</button>
  <textarea id="s-backup" style="margin-top:10px" placeholder="Yedek metni burada görünür. Geri yüklemek için buraya yapıştır."></textarea>
  <button class="btn block" style="margin-top:10px" data-act="restore">Yapıştırılan yedeği geri yükle</button>
  <button class="btn block ghost" style="margin-top:10px" data-act="close">Kapat</button>
  <div class="foot" style="text-align:center">Bebek Takip · Sürüm ${VERSION} · ${BUILD}</div>`);
}

function lastBackupText(){try{const t=+localStorage.getItem('bt_backup');return t?'Son yedek: '+fmtDate(t)+'.':'Henüz yedek alınmadı.';}catch(e){return '';}}
function backupJSON(){return allEvents().then(ev=>JSON.stringify(Object.assign({},S,{events:ev,timers:{},_v:VERSION,_at:Date.now()})));}
function markBackup(){try{localStorage.setItem('bt_backup',String(Date.now()));}catch(e){}}
function doRestore(txt){
  try{const o=JSON.parse(txt);if(!o||!Array.isArray(o.events))throw 0;
    const mg=(a,b)=>{const ids=new Set(a.map(x=>x.id));return a.concat((b||[]).filter(x=>x&&x.id&&!ids.has(x.id)));};
    const n0=S.events.length;
    S.events=mg(S.events,o.events);S.meds=mg(S.meds,o.meds);S.appts=mg(S.appts,o.appts);S.notes=mg(S.notes,o.notes);S.qs=mg(S.qs,o.qs);
    S.growth=mg(S.growth,(o.growth||[]).map(g=>Object.assign({},g,{kg:g.kg>30?Math.round(g.kg)/1000:g.kg})));
    S.vax=Object.assign({},o.vax||{},S.vax);
    if(o.baby&&(o.baby.name||o.baby.birth)&&!(S.baby.name||S.baby.birth))S.baby=o.baby;
    S.events.forEach(e=>{if(SINCE&&e.t<SINCE)oldEv.set(e.id,e);});
    save();closeSheet();render();flash('Geri yüklendi ('+(S.events.length-n0)+' yeni kayıt)');}catch(e){flash('Yedek okunamadı');}
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
  tab(el){if(!$('#sheet').hidden)closeSheet();if(el.dataset.v==='info'&&tab==='info'&&el.dataset.topic===undefined)infoTopic='';tab=el.dataset.v;if(el.dataset.topic!==undefined)infoTopic=el.dataset.topic;render();scrollTo(0,0);},
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
  sndPlay(el){if(sndId===el.dataset.id)sndStop();else sndStart(el.dataset.id);render();},
  sndVol(el){sndVol=+el.value;if(SG&&AC)SG.gain.setTargetAtTime(sndGain(sndVol),AC.currentTime,0.05);},
  sndTimer(el){sndMin=+el.dataset.v;sndSetTimer();render();},
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
  sleepSum(){sleepSum();},
  medName(el){$('#m-name').value=el.dataset.v;},
  qBottle:openBottle,qDiaper:openDiaper,qSleep:openSleep,qPump:openPump,qTemp:openTemp,
  whoName(el){$('#s-who').value=el.dataset.v;},
  tempHint(){tempHint();},
  repeatBottle(){const l=[...S.events].filter(e=>e.type==='bottle').sort((a,b)=>b.t-a.t)[0];if(!l)return;
    if(Date.now()-l.t<5*60000)return flash('Az önce kaydedildi (5 dk içinde tekrar edilmez)');
    addEv({id:uid(),type:'bottle',t:Date.now(),ml:l.ml,kind:l.kind||'Anne sütü'});save();render();flash(l.ml+' ml '+(l.kind||'')+' kaydedildi');},
  savePump(){const ml=num($('#f-ml').value);if(!(ml>0))return flash('Miktarı gir');const t=tAt('f-time','f-date');if(t===null)return;addEv({id:uid(),type:'pump',t,ml:Math.round(ml)});save();closeSheet();render();flash('Kaydedildi');},
  saveTemp(){const c=tempVal();if(c===null)return;const t=tAt('f-time','f-date');if(t===null)return;const e={id:uid(),type:'temp',t,c},n=$('#f-note').value.trim();if(n)e.note=n;addEv(e);save();closeSheet();render();flash('Kaydedildi');},
  growthTab(el){growthTab=el.dataset.v;render();},
  qAdd(){const i=$('#q-text'),v=i.value.trim();if(!v)return flash('Soruyu yaz');S.qs.push({id:uid(),text:v,c:Date.now(),done:false});save();render();},
  qToggle(el){const q=S.qs.find(x=>x.id===el.dataset.id);if(q){q.done=!q.done;save();render();}},
  qDel(el){S.qs=S.qs.filter(x=>x.id!==el.dataset.id);save();render();},
  docCopy(){copyText(docText());},
  startTimer(el){const k=el.dataset.k;S.timers[k]={start:Date.now()};if(k==='breast')S.timers[k].side=sel.side||'Sol';save();closeSheet();render();},
  stopTimer(el){const k=el.dataset.k,t=S.timers[k];if(!t)return;const e={id:uid(),type:k,t:t.start,dur:Date.now()-t.start};if(k==='breast')e.side=t.side;addEv(e);delete S.timers[k];save();render();flash('Kaydedildi');},
  cancelTimer(el){delete S.timers[el.dataset.k];save();render();},
  saveSleep(){const r=sleepAt();if(!r)return;addEv({id:uid(),type:'sleep',t:r.t,dur:r.dur});save();closeSheet();render();flash('Kaydedildi');},
  saveBottle(){const ml=num($('#f-ml').value);if(!(ml>0))return flash('Miktarı gir');const t=tAt('f-time','f-date');if(t===null)return;addEv({id:uid(),type:'bottle',t,ml:Math.round(ml),kind:sel.bk||'Anne sütü'});save();closeSheet();render();flash('Kaydedildi');},
  saveDiaper(){if(!sel.dk)return flash('Çiş mi kaka mı seç');const t=tAt('f-time','f-date');if(t===null)return;const e={id:uid(),type:'diaper',t,kind:sel.dk};if(sel.col&&sel.dk!=='wet')e.color=sel.col;addEv(e);save();closeSheet();render();flash('Kaydedildi');},
  openEv(el){openEv(el.dataset.id);},
  saveEv(el){
    const e=S.events.find(x=>x.id===el.dataset.id);if(!e)return;
    if(e.type==='sleep'){const r=sleepAt();if(!r)return;e.t=r.t;e.dur=r.dur;keepOld(e);save();closeSheet();render();flash('Güncellendi');return;}
    const t=tAt('f-time','f-date');if(t===null)return;
    if(e.type==='breast'){const m=num($('#f-dur').value);if(!(m>0))return flash('Süreyi gir');e.dur=m*60000;e.side=sel.side||'Sol';}
    if(e.type==='bottle'){const ml=num($('#f-ml').value);if(!(ml>0))return flash('Miktarı gir');e.ml=Math.round(ml);e.kind=sel.bk||'Anne sütü';}
    if(e.type==='diaper'){if(!sel.dk)return flash('Çiş mi kaka mı seç');e.kind=sel.dk;if(sel.col&&sel.dk!=='wet')e.color=sel.col;else delete e.color;}
    if(e.type==='pump'){const ml=num($('#f-ml').value);if(!(ml>0))return flash('Miktarı gir');e.ml=Math.round(ml);}
    if(e.type==='temp'){const c=tempVal();if(c===null)return;e.c=c;const n=$('#f-note').value.trim();if(n)e.note=n;else delete e.note;}
    e.t=t;keepOld(e);save();closeSheet();render();flash('Güncellendi');
  },
  dayGo(el){const d=parseDate(vday());d.setDate(d.getDate()+(+el.dataset.v));const k=dayKey(d.getTime());viewDay=k>=todayStr()?'':k;render();},
  dayPick(el){const v=el.value;viewDay=!v||v>=todayStr()?'':v;render();},
  delEv(el){oldEv.delete(el.dataset.id);S.events=S.events.filter(e=>e.id!==el.dataset.id);save();closeSheet();render();},
  medSlot(el){const {id,slot}=el.dataset,i=S.events.findIndex(e=>e.type==='med'&&e.medId===id&&e.slot===slot);
    if(i>=0)S.events.splice(i,1);else{const m=S.meds.find(x=>x.id===id);addEv({id:uid(),type:'med',medId:id,name:m.name,dose:m.dose,slot,t:Date.now()});}
    save();render();},
  medNow(el){const m=S.meds.find(x=>x.id===el.dataset.id);addEv({id:uid(),type:'med',medId:m.id,name:m.name,dose:m.dose,t:Date.now()});save();render();flash(m.name+' kaydedildi');},
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
  saveSettings(){S.baby.name=$('#s-name').value.trim();S.baby.birth=$('#s-birth').value;if(sel.sex)S.baby.sex=sel.sex;try{const w=$('#s-who').value.trim();if(w)localStorage.setItem('bt_who',w);else localStorage.removeItem('bt_who');}catch(e){}save();closeSheet();render();},
  backup(){backupJSON().then(js=>{const ta=$('#s-backup');if(!ta)return;ta.value=js;ta.select();markBackup();
    try{navigator.clipboard.writeText(js).then(()=>flash('Kopyalandı'),()=>flash('Metni elle kopyala'));}catch(e){flash('Metni elle kopyala');}});},
  backupFile(){flash('Yedek hazırlanıyor…');backupJSON().then(async js=>{
    const name='bebek-takip-yedek-'+todayStr()+'.json',blob=new Blob([js],{type:'application/json'});
    const ios=/iP(hone|ad|od)/.test(navigator.userAgent)||(navigator.platform==='MacIntel'&&navigator.maxTouchPoints>1);
    if(ios){try{const file=new File([blob],name,{type:'application/json'});
      if(navigator.canShare&&navigator.canShare({files:[file]})){await navigator.share({files:[file],title:'Bebek Takip yedeği'});markBackup();return;}}catch(e){if(e&&e.name==='AbortError')return;}}
    const url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download=name;document.body.appendChild(a);a.click();a.remove();
    setTimeout(()=>URL.revokeObjectURL(url),30000);markBackup();flash('Yedek dosyası indi');});},
  restoreFile(el){const f=el.files&&el.files[0];if(!f)return;const r=new FileReader();r.onload=()=>doRestore(String(r.result||''));r.onerror=()=>flash('Dosya okunamadı');r.readAsText(f);},
  restore(){doRestore($('#s-backup').value);},
  vaxImg(){vaxSave(false);},
  vaxShare(){vaxSave(true);},
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

document.addEventListener('input',e=>{const el=e.target.closest&&e.target.closest('[data-inp]');if(el&&H[el.dataset.inp])H[el.dataset.inp](el);});
document.addEventListener('change',e=>{const el=e.target.closest&&e.target.closest('[data-chg]');if(el&&H[el.dataset.chg])H[el.dataset.chg](el);});

/* ---------- başlat ---------- */
(function boot(){
  applyTheme();matchMedia('(prefers-color-scheme: dark)').addEventListener&&matchMedia('(prefers-color-scheme: dark)').addEventListener('change',applyTheme);document.addEventListener('visibilitychange',()=>{if(!document.hidden)applyTheme();});window.addEventListener('pageshow',applyTheme);
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
