const C='bebek-takip-v1';
const SHELL=['./','index.html','app.js','manifest.webmanifest','icons/icon-192.png','icons/icon-512.png'];
self.addEventListener('install',e=>{e.waitUntil(caches.open(C).then(c=>c.addAll(SHELL)).then(()=>self.skipWaiting()));});
self.addEventListener('activate',e=>{e.waitUntil(caches.keys().then(ks=>Promise.all(ks.filter(k=>k!==C).map(k=>caches.delete(k)))).then(()=>self.clients.claim()));});
self.addEventListener('fetch',e=>{
  const r=e.request;
  if(r.method!=='GET'||new URL(r.url).origin!==location.origin)return;
  e.respondWith(
    fetch(r,{cache:'no-cache'}).then(res=>{const cp=res.clone();if(res.ok)caches.open(C).then(c=>c.put(r,cp));return res;})
      .catch(()=>caches.match(r).then(h=>h||caches.match('index.html')))
  );
});
