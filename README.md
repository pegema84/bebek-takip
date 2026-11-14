# Bebek Takip

Telefonda çalışan, iki ebeveynin ortak kullandığı bebek takip uygulaması (emzirme, bez, uyku, ilaç, aşı, büyüme).
Kayıtlar Firebase Firestore'da gizli bir aile koduyla saklanır. Bu depoda bebeğe ait hiçbir veri yoktur.

Kaynak kod: `src/app.js`. Yayın dosyası `app.js` şu komutla üretilir:
`npm install && npx esbuild src/app.js --bundle --minify --format=iife --target=es2020 --outfile=app.js`
