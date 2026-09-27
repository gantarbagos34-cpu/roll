const express=require('express');
const http=require('http');
const WebSocket=require('ws');
const path=require('path');

const app=express();
const server=http.createServer(app);
const WSS_URL='wss://developer.mig33.id/developer/ws';
const sessions=new Map();
const rollState={running:false,stop:false,room:'',delay:0,message:''};

app.use(express.json({limit:'10mb'}));
app.use(express.static(path.join(__dirname,'public')));

function makeId(){return Math.random().toString(36).slice(2)+Date.now().toString(36)}
function safe(s){return String(s||'').slice(0,300)}
function closeSession(s){
 try{s.ws?.close()}catch{}
 if(s.ping)clearInterval(s.ping);
 s.status='closed';
}
function wait(ms){return new Promise(r=>setTimeout(r,Math.max(0,ms)))}

function connectAccount(account){
 return new Promise((resolve,reject)=>{
  let settled=false;
  const finish=(ok,err)=>{
   if(settled)return; settled=true;
   if(ok)resolve(account); else reject(err);
  };
  let ws;
  try{ws=new WebSocket(WSS_URL)}catch(e){finish(false,e);return}
  account.ws=ws; account.status='connecting';

  const timeout=setTimeout(()=>{
   if(!settled){account.status='error';try{ws.close()}catch{};finish(false,new Error('Timeout login 15 detik'))}
  },15000);

  ws.on('open',()=>{account.status='authenticating'});

  ws.on('message',raw=>{
   let m; try{m=JSON.parse(raw.toString())}catch{return}
   account.last=m;
   if(m.type==='auth.required'){
    try{ws.send(JSON.stringify({type:'developer.login',username:account.username,password:account.password}))}
    catch(e){account.status='error';clearTimeout(timeout);finish(false,e)}
   }else if(m.type==='session.ready'){
    account.status='ready';
    account.permissions=m.data?.developer?.permissions||[];
    account.wallet=m.data?.wallet||null;
    if(account.wallet)account.balance=account.wallet.label||String(account.wallet.balance_cr||'');
    account.ping=setInterval(()=>{
     if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify({type:'ping'}));
    },40000);
    clearTimeout(timeout); finish(true);
   }else if(m.type==='wallet.balance.result'){
    account.wallet=m.data?.wallet||account.wallet;
    if(account.wallet)account.balance=account.wallet.label||String(account.wallet.balance_cr||'');
   }else if(m.type==='error' && !settled){
    account.status='error'; clearTimeout(timeout);
    finish(false,new Error(m.data?.message||m.data?.error||'Login gagal'));
   }else if(m.type==='session.replaced'){
    account.status='error'; clearTimeout(timeout);
    finish(false,new Error('Session digantikan oleh koneksi lain'));
   }
  });

  ws.on('close',()=>{
   clearTimeout(timeout);
   if(account.ping)clearInterval(account.ping);
   if(account.status!=='error' && account.status!=='closed')account.status='closed';
   if(!settled)finish(false,new Error('WebSocket ditutup sebelum login selesai'));
  });
  ws.on('error',e=>{
   account.status='error'; clearTimeout(timeout);
   if(!settled)finish(false,e);
  });
 });
}

async function sendToOpen(action,room){
 let sent=0,failed=0;
 for(const a of sessions.values()){
  if(a.status!=='ready'||a.ws?.readyState!==WebSocket.OPEN){failed++;continue}
  try{a.ws.send(JSON.stringify({type:action==='join'?'room.join':'room.leave',room}));sent++}
  catch{failed++}
 }
 return {sent,failed};
}

app.post('/api/accounts',async(req,res)=>{
 for(const a of sessions.values())closeSession(a);
 sessions.clear();
 const rawList=Array.isArray(req.body.accounts)?req.body.accounts:[];
 const list=rawList.filter(a=>a&&String(a.username||'').trim());
 const created=list.map(raw=>({
  id:makeId(),username:String(raw.username).trim(),password:String(raw.password||''),
  status:'new',permissions:[],balance:''
 }));
 created.forEach(a=>sessions.set(a.id,a));

 // Login bertahap: satu perintah HTTP mengendalikan seluruh koneksi, tetapi
 // WebSocket tidak dibuka sekaligus. Ini mencegah burst koneksi membuat server/API
 // menolak akun setelah belasan/dua puluh koneksi. Tidak ada batas jumlah akun
 // yang ditanamkan di aplikasi; akun diproses dalam batch kecil sampai selesai.
 const LOGIN_BATCH_SIZE=Math.max(1,Math.min(5,Number(process.env.LOGIN_BATCH_SIZE)||5));
 const LOGIN_BATCH_GAP_MS=Math.max(0,Number(process.env.LOGIN_BATCH_GAP_MS)||500);
 const results=[];
 for(let start=0;start<created.length;start+=LOGIN_BATCH_SIZE){
  const batch=created.slice(start,start+LOGIN_BATCH_SIZE);
  const batchResults=await Promise.all(batch.map(async a=>{
   try{await connectAccount(a)}
   catch(e){a.status='error';a.error=safe(e.message)}
   return {id:a.id,username:a.username,status:a.status,permissions:a.permissions,balance:a.balance||'',error:a.error||''};
  }));
  results.push(...batchResults);
  if(start+LOGIN_BATCH_SIZE<created.length)await wait(LOGIN_BATCH_GAP_MS);
 }
 const ready=results.filter(a=>a.status==='ready').length;
 res.json({ok:true,count:results.length,ready,accounts:results});
});

app.post('/api/close',(_,res)=>{
 rollState.stop=true; rollState.running=false;
 for(const a of sessions.values())closeSession(a);
 sessions.clear();
 res.json({ok:true});
});

app.get('/api/status',(_,res)=>res.json({
 ok:true,
 accounts:[...sessions.values()].map(a=>({
  id:a.id,username:a.username,status:a.status,permissions:a.permissions,
  balance:a.balance||'',error:a.error||''
 }))
}));

app.post('/api/room',async(req,res)=>{
 const room=String(req.body.room||'').trim();
 const action=req.body.action;
 if(!room||!['join','leave'].includes(action))
  return res.status(400).json({ok:false,error:'room/action invalid'});
 const result=await sendToOpen(action,room);
 res.json({ok:result.failed===0,...result});
});

app.get('/api/roll/status',(_,res)=>res.json({
 ok:true,running:rollState.running,room:rollState.room,delay:rollState.delay,message:rollState.message
}));

app.post('/api/roll/start',async(req,res)=>{
 if(rollState.running)return res.status(409).json({ok:false,error:'ROLL sedang berjalan'});
 const room=String(req.body.room||'').trim();
 const delay=Math.max(0,Number(req.body.delay)||0);
 if(!room)return res.status(400).json({ok:false,error:'room wajib diisi'});

 // ROLL memakai akun yang sudah dimuat/login. Setiap WebSocket diproses
 // sepenuhnya satu per satu:
 // LOGIN -> ENTER ROOM -> LEAVE ROOM -> LOGOUT, lalu lanjut ke WebSocket berikutnya.
 // Setelah WebSocket terakhir selesai, kembali lagi ke WebSocket pertama.
 const ids=[...sessions.values()].filter(a=>a.username&&a.password);
 if(!ids.length)return res.status(409).json({ok:false,error:'Belum ada akun yang siap diproses'});

 rollState.running=true;rollState.stop=false;rollState.room=room;rollState.delay=delay;
 rollState.message=`ROLL berjalan: ${ids.length} WebSocket, urutan 1→${ids.length}→1`;

 (async()=>{
  let i=0;
  try{
   while(rollState.running&&!rollState.stop){
    const a=ids[i%ids.length];

    // Pastikan koneksi lama benar-benar logout sebelum login untuk putaran ini.
    if(a.ws || a.status==='ready' || a.status==='closed'){
     closeSession(a);
     a.ws=null;
     a.status='new';
    }

    rollState.message=`WebSocket ${(i%ids.length)+1}/${ids.length}: LOGIN`;
    try{
     await connectAccount(a);
    }catch(e){
     a.status='error';
     a.error=safe(e.message);
     rollState.message=`WebSocket ${(i%ids.length)+1}/${ids.length}: LOGIN gagal`;
     i++;
     continue;
    }

    if(rollState.stop)break;

    // Delay hanya berlaku dari LOGIN selesai menuju ENTER ROOM.
    await wait(delay);
    if(rollState.stop)break;

    rollState.message=`WebSocket ${(i%ids.length)+1}/${ids.length}: ENTER ROOM`;
    if(a.ws?.readyState===WebSocket.OPEN){
     a.ws.send(JSON.stringify({type:'room.join',room}));
    }
    if(rollState.stop)break;

    // ENTER -> LEAVE berjalan langsung tanpa delay tambahan.
    rollState.message=`WebSocket ${(i%ids.length)+1}/${ids.length}: LEAVE ROOM`;
    if(a.ws?.readyState===WebSocket.OPEN){
     a.ws.send(JSON.stringify({type:'room.leave',room}));
    }
    if(rollState.stop)break;

    // LEAVE -> LOGOUT juga langsung, tanpa memakai setting Delay.
    rollState.message=`WebSocket ${(i%ids.length)+1}/${ids.length}: LOGOUT`;
    closeSession(a);
    a.ws=null;
    a.status='closed';

    i++;
   }
  }catch(e){
   rollState.message='ROLL berhenti karena error: '+safe(e.message);
  }finally{
   // Jika STOP ditekan di tengah proses, tutup koneksi aktif agar tidak ada
   // WebSocket yang tertinggal dalam keadaan login.
   for(const a of ids){
    if(a.ws && (a.status==='ready'||a.status==='connecting'||a.status==='authenticating')){
     closeSession(a);
     a.ws=null;
    }
   }
   rollState.running=false;
   rollState.stop=false;
   if(!rollState.message.startsWith('ROLL berhenti'))rollState.message='ROLL dihentikan.';
  }
 })();

 res.json({ok:true,message:'ROLL dimulai: LOGIN → ENTER → LEAVE → LOGOUT per WebSocket.'});
});

app.post('/api/roll/stop',(_,res)=>{
 rollState.stop=true;
 rollState.running=false;
 rollState.message='ROLL dihentikan.';
 res.json({ok:true,message:rollState.message});
});

const port=Number(process.env.PORT)||3000;
server.listen(port,()=>console.log('MigMaster Roll listening on '+port));
