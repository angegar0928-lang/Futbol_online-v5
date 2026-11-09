/* Servidor de Modo Carrera: sirve /public y hace de relay de salas en /ws.
   Sin dependencias (WebSocket implementado con módulos de Node).
   Protocolo (el mismo que espera el juego):
     {type:"create"}              -> {type:"created", code, id}
     {type:"join", code}          -> {type:"joined", id, hostId} | {type:"error", error:"NOT_FOUND"|"FULL"}
     {type:"relay", to, data}     -> a los demás: {type:"msg", from, data}   (to = "host" | "all" | id)
     al anfitrión: {type:"peer_join", id} / {type:"peer_leave", id}
     a los invitados si se va el anfitrión: {type:"closed", reason:"HOST_LEFT"} */
const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, "public");
const MAX_PLAYERS = 8;                 // igual que MAX_ONLINE del juego
const MAX_PAYLOAD = 64 * 1024;
const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MIME = { ".html": "text/html; charset=utf-8", ".js": "text/javascript", ".css": "text/css", ".png": "image/png", ".jpg": "image/jpeg", ".svg": "image/svg+xml", ".ico": "image/x-icon", ".json": "application/json", ".mp3": "audio/mpeg", ".ogg": "audio/ogg" };

/* ---------- Web estática ---------- */
const server = http.createServer((req, res) => {
  let p = decodeURIComponent((req.url || "/").split("?")[0]);
  if (p === "/") p = "/index.html";
  const file = path.normalize(path.join(PUBLIC, p));
  if (!file.startsWith(PUBLIC)) { res.writeHead(403); return res.end("403"); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end("404"); }
    res.writeHead(200, { "Content-Type": MIME[path.extname(file)] || "application/octet-stream", "Cache-Control": "no-cache" });
    res.end(data);
  });
});

/* ---------- WebSocket mínimo (RFC 6455) ---------- */
function frame(opcode, payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!req.url.startsWith("/ws") || !key) { socket.destroy(); return; }
  const accept = crypto.createHash("sha1").update(key + GUID).digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  socket.setNoDelay(true);
  const client = { id: crypto.randomBytes(4).toString("hex"), socket, room: null, isHost: false, alive: true };
  let buf = Buffer.alloc(0), frags = [];

  client.send = obj => { if (!socket.destroyed) socket.write(frame(1, Buffer.from(JSON.stringify(obj)))); };
  client.close = () => { try { socket.end(frame(8, Buffer.alloc(0))); } catch (_) {} };

  socket.on("data", chunk => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const fin = (buf[0] & 0x80) !== 0, op = buf[0] & 0x0f, masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_PAYLOAD) { socket.destroy(); return; }
      if (buf.length < off + (masked ? 4 : 0) + len) return;
      let payload;
      if (masked) {
        const mask = buf.subarray(off, off + 4); off += 4;
        payload = Buffer.from(buf.subarray(off, off + len));
        for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
      } else payload = Buffer.from(buf.subarray(off, off + len));
      buf = buf.subarray(off + len);
      if (op === 8) { client.close(); return; }
      if (op === 9) { socket.write(frame(10, payload)); continue; }
      if (op === 10) { client.alive = true; continue; }
      if (op === 1 || op === 2 || op === 0) {
        frags.push(payload);
        if (fin) {
          const text = Buffer.concat(frags).toString("utf8"); frags = [];
          let msg; try { msg = JSON.parse(text); } catch (_) { continue; }
          if (msg && typeof msg === "object") onMessage(client, msg);
        }
      }
    }
  });
  socket.on("close", () => onClose(client));
  socket.on("error", () => socket.destroy());
  client.ping = () => { if (!client.alive) { socket.destroy(); return; } client.alive = false; socket.write(frame(9, Buffer.alloc(0))); };
  clients.add(client);
});

/* ---------- Salas ---------- */
const rooms = new Map();   // code -> { host, guests: Map(id -> client) }
const clients = new Set();

function newCode() {
  for (;;) {
    let c = ""; for (let i = 0; i < 6; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
}
function leaveRoom(c) {
  const room = c.room && rooms.get(c.room);
  if (!room) { c.room = null; return; }
  if (c.isHost) {
    for (const g of room.guests.values()) { g.send({ type: "closed", reason: "HOST_LEFT" }); g.room = null; g.close(); }
    rooms.delete(c.room);
  } else if (room.guests.delete(c.id)) {
    room.host.send({ type: "peer_leave", id: c.id });
  }
  c.room = null; c.isHost = false;
}
function onMessage(c, m) {
  switch (m.type) {
    case "create": {
      leaveRoom(c);
      const code = newCode();
      rooms.set(code, { host: c, guests: new Map() });
      c.room = code; c.isHost = true;
      c.send({ type: "created", code, id: c.id });
      break;
    }
    case "join": {
      const code = String(m.code || "").toUpperCase(), room = rooms.get(code);
      if (!room) return c.send({ type: "error", error: "NOT_FOUND" });
      if (room.guests.size + 1 >= MAX_PLAYERS) return c.send({ type: "error", error: "FULL" });
      leaveRoom(c);
      room.guests.set(c.id, c); c.room = code; c.isHost = false;
      c.send({ type: "joined", id: c.id, hostId: room.host.id });
      room.host.send({ type: "peer_join", id: c.id });
      break;
    }
    case "relay": {
      const room = c.room && rooms.get(c.room);
      if (!room) return;
      const out = { type: "msg", from: c.id, data: m.data };
      if (m.to === "host") { if (!c.isHost) room.host.send(out); }
      else if (m.to === "all") { if (c.isHost) room.guests.forEach(g => g.send(out)); else { room.host.send(out); room.guests.forEach(g => { if (g !== c) g.send(out); }); } }
      else { const t = m.to === room.host.id ? room.host : room.guests.get(m.to); if (t && t !== c) t.send(out); }
      break;
    }
  }
}
function onClose(c) { clients.delete(c); leaveRoom(c); }

setInterval(() => clients.forEach(c => c.ping && c.ping()), 25000);   // mantiene viva la conexión tras proxies

server.listen(PORT, () => console.log("Modo Carrera en http://localhost:" + PORT));
