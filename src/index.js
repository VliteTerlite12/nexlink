// ============================================================
//  Socket-Signalling — CF Worker + Durable Object (SQLite)
//  Flow: Register → Lookup → Offer/Answer/ICE → P2P Direct
// ============================================================

export class SocketObject {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sql = state.storage.sql;
    this.sessions = new Map(); // hash → WebSocket

    // Tabel online users (persistent via SQLite)
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS online_users (
        hash TEXT PRIMARY KEY,
        name TEXT,
        registered_at INTEGER
      )
    `);

    // Bersihkan data lama saat DO restart
    this.sql.exec(`DELETE FROM online_users`);
  }

  async fetch(request) {
    const upgrade = request.headers.get('Upgrade');

    // Health check
    if (!upgrade || upgrade !== 'websocket') {
      return new Response(
        JSON.stringify({ status: 'ok', server: 'Socket-Signalling v1.0' }),
        { headers: { 'Content-Type': 'application/json', ...cors() } }
      );
    }

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();

    let myHash = null;

    server.addEventListener('message', async (e) => {
      let msg;
      try {
        msg = JSON.parse(e.data);
      } catch {
        return;
      }

      switch (msg.type) {

        // ── Registrasi ──────────────────────────────────────
        case 'register': {
          if (!msg.hash || !msg.name) return;
          myHash = msg.hash.toUpperCase();

          // Tutup sesi lama jika ada (login ulang)
          const old = this.sessions.get(myHash);
          if (old) { try { old.close(); } catch {} }

          this.sessions.set(myHash, server);
          this.sql.exec(
            `INSERT OR REPLACE INTO online_users (hash, name, registered_at)
             VALUES (?, ?, ?)`,
            myHash, msg.name, Date.now()
          );

          reply(server, { type: 'registered', hash: myHash });
          break;
        }

        // ── Cek apakah teman online ─────────────────────────
        case 'lookup': {
          if (!msg.hash) return;
          const target = msg.hash.toUpperCase();
          const online = this.sessions.has(target);

          // Ambil nama jika ada
          let name = null;
          if (online) {
            const rows = [...this.sql.exec(
              `SELECT name FROM online_users WHERE hash = ?`, target
            )];
            name = rows[0]?.name ?? null;
          }

          reply(server, { type: 'lookup-result', hash: target, online, name });
          break;
        }

        // ── Relay WebRTC Signaling ───────────────────────────
        case 'offer':
        case 'answer':
        case 'ice-candidate': {
          if (!msg.to || !myHash) return;
          const dest = this.sessions.get(msg.to.toUpperCase());

          if (dest && dest.readyState === 1) {
            dest.send(JSON.stringify({ ...msg, from: myHash }));
          } else {
            reply(server, { type: 'peer-offline', hash: msg.to });
          }
          break;
        }

        // ── Keepalive ────────────────────────────────────────
        case 'ping': {
          reply(server, { type: 'pong' });
          break;
        }
      }
    });

    const cleanup = () => {
      if (myHash) {
        this.sessions.delete(myHash);
        try {
          this.sql.exec(`DELETE FROM online_users WHERE hash = ?`, myHash);
        } catch {}
      }
    };

    server.addEventListener('close', cleanup);
    server.addEventListener('error', cleanup);

    return new Response(null, { status: 101, webSocket: client });
  }
}

// ── Main Worker ──────────────────────────────────────────────
export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: cors() });
    }

    // Satu DO global untuk semua user
    const id = env.SOCKET_OBJECT.idFromName('main');
    const obj = env.SOCKET_OBJECT.get(id);
    return obj.fetch(request);
  }
};

// ── Helpers ──────────────────────────────────────────────────
function reply(ws, data) {
  if (ws.readyState === 1) ws.send(JSON.stringify(data));
}

function cors() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Upgrade, Connection',
  };
}
