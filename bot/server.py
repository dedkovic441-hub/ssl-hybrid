#!/usr/bin/env python3
"""Local dashboard + JSON API for the SSL Hybrid 15m paper bot (read-only; binds 127.0.0.1)."""
import os, json, time, sys, threading
import pausectl
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.environ.get("PORT", "8811"))
COINS = {"ZEC", "NEAR", "PEPE", "SOL", "XRP"}

def jload(name, default=None):
    try:
        with open(os.path.join(HERE, name)) as f: return json.load(f)
    except Exception: return default
def tail_jsonl(name, n):
    p = os.path.join(HERE, name)
    if not os.path.exists(p): return []
    with open(p, "rb") as f:
        f.seek(0, 2); size = f.tell(); f.seek(max(0, size - 400_000)); lines = f.read().decode(errors="ignore").splitlines()
    out = []
    for l in lines[-n:]:
        try: out.append(json.loads(l))
        except Exception: pass
    return out
RUN = os.path.join(HERE, "run")
def pid_alive(name):
    try: os.kill(int(open(os.path.join(RUN, name + ".pid")).read().strip()), 0); return True
    except Exception: return False
def wib(ms): return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ms / 1000 + 7 * 3600)) if ms else None
def status():
    """RUNNING (fresh heartbeat) / PAUSED (flag set, bot alive) / STOPPED (user stop, or crashed/stale)."""
    live = jload("live.json", {}) or {}; now = time.time() * 1000
    hb = live.get("heartbeat_ms"); age = (now - hb) / 1000 if hb else None
    alive = pid_alive("bot"); sup = pid_alive("supervisor")
    paused = pausectl.current(); user = jload("run/STOPPED_BY_USER", None)
    o = {"pid_alive": alive, "supervisor_alive": sup, "heartbeat_age_s": age, "last_heartbeat_wib": wib(hb), "server_time_ms": now, "paused": paused}
    if user and not alive:
        o.update(state="STOPPED", kind="user", detail=f"Stopped by user (stop.sh) at {wib(user.get('at_ms'))} WIB. Run start.sh to resume the bot.")
    elif not alive or age is None or age > 1800:
        o.update(state="STOPPED", kind="crash", detail=("Crashed / not responding" if alive else "Crashed (process not running)") + f"; last heartbeat {wib(hb) or 'never'} WIB. " + ("The supervisor is restarting it (checks every 10 s)." if sup else "Supervisor is NOT running: run start.sh."))
    elif paused:
        o.update(state="PAUSED", kind="paused", detail=f"Paused since {wib(paused.get('since_ms'))} WIB: {paused.get('reason')}. No new entries; exits still follow the rules.", paused_since_ms=paused.get("since_ms"))
    else:
        o.update(state="RUNNING", kind="ok", detail=f"Heartbeat {round(age)} s ago" + (" (late, the bot may be busy fetching data)" if age > 90 else ""))
    o["running"] = o["state"] != "STOPPED"
    return o
def build():
    try: return int(os.path.getmtime(os.path.join(HERE, "dashboard.html")))
    except Exception: return 0
def summary_status_loop():   # keep summary.json's status truthful even when the bot is down
    while True:
        try:
            s = jload("summary.json", None); st = status()
            if s is not None and (s.get("bot_state") != st["state"] or (st["state"] == "STOPPED" and s.get("status_detail") != st["detail"])):
                s.update(bot_state=st["state"], status=st["state"].lower(), status_detail=st["detail"], status_checked_wib=wib(st["server_time_ms"]))
                tmp = os.path.join(HERE, "summary.json.srvtmp"); open(tmp, "w").write(json.dumps(s, separators=(",", ":"))); os.replace(tmp, os.path.join(HERE, "summary.json"))
        except Exception: pass
        time.sleep(15)

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def send(self, code, body, ctype="application/json"):
        b = body if isinstance(body, bytes) else body.encode()
        self.send_response(code); self.send_header("Content-Type", ctype); self.send_header("Cache-Control", "no-store")
        origin = self.headers.get("Origin", "")
        allowed = "https://dedkovic441-hub.github.io"
        if origin == allowed:
            self.send_header("Access-Control-Allow-Origin", allowed)
            self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Requested-With")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def authorized_remote(self):
        origin = self.headers.get("Origin", "")
        if not origin: return True
        if origin != "https://dedkovic441-hub.github.io": return False
        expected = os.environ.get("LOCAL_DASHBOARD_TOKEN", "").strip()
        supplied = self.headers.get("Authorization", "")
        return bool(expected) and supplied == "Bearer " + expected
    def do_OPTIONS(self):
        if self.headers.get("Origin", "") != "https://dedkovic441-hub.github.io":
            return self.send(403, '{"error":"origin not allowed"}')
        return self.send(204, b"")
    def js(self, obj): self.send(200, json.dumps(obj, separators=(",", ":")))
    def do_GET(self):
        u = urlparse(self.path); q = parse_qs(u.query); p = u.path
        if self.headers.get("Origin") and not self.authorized_remote():
            return self.send(403, '{"error":"remote access denied: configure LOCAL_DASHBOARD_TOKEN and check the dashboard origin"}')
        try:
            if p in ("/", "/index.html"): return self.send(200, open(os.path.join(HERE, "dashboard.html"), "rb").read(), "text/html; charset=utf-8")
            if p == "/static/lightweight-charts.js": return self.send(200, open(os.path.join(HERE, "static/lightweight-charts.js"), "rb").read(), "application/javascript")
            if p == "/api/summary":
                s = jload("summary.json", {}) or {}; s["bot"] = status(); return self.js(s)
            if p == "/api/state":
                st = jload("state.json", {}) or {}
                st.pop("_seen", None); st.pop("_first_trade_px", None)
                return self.js({"state": st, "live": jload("live.json", {}), "bot": status(), "build": build()})
            if p == "/api/equity": return self.js(tail_jsonl("equity.jsonl", 100000))
            if p == "/api/trades": return self.js(tail_jsonl("trades.jsonl", 5000))
            if p == "/api/events":
                n = min(int(q.get("n", ["150"])[0]), 2000)
                ev = [e for e in tail_jsonl("events.jsonl", 4000) if q.get("all") or e.get("kind") != "heartbeat"]
                return self.js(ev[-n:])
            if p == "/api/chart":
                c = q.get("coin", [""])[0].upper()
                st0 = jload("state.json", {}) or {}
                if c not in COINS | set(st0.get("coins_traded") or []) | set(st0.get("universe") or []): return self.send(400, '{"error":"unknown coin"}')
                return self.js(jload(f"data/chart_{c}.json", {}))
            if p == "/healthz": return self.js(status())
            return self.send(404, '{"error":"not found"}')
        except Exception as e:
            return self.send(500, json.dumps({"error": str(e)}))

    def do_POST(self):
        p = urlparse(self.path).path
        if self.headers.get("Origin") and not self.authorized_remote():
            return self.send(403, '{"error":"remote access denied: configure LOCAL_DASHBOARD_TOKEN and check the dashboard origin"}')
        # Keep the existing custom-header guard for commands.
        if self.headers.get("X-Requested-With") != "ssl-dashboard": return self.send(403, '{"error":"forbidden"}')
        try:
            n = int(self.headers.get("Content-Length") or 0); body = json.loads(self.rfile.read(n) or b"{}") if n else {}
            if p == "/api/pause": pausectl.pause(str(body.get("reason") or "paused from the dashboard")); return self.js(status())
            if p == "/api/resume": pausectl.resume(); return self.js(status())
            if p == "/api/timeframe":
                tf = body.get("tf")
                if tf not in ("15m", "1h", "4h"): return self.send(400, '{"error":"tf must be 15m, 1h or 4h"}')
                rq = os.path.join(HERE, "run", "tf_request.json"); tmp = rq + ".tmp"
                open(tmp, "w").write(json.dumps({"tf": tf, "at_ms": int(time.time() * 1000)})); os.replace(tmp, rq)
                return self.js({"requested": tf})
            return self.send(404, '{"error":"not found"}')
        except Exception as e: return self.send(500, json.dumps({"error": str(e)}))

if __name__ == "__main__":
    threading.Thread(target=summary_status_loop, daemon=True).start()
    ThreadingHTTPServer.allow_reuse_address = True
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), H)
    print(f"dashboard on http://127.0.0.1:{PORT}/", flush=True)
    srv.serve_forever()
