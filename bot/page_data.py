#!/usr/bin/env python3
"""Print a compact JSON of everything the "SSL Hybrid Paper Bot" phone Page needs.
Read-only: reads summary.json, live.json, state.json, trades.jsonl, equity.jsonl (and server.status()
for RUNNING/PAUSED/STOPPED). Usage: python3 page_data.py            -> compact JSON on stdout
                                    python3 page_data.py --js       -> the full DATA block (with markers)
See PAGE_REFRESH.md for how the 30-minute refresh uses this."""
import os, sys, json, time
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)

def jl(name, d=None):
    try: return json.load(open(os.path.join(HERE, name)))
    except Exception: return d
def jsonl(name):
    out = []
    try:
        for l in open(os.path.join(HERE, name)):
            l = l.strip()
            if l:
                try: out.append(json.loads(l))
                except Exception: pass
    except FileNotFoundError: pass
    return out
def wib(ms, fmt="%Y-%m-%d %H:%M"):
    return time.strftime(fmt, time.gmtime(ms / 1000 + 7 * 3600)) if ms else None
def r(x, n=4): return None if x is None else round(float(x), n)
def rp(x):  # price: keep significant digits for tiny coins like PEPE
    if x is None: return None
    x = float(x); return float(f"{x:.6g}") if abs(x) < 1 else round(x, 4)

def downsample(rows, n=80):
    if len(rows) <= n: return rows
    step = (len(rows) - 1) / (n - 1)
    idx = sorted({round(i * step) for i in range(n)}); return [rows[i] for i in idx]

def build():
    S = jl("summary.json", {}) or {}; L = jl("live.json", {}) or {}; ST = jl("state.json", {}) or {}
    try:
        import server; st = server.status()
    except Exception as e:
        hb = L.get("heartbeat_ms"); age = (time.time() * 1000 - hb) / 1000 if hb else None
        st = {"state": "STOPPED" if age is None or age > 1800 else ("PAUSED" if L.get("paused") else "RUNNING"),
              "detail": f"status fallback ({e})", "heartbeat_age_s": age}
    cap = float(S.get("capital") or ST.get("capital") or 500)
    bal = L.get("balance", S.get("balance")); bh = L.get("bh_balance", S.get("buy_hold_balance"))
    stats = ST.get("stats", {})
    lc = L.get("coins", {})
    coins = []
    for c in ST.get("coins", []):
        ind = c.get("ind") or {}; pos = c.get("pos"); live = lc.get(c["coin"], {})
        coins.append({
            "coin": c["coin"], "pos": "long" if pos else "cash",
            "entry": rp(pos["entry_px"]) if pos else None, "entry_wib": wib(pos["entry_t"]) if pos else None,
            "price": rp(live.get("price", ind.get("close"))), "value": r(live.get("value", c.get("value_close")), 2),
            "unreal": r(live.get("unreal"), 2), "unreal_pct": r(live.get("unreal_pct"), 5),
            "realised": r(c.get("pnl_realised"), 2), "trades": c.get("n_trades"),
            "arrow": ind.get("last_arrow"), "arrow_wib": wib(ind.get("last_arrow_t")), "bars_since": ind.get("bars_since_arrow"),
            "exit_state": ind.get("exit_state"), "exit_line": rp(ind.get("exit_line")),
            "ssl1": rp(ind.get("ssl1")), "baseline": rp(ind.get("base")), "color": ind.get("color"),
            "pending": c.get("pending")})
    tr = jsonl("trades.jsonl"); tr.sort(key=lambda t: t.get("exit_t", 0))
    trades = [{"coin": t["coin"], "in": t.get("entry_wib", wib(t.get("entry_t")))[5:16], "out": t.get("exit_wib", wib(t.get("exit_t")))[5:16],
               "entry": rp(t["entry_px"]), "exit": rp(t["exit_px"]), "pnl": r(t["pnl"], 2), "ret": r(t["ret"], 5),
               "tf": t.get("exit_tf") or t.get("entry_tf"), "mode": t.get("exit_mode") or t.get("entry_mode")} for t in tr[-15:]][::-1]
    eq = jsonl("equity.jsonl")
    hbms = L.get("heartbeat_ms")
    if bal is not None and hbms and (not eq or hbms > eq[-1]["t"]): eq.append({"t": hbms, "eq": bal, "bh": bh})
    eq = downsample(eq, 80)
    equity = [{"l": wib(e["t"], "%H:%M"), "eq": r(e["eq"], 2), "bh": r(e.get("bh"), 2)} for e in eq]
    opens = [{"coin": p["coin"], "entry_wib": p["entry_wib"][5:16], "entry": rp(p["entry_px"]), "price": rp(p["price"]),
              "unreal": r(p["unreal_usd"], 2), "unreal_pct": r(p["unreal_pct"], 5), "bars": p.get("bars")} for p in S.get("open_positions", [])]
    rules = ST.get("rules", {})
    return {
        "updated_wib": time.strftime("%d %b %Y, %H:%M", time.gmtime(time.time() + 7 * 3600)) + " WIB",
        "state": st.get("state"), "state_detail": st.get("detail"), "hb_age_s": r(st.get("heartbeat_age_s"), 0),
        "heartbeat_wib": L.get("heartbeat_wib") or S.get("heartbeat_wib"),
        "mode": S.get("trade_mode") or ST.get("trade_mode"), "mode_text": S.get("trade_mode_text"),
        "tf": L.get("active_tf") or S.get("active_tf"), "start_wib": (ST.get("start_wib") or "").replace("T", " "),
        "next_bar_wib": L.get("next_bar_close_wib"), "last_bar_close_wib": ST.get("last_bar_close_wib"),
        "capital": cap, "balance": r(bal, 2), "pnl": r(bal - cap, 2) if bal is not None else None,
        "pnl_pct": r(bal / cap - 1, 5) if bal is not None else None, "bh": r(bh, 2),
        "bh_pct": r(bh / cap - 1, 5) if bh is not None else None,
        "fees": r(stats.get("fees", S.get("fees")), 2), "win_rate": r(stats.get("win_rate", S.get("win_rate")), 4),
        "wins": stats.get("wins"), "losses": stats.get("losses"), "n_trades": stats.get("n_trades", S.get("trades")),
        "max_dd_pct": r(stats.get("max_dd_pct", S.get("max_dd_pct")), 5), "max_dd_usd": r(stats.get("max_dd_usd"), 2),
        "profit_factor": r(stats.get("profit_factor"), 3), "exposure_pct": r(stats.get("exposure_pct"), 4),
        "realised": r(stats.get("realised_pnl"), 2), "source": ST.get("source"),
        "rules": {"entry": rules.get("entry"), "exit": rules.get("exit"), "fee": rules.get("fee_per_side"), "fill": rules.get("fill"),
                  "direction": rules.get("direction"), "compounding": rules.get("compounding")},
        "tf_switches": [{"at": s.get("at_wib"), "from": s.get("from"), "to": s.get("to")} for s in S.get("tf_switches", [])],
        "mode_switches": [{"at": s.get("at_wib"), "from": s.get("from"), "to": s.get("to")} for s in S.get("mode_switches", [])],
        "equity": equity, "coins": coins, "open": opens, "trades": trades,
        "pending": S.get("pending", []), "flips": S.get("fresh_watchlist_flips_15m", []),
    }

if __name__ == "__main__":
    d = json.dumps(build(), separators=(",", ":"), ensure_ascii=False)
    if "--js" in sys.argv: print("// DATA-START\nconst DATA: any = " + d + ";\n// DATA-END")
    else: print(d)
