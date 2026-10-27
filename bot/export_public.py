#!/usr/bin/env python3
"""Export the 24/7 paper bot's state as a public bot.json for the GitHub Pages "Real bot" tab.
Read-only towards the bot: reads summary.json, live.json, state.json, trades.jsonl, equity.jsonl,
events.jsonl and data/chart_<COIN>.json (reusing page_data.build() for the headline figures).
Writes <repo>/bot.json atomically. Contains no secrets, keys, pids or local paths.
Usage: python3 export_public.py [out_path]   (default: ghpages-repo/bot.json next to this file)"""
import os, sys, json, time, re
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import page_data as P
from page_data import jl, jsonl, wib, r, rp

BARS = 150; EQ_POINTS = 500; TRADES = 200; EVENTS = 30
PATH_RE = re.compile(r"(/(?:workspace|home|tmp|root|usr|var|etc|cursor)/[^\s'\"),]*)")
PID_RE = re.compile(r"\s*\(pid \d+\)|\bpid \d+\b")
def clean(s):
    if not isinstance(s, str): return s
    return PID_RE.sub("", PATH_RE.sub("[local]", s)).strip()

def downsample(rows, n):
    if len(rows) <= n: return rows
    step = (len(rows) - 1) / (n - 1)
    return [rows[i] for i in sorted({round(i * step) for i in range(n)})]

def chart(coin, fills):
    d = jl(f"data/chart_{coin}.json", None)
    if not d or not d.get("t"): return None
    n = len(d["t"]); a = max(0, n - BARS); t0 = d["t"][a]
    def sl(k, f=rp): return [f(v) if v is not None else None for v in (d.get(k) or [None] * n)[a:]]
    def ev(k): return [x for x in (d.get(k) or []) if x >= t0]
    return {"t": d["t"][a:], "o": sl("o"), "h": sl("h"), "l": sl("l"), "c": sl("c"),
            "base": sl("base"), "upper": sl("upper"), "lower": sl("lower"), "ssl1": sl("ssl1"), "ssl2": sl("ssl2"), "exit": sl("exit"),
            "au": ev("AU"), "ad": ev("AD"), "L": ev("L"), "S": ev("S"),
            "fills": [{"t": f["t"], "side": f["side"], "px": rp(f["px"]), "why": f.get("why"), "tf": f.get("tf"), "pnl": r(f.get("pnl"), 2), "reason": f.get("reason")}
                      for f in fills if f.get("t", 0) >= t0]}

def pool_view(ST):
    p = ST.get("pool")
    if not p: return None
    return {"max_positions": p["max_positions"], "cash": p["cash"], "slots_used": p["slots_used"], "free_slots": p["free_slots"],
            "next_trade_size": r(p["next_trade_size"], 4), "tp_usd": p.get("tp_usd"), "sl_usd": p.get("sl_usd"), "since_ms": p.get("since_ms"), "since_wib": p.get("since_wib"),
            "slots": [{"coin": s["coin"], "cost": s["cost"], "units": s["units"], "entry_ms": s["entry_t"], "entry": rp(s["entry_px"]), "tp_px": s.get("tp_px"), "sl_px": s.get("sl_px")} for s in p.get("slots", [])],
            "n_skipped": p.get("n_skipped", 0),
            "skipped": [{"coin": k["coin"], "ms": k["sig_t"], "wib": wib(k["sig_t"]), "tf": k.get("tf"), "held": k.get("held"), "strength": r(k.get("strength"), 3), "reason": k.get("reason")} for k in p.get("skipped", [])[-20:]][::-1],
            "switches": [{"at_wib": s["at_wib"], "why": s["why"], "equity_before": r(s["equity_before"], 4), "equity_after": r(s["equity_after"], 4), "fees": r(s["fees"], 4)} for s in p.get("switches", [])]}

def build():
    now = int(time.time() * 1000)
    B = P.build()                                   # the same compact figures the Canvas page uses
    S = jl("summary.json", {}) or {}; L = jl("live.json", {}) or {}; ST = jl("state.json", {}) or {}
    cfg = jl("config.json", {}) or {}
    stc = {c["coin"]: c for c in ST.get("coins", [])}
    coins = []
    for c in B["coins"]:
        s = stc.get(c["coin"], {}); ind = s.get("ind") or {}; pos = s.get("pos")
        px, ex = c.get("price"), c.get("exit_line")
        c = dict(c)
        c.update(arrow_ms=ind.get("last_arrow_t"), bar_ms=ind.get("bar_t"), close=rp(ind.get("close")),
                 vs_exit_pct=r(px / ex - 1, 5) if px and ex else None, above_exit=(px > ex) if px and ex else None,
                 alloc=r(s.get("alloc"), 2), fees=r(s.get("fees"), 4), bh_value=r((L.get("coins", {}).get(c["coin"], {}) or {}).get("bh_value"), 2),
                 units=pos.get("units") if pos else None, entry_ms=pos.get("entry_t") if pos else None, entry_fee=pos.get("entry_fee") if pos else None,
                 symbol=c["coin"] + "USDT",
                 # exact inputs for the browser's live mark (same formula as paper_bot.marked()):
                 #   value = units*price if long else value_close;  unreal = value - cost;  bh = alloc*price/bh_c0
                 calc={"side": (pos or {}).get("side") if pos else None, "units": pos.get("units") if pos else None, "cost": pos.get("cost") if pos else None,
                       "entry_px": pos.get("entry_px") if pos else None, "entry_fee": pos.get("entry_fee") if pos else None,
                       "cash": s.get("cash"), "value_close": s.get("value_close"), "alloc": s.get("alloc"), "bh_c0": s.get("bh_c0"),
                       "bh_value_close": s.get("bh_value_close"), "price_at_export": (L.get("coins", {}).get(c["coin"], {}) or {}).get("price"),
                       "tp_px": pos.get("tp_px") if pos else None, "sl_px": pos.get("sl_px") if pos else None},
                 size=r(pos.get("cost"), 4) if pos else None, tp_px=pos.get("tp_px") if pos else None, sl_px=pos.get("sl_px") if pos else None,
                 upper=rp(ind.get("upper")), lower=rp(ind.get("lower")), ssl2=rp(ind.get("ssl2")), atr_pct=r(ind.get("atr_pct"), 5),
                 rank=((ST.get("universe_info") or {}).get(c["coin"]) or {}).get("rank"), mcap_usd=((ST.get("universe_info") or {}).get(c["coin"]) or {}).get("mcap_usd"),
                 in_universe=s.get("in_universe"), tradable=s.get("tradable"), entry_reason=pos.get("entry_reason") if pos else None,
                 live_arrow=(ind.get("live") or {}).get("arrow"), live_exit_line=rp((ind.get("live") or {}).get("exit_line")),
                 last_signal=ind.get("last_signal"), bars_since_signal=ind.get("bars_since_signal"),
                 chart=chart(c["coin"], s.get("fills") or []))
        coins.append(c)
    tr = jsonl("trades.jsonl"); tr.sort(key=lambda t: t.get("exit_t", 0))
    trades = [{"coin": t["coin"], "side": t.get("side"), "in_ms": t.get("entry_t"), "out_ms": t.get("exit_t"),
               "in": (t.get("entry_wib") or wib(t.get("entry_t")) or "")[:16], "out": (t.get("exit_wib") or wib(t.get("exit_t")) or "")[:16],
               "entry": rp(t["entry_px"]), "exit": rp(t["exit_px"]), "pnl": r(t["pnl"], 4), "ret": r(t["ret"], 5), "fee": r(t.get("fee"), 4),
               "bars": t.get("bars"), "tf": t.get("exit_tf") or t.get("entry_tf"), "mode": t.get("exit_mode") or t.get("entry_mode"),
               "reason": t.get("exit_reason") or "ARROW", "entry_reason": t.get("entry_reason"), "size": r(t.get("cost"), 2)}
              for t in tr[-TRADES:]][::-1]
    eq = jsonl("equity.jsonl"); hb = L.get("heartbeat_ms")
    if B.get("balance") is not None and hb and (not eq or hb > eq[-1]["t"]): eq.append({"t": hb, "eq": L.get("balance"), "bh": L.get("bh_balance")})
    equity = [{"t": e["t"], "eq": r(e["eq"], 3), "bh": r(e.get("bh"), 3)} for e in downsample(eq, EQ_POINTS)]
    evs = [e for e in jsonl("events.jsonl") if e.get("kind") not in ("heartbeat",)][-EVENTS:][::-1]
    events = [{"ms": e.get("ts"), "wib": e.get("ts_wib"), "kind": e.get("kind"), "coin": e.get("coin"), "msg": clean(e.get("msg"))} for e in evs]
    paused = None
    try:
        import pausectl; cur = pausectl.current()
        if cur: paused = {"since_ms": cur.get("since_ms"), "since_wib": wib(cur.get("since_ms")), "reason": clean(cur.get("reason"))}
    except Exception: pass
    out = {
        "schema": 1, "kind": "ssl-hybrid-paper-bot", "paper_only": True,
        "exported_ms": now, "exported_wib": wib(now, "%Y-%m-%d %H:%M:%S"),
        "status": B.get("state"), "status_detail": clean(B.get("state_detail")), "paused": paused,
        "heartbeat_ms": hb, "heartbeat_wib": B.get("heartbeat_wib"), "hb_age_s": B.get("hb_age_s"),
        "mode": B.get("mode") or cfg.get("mode"), "mode_text": B.get("mode_text"), "tf": B.get("tf"),
        "start_wib": B.get("start_wib"), "start_ms": ST.get("start_ms"),
        "next_bar_ms": L.get("next_bar_close_ms"), "next_bar_wib": B.get("next_bar_wib"), "last_bar_close_wib": B.get("last_bar_close_wib"),
        "source": B.get("source") or L.get("source"), "prices_ms": L.get("prices_t"),
        "positions_as_of_ms": hb or now, "live_formula": "value=units*price (long) else value_close; unreal=value-cost; unreal_pct=value/cost-1; net_if_sold=units*price*(1-fee)-cost; bh=alloc*price/bh_c0; balance=pool.cash+sum(value) (pool sizing) else sum(value)",
        "sizing": ST.get("sizing", "coin"), "sizing_text": ST.get("sizing_text"), "fee": (ST.get("rules") or {}).get("fee_per_side"),
        "pool": pool_view(ST),
        "signal_timing": ST.get("signal_timing") or cfg.get("signal_timing") or "close", "signal_timing_text": ST.get("signal_timing_text") or "Signals: on candle close",
        "universe": ST.get("universe") or ST.get("coins_traded"), "exclude_top_mcap": ST.get("exclude_top_mcap"), "excluded": ST.get("excluded"),
        "universe_ranks": {k: {"rank": v.get("rank"), "mcap_usd": v.get("mcap_usd"), "vol24h_usd": v.get("vol24h_usd")} for k, v in (ST.get("universe_info") or {}).items() if isinstance(v, dict)},
        "legacy_coins": ST.get("legacy_coins"), "epoch_from_wib": ST.get("epoch_from_wib"), "last_eval_ms": L.get("last_eval_ms") or (L.get("last_eval") or {}).get("ms") if isinstance(L.get("last_eval"), dict) else L.get("last_eval_ms"),
        "capital": B["capital"], "balance": B["balance"], "pnl": B["pnl"], "pnl_pct": B["pnl_pct"], "bh": B["bh"], "bh_pct": B["bh_pct"],
        "vs_bh": r(B["balance"] - B["bh"], 2) if B["balance"] is not None and B["bh"] is not None else None,
        "fees": B["fees"], "win_rate": B["win_rate"], "wins": B["wins"], "losses": B["losses"], "n_trades": B["n_trades"],
        "max_dd_pct": B["max_dd_pct"], "max_dd_usd": B["max_dd_usd"], "profit_factor": B["profit_factor"], "exposure_pct": B["exposure_pct"],
        "realised": B["realised"], "rules": B["rules"], "tf_switches": B["tf_switches"], "mode_switches": B["mode_switches"],
        "coins": coins, "open": [dict(o, **{k: (stc.get(o["coin"], {}).get("pos") or {}).get(k) for k in ("tp_px", "sl_px", "cost", "units")}) for o in B["open"]], "pending": S.get("pending", []), "watch_flips": B.get("flips", []),
        "trades": trades, "equity": equity, "events": events,
    }
    return out

if __name__ == "__main__":
    dst = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "ghpages-repo", "bot.json")
    data = json.dumps(build(), separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    tmp = dst + ".tmp"; open(tmp, "w").write(data); os.replace(tmp, dst)
    print(f"wrote {dst} ({len(data)} bytes)")
