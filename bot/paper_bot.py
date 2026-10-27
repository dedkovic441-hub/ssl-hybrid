#!/usr/bin/env python3
"""
SSL Hybrid 15m PAPER-TRADING daemon (simulation only: no exchange keys, no orders are ever sent).

Rules = the dashboard's Paper tab: 15m, long-only, baseline exit, 0.1% fee per side, full compounding,
$500 split equally across ZEC, NEAR, PEPE, SOL, XRP, start 2026-10-27 07:52 WIB.
Deterministic: every bar the whole run is recomputed from the start time (same data window as the browser:
1000 bars of warm-up before the first 15m bar after the start), so restarts reproduce the same trades.
Indicator + backtest come unchanged from ssl_core.py (= ssl_hybrid_backtest.py, parity-tested with the JS dashboard).
"""
import os, sys, json, time, math, logging, argparse, signal, traceback
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta
import numpy as np, pandas as pd, requests
import ssl_core as core
import pausectl

HERE = os.path.dirname(os.path.abspath(__file__))
WIB = timezone(timedelta(hours=7))
BAR = 900_000
PRE_BARS = 1000
FEE = core.FEE
WARMUP = core.WARMUP
START_WIB = "2026-10-27T07:52"
CAPITAL = 500.0
COINS = ["ZEC", "NEAR", "PEPE", "SOL", "XRP"]
# Trading mode (config.json {"mode": ...} or --mode):
#   state_long  (default) long-only, follows the baseline state: LONG while bull, cash while bear; buys at the start if already bull
#   state_ls    long+short, follows the baseline state: LONG while bull, SHORT (unlevered) while bear; enters at the start
#   cross_long  the original mode: long-only, waits for a fresh crossover (long signal) after the start, sells on a short signal
# Baseline state = SSL Hybrid baseline colour on the last closed 15m bar: bull (close > upper channel), bear (close < lower
# channel), neutral (inside the channel, grey). Neutral keeps the previous bull/bear state, exactly like the indicator's
# BUY/SELL arrows, which only fire when price leaves the channel on the opposite side.
#   arrow_long  long-only, follows the indicator's Exit Arrows (Pine codiff: up = ta.crossover(close, sslExit), down =
#               ta.crossover(sslExit, close), sslExit = Exit SSL HMA 15): BUY at the next bar open after an up arrow, SELL to
#               cash after a down arrow, hold the latest arrow's state in between (enters at once if the latest arrow is up)
# state_long and arrow_long share the switchable 15m/1h/4h segment engine; a mode change is logged as a segment like a
# timeframe switch (live-price sync, balance carried over), so the whole run stays deterministic across restarts.
MODES = {"state_long": "Long-only · follow baseline state (buy at start if bull, sell to cash on bear)",
         "arrow_long": "Long-only · follow the Exit Arrows (BUY after an up arrow, sell to cash after a down arrow)",
         "state_ls": "Long+Short · follow baseline state (long while bull, unlevered short while bear)",
         "cross_long": "Long-only · fresh crossover only (original mode)"}
DEFAULT_MODE = "state_long"
SEG_MODES = ("state_long", "arrow_long")
WATCH = ["BTC","ETH","ZEC","SOL","XRP","NEAR","BNB","ENA","QNT","SUI","WLD","DOGE","PEPE","UNI","TRX","BMT","HYPE","ERA","LINK","HUMA","PUMP","AAVE","ADA","TAO","INJ","CHIP","LTC","ONDO","AVAX","AI"]
WATCH_TFS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}
SOURCES = [("api.binance.com", "https://api.binance.com"), ("data-api.binance.vision", "https://data-api.binance.vision")]
UA = {"User-Agent": "ssl-hybrid-paper-bot/1.0 (paper simulation)"}

log = logging.getLogger("paper")
S = requests.Session(); S.headers.update(UA)
SRC = {"name": SOURCES[0][0], "idx": 0}

def wib_to_ms(s):
    d = datetime.strptime(s[:16], "%Y-%m-%dT%H:%M").replace(tzinfo=WIB)
    return int(d.timestamp() * 1000)
def ms_wib(ms): return datetime.fromtimestamp(ms / 1000, WIB).strftime("%Y-%m-%d %H:%M:%S")
def now_ms(): return int(time.time() * 1000)

def atomic_write(path, text):
    tmp = path + ".tmp"
    with open(tmp, "w") as f: f.write(text); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)
def write_json(path, obj): atomic_write(path, json.dumps(obj, separators=(",", ":"), default=float))
def write_jsonl(path, rows): atomic_write(path, "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows))
def append_jsonl(path, row):
    with open(path, "a") as f: f.write(json.dumps(row, separators=(",", ":")) + "\n"); f.flush(); os.fsync(f.fileno())

# ---------------------------------------------------------------- data
_last_req = [0.0]
def api(path, params):
    last = None
    for k in range(len(SOURCES)):
        i = (SRC["idx"] + k) % len(SOURCES); name, base = SOURCES[i]
        for attempt in range(2):
            gap = time.time() - _last_req[0]
            if gap < 0.08: time.sleep(0.08 - gap)
            _last_req[0] = time.time()
            try:
                r = S.get(base + path, params=params, timeout=15)
                if r.status_code in (429, 418): time.sleep(5); last = RuntimeError(f"{name} HTTP {r.status_code}"); continue
                if r.status_code == 400: raise ValueError(f"bad request {params}: {r.text[:120]}")
                r.raise_for_status()
                SRC["idx"], SRC["name"] = i, name
                return r.json()
            except ValueError: raise
            except Exception as e:
                last = e; time.sleep(1)
    raise RuntimeError(f"all sources failed: {last}")

def klines(sym, tf, start=None, limit=1000):
    p = {"symbol": sym, "interval": tf, "limit": limit}
    if start is not None: p["startTime"] = int(start)
    return [[int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), int(r[6])] for r in api("/api/v3/klines", p)]

class KStore:
    """Per symbol/tf kline cache on disk; incremental updates."""
    def __init__(self, d): self.d = d; self.mem = {}
    def path(self, sym, tf): return os.path.join(self.d, f"klines_{sym}_{tf}.json")
    def get(self, sym, tf, frm):
        key = (sym, tf)
        rows = self.mem.get(key)
        if rows is None and os.path.exists(self.path(sym, tf)):
            try: rows = json.load(open(self.path(sym, tf)))
            except Exception: rows = None
        iv = WATCH_TFS.get(tf, BAR)
        if not rows or rows[0][0] > frm:
            rows = []
            s = frm
            while s < now_ms():
                part = klines(sym, tf, s, 1000)
                if not part: break
                rows += part; s = part[-1][0] + iv
                if len(part) < 1000: break
        else:
            last = rows[-1][0]
            while True:   # re-fetch from the last cached bar (it may have been still open)
                part = klines(sym, tf, last, 1000)
                rows = [r for r in rows if r[0] < last] + part
                if len(part) < 1000: break
                last = part[-1][0]
        m = {r[0]: r for r in rows}; rows = [m[k] for k in sorted(m)]
        self.mem[key] = rows
        write_json(self.path(sym, tf), rows)
        return [r for r in rows if r[0] >= frm]

def closed_df(rows, now):
    rows = [r for r in rows if r[6] < now]
    return pd.DataFrame(rows, columns=["t", "o", "h", "l", "c", "v", "ct"])

# ---------------------------------------------------------------- engine (pure; used by the daemon and the tests)
def atr14(df):
    """SSL Hybrid ATR: WMA 14 of ta.tr(true) (TradingView v6 default smoothing)."""
    return core.wma_tr_atr(df.h.values, df.l.values, df.c.values, 14)

def last_arrow(ind, t):
    a = ind["arrow"]
    for i in range(len(a) - 1, -1, -1):
        if a[i]: return i, ("up" if a[i] > 0 else "down"), int(t[i])
    return None, None, None

def seg_state(ind, mode):
    """+1 long / -1 out / 0 none, per bar, for a segment-engine mode."""
    return ind["arrow_state"] if mode == "arrow_long" else persistent_state(ind["color"])

def last_signal(ind, t):
    L, Sg = ind["long_sig"], ind["short_sig"]
    for i in range(len(L) - 1, -1, -1):
        if L[i] or Sg[i]: return i, ("long" if L[i] else "short"), int(t[i])
    return None, None, None

def persistent_state(color):
    """bull (+1) / bear (-1) from the baseline colour; neutral (0) keeps the previous non-neutral state."""
    s = np.zeros(len(color)); cur = 0
    for i, v in enumerate(color):
        if v != 0: cur = v
        s[i] = cur
    return s

def in_pause(f, pauses):
    for p in pauses:
        if p["from_ms"] <= f and (p.get("to_ms") is None or f < p["to_ms"]): return p
    return None

def sim_state(ds, tgt, pauses):
    """Same mechanics and arithmetic as ssl_core.backtest (decide at close i, fill at the open of i+1, FEE per side,
    compounding, unlevered short), driven by a target series; opening a position is blocked while paused, closing is not."""
    o, c, tt = ds.o.values, ds.c.values, ds.t.values; n = len(c)
    pos = 0; target = 0; cash = 1.0; units = 0.0; entry_eq = None
    eq = np.ones(n); trades = []; events = []; skipped = []; last_skip = -9
    for i in range(WARMUP, n):
        if i > WARMUP and target != pos:
            px = o[i]
            if pos != 0 and target != pos:
                val = units * px if pos > 0 else core.cash_short_val(cash, units, px)
                val *= (1 - FEE); trades.append(val / entry_eq - 1); events.append((i, "x")); cash = val; pos = 0
            if target != 0:
                p = in_pause(int(tt[i]), pauses)
                if p:
                    if last_skip != i - 1: skipped.append({"sig_t": int(tt[i - 1]), "fill_t": int(tt[i]), "side": "long" if target > 0 else "short", "reason": p.get("reason")})
                    last_skip = i
                else:
                    entry_eq = cash; ca = cash * (1 - FEE); units = ca / px; cash = ca; pos = target; events.append((i, "L" if target > 0 else "S"))
        if pos > 0: eq[i] = units * c[i]
        elif pos < 0: eq[i] = core.cash_short_val(cash, units, c[i])
        else: eq[i] = cash
        target = int(tgt[i])
    eq[:WARMUP] = 1.0
    return eq, trades, ((eq[-1] / entry_eq - 1) if pos != 0 else None), pos, events, target, skipped

def mode_signals(ind, mode):
    pst = persistent_state(ind["color"])
    return pst, (np.where(pst > 0, 1, 0) if mode == "state_long" else pst.astype(int))

def paper_coin(df, start_ms, coin_cap, coin, pauses=(), mode=DEFAULT_MODE):
    t0 = -(-start_ms // BAR) * BAR
    n = len(df); res = {"coin": coin, "alloc": coin_cap, "started": False, "trades": [], "fills": [], "pos": None,
                        "eq_t": [], "eq_v": [], "bh_v": [], "value": coin_cap, "cash": coin_cap, "fees": 0.0, "pending": None,
                        "bars_in_pos": 0, "bars_total": 0}
    if n < WARMUP + 60: res["error"] = "not enough data"; return res, None
    ind = core.indicator(df)
    tv = df.t.values
    w = np.where(tv == t0 - BAR)[0]
    res["first_fill_ms"] = int(t0)
    if not len(w) or w[0] < WARMUP: return res, ind
    sidx = int(w[0]); off = sidx - WARMUP
    ds = df.iloc[off:].reset_index(drop=True)
    inds = {k: v[off:] for k, v in ind.items()}
    res["skipped"] = []; res["mode"] = mode
    if mode != "cross_long":
        pst, tgt = mode_signals(ind, mode)
        eq, trades, open_ret, pos, events, target, res["skipped"] = sim_state(ds, tgt[off:], pauses)
        res["state_now"] = int(pst[-1]); res["color_now"] = int(ind["color"][-1])
    if mode == "cross_long" and pauses:   # paused: a long signal whose fill (next bar open) falls inside a pause opens nothing; exits are untouched
        ls = inds["long_sig"].copy(); tt = ds.t.values
        for i in np.where(ls)[0]:
            f = int(tt[i]) + BAR
            for p in pauses:
                if p["from_ms"] <= f and (p.get("to_ms") is None or f < p["to_ms"]):
                    ls[i] = False; res["skipped"].append({"sig_t": int(tt[i]), "fill_t": f, "reason": p.get("reason")}); break
        inds["long_sig"] = ls
    if mode == "cross_long":
        eq, trades, open_ret, pos, events = core.backtest(ds, inds, "long", "baseline")
    res["started"] = True
    cash, units, ent = coin_cap, 0.0, None
    for i, k in events:
        px, tm = float(ds.o[i]), int(ds.t[i])
        if k in ("L", "S"):
            fee = cash * FEE; units = (cash - fee) / px; side = "long" if k == "L" else "short"
            ent = {"i": i, "t": tm, "px": px, "cost": cash, "fee": fee, "sig_t": int(ds.t[i - 1]), "side": side, "coll": cash - fee}
            cash -= fee; res["fills"].append({"coin": coin, "side": "BUY" if k == "L" else "SHORT", "t": tm, "px": px, "fee": fee, "sig_t": ent["sig_t"], "value": cash})
        elif k == "x" and ent:
            gross = units * px if ent["side"] == "long" else core.cash_short_val(ent["coll"], units, px)
            fee = gross * FEE; cash = gross - fee
            tr = {"coin": coin, "side": ent["side"], "entry_t": ent["t"], "entry_px": ent["px"], "exit_t": tm, "exit_px": px, "cost": ent["cost"],
                  "proceeds": cash, "pnl": cash - ent["cost"], "ret": cash / ent["cost"] - 1, "fee": ent["fee"] + fee,
                  "entry_fee": ent["fee"], "exit_fee": fee, "bars": i - ent["i"]}
            res["trades"].append(tr); res["fees"] += ent["fee"] + fee
            res["fills"].append({"coin": coin, "side": "SELL" if ent["side"] == "long" else "COVER", "t": tm, "px": px, "fee": fee, "sig_t": int(ds.t[i - 1]), "pnl": tr["pnl"], "value": cash})
            units = 0.0; ent = None
    nl = len(ds); last_c = float(ds.c.values[-1])
    if pos != 0 and ent:
        vc = units * last_c if pos > 0 else core.cash_short_val(ent["coll"], units, last_c)
        res["pos"] = {"side": ent["side"], "entry_t": ent["t"], "entry_px": ent["px"], "units": units, "collateral": ent["coll"], "cost": ent["cost"], "entry_fee": ent["fee"],
                      "bars": nl - ent["i"], "close_px": last_c, "value_close": vc}
        res["fees"] += ent["fee"]; res["value"] = vc; res["cash"] = 0.0
    else:
        res["value"] = cash; res["cash"] = cash
    li = nl - 1
    if mode == "cross_long":
        tgt_now = 1 if inds["long_sig"][li] else (0 if inds["short_sig"][li] else pos)
        if inds["long_sig"][li] != ind["long_sig"][off + li] and pos == 0: res["pending"] = "long signal, entry SKIPPED (bot paused)"
    else:
        tgt_now = target
    names = {1: "LONG", -1: "SHORT", 0: "cash"}
    if tgt_now != pos:
        if tgt_now != 0 and in_pause(now_ms(), pauses) is not None:
            res["pending"] = (f"SELL/COVER at next bar open, then " if pos != 0 else "") + f"{names[tgt_now]} entry blocked while the bot is paused"
        elif tgt_now == 1: res["pending"] = ("COVER short and " if pos < 0 else "") + "BUY at next bar open"
        elif tgt_now == -1: res["pending"] = ("SELL long and " if pos > 0 else "") + "SHORT at next bar open"
        else: res["pending"] = ("SELL (exit to cash)" if pos > 0 else "COVER (exit to cash)") + " at next bar open"
    c0 = float(ds.c.values[WARMUP])
    for i in range(WARMUP, nl):
        res["eq_t"].append(int(ds.t[i]) + BAR); res["eq_v"].append(coin_cap * float(eq[i])); res["bh_v"].append(coin_cap * float(ds.c[i]) / c0)
    # exposure: bars (since start) where the position was held through the bar
    held = 0; p = 0; evs = {i: k for i, k in events}
    for i in range(WARMUP + 1, nl):
        if i in evs: p = {"L": 1, "S": -1}.get(evs[i], 0)
        held += p != 0
    res["bars_in_pos"], res["bars_total"] = held, max(0, nl - WARMUP - 1)
    res["bh_c0"] = c0
    return res, ind

def readout(df, ind, coin):
    n = len(df); c = float(df.c.values[-1]); a = atr14(df)
    up, lo, base, ex = (float(ind[k][-1]) for k in ("upper", "lower", "base", "ex_line"))
    li, ld, lt = last_signal(ind, df.t.values)
    color = int(ind["color"][-1])
    ai, ad, at = last_arrow(ind, df.t.values); L = lambda k: float(ind[k][-1])
    return {"coin": coin, "close": c, "bar_t": int(df.t.values[-1]), "base": base, "upper": up, "lower": lo, "exit_line": ex,
            "ssl1": L("ssl1_line"), "ssl1_state": int(ind["ssl1"][-1]), "ssl2": L("ssl2_line"), "ssl2_state": int(ind["ssl2"][-1]),
            "atr_up": L("atr_up"), "atr_lo": L("atr_lo"), "diamond": bool(ind["diamond"][-1]),
            "ssl2_cont": "buy" if ind["buy_atr"][-1] else ("sell" if ind["sell_atr"][-1] else None),
            "last_arrow": ad, "last_arrow_t": at, "bars_since_arrow": (n - 1 - ai) if ai is not None else None, "arrow_now": int(ind["arrow"][-1]),
            "exit_state": int(ind["ex_st"][-1]), "atr14": float(a[-1]), "atr_pct": float(a[-1]) / c, "color": color,
            "dist_upper_pct": up / c - 1, "dist_lower_pct": lo / c - 1, "dist_base_pct": base / c - 1,
            "last_signal": ld, "last_signal_t": lt, "bars_since_signal": (n - 1 - li) if li is not None else None,
            "fresh": li is not None and n - 1 - li <= 1}

def chart_payload(df, ind, res, bars=400):
    k = max(0, len(df) - bars); r4 = lambda a: [None if (x is None or (isinstance(x, float) and math.isnan(x))) else float(f"{x:.6g}") for x in a]
    return {"t": [int(x) for x in df.t.values[k:]], "o": r4(df.o.values[k:]), "h": r4(df.h.values[k:]), "l": r4(df.l.values[k:]), "c": r4(df.c.values[k:]),
            "base": r4(ind["base"][k:]), "upper": r4(ind["upper"][k:]), "lower": r4(ind["lower"][k:]), "exit": r4(ind["ex_line"][k:]),
            "L": [int(df.t.values[i]) for i in np.where(ind["long_sig"])[0] if i >= k], "S": [int(df.t.values[i]) for i in np.where(ind["short_sig"])[0] if i >= k],
            "ssl1": r4(ind["ssl1_line"][k:]), "ssl2": r4(ind["ssl2_line"][k:]), "atr_up": r4(ind["atr_up"][k:]), "atr_lo": r4(ind["atr_lo"][k:]),
            "AU": [int(df.t.values[i]) for i in np.where(ind["arrow"] > 0)[0] if i >= k], "AD": [int(df.t.values[i]) for i in np.where(ind["arrow"] < 0)[0] if i >= k],
            "D": [int(df.t.values[i]) for i in np.where(ind["diamond"])[0] if i >= k],
            "fills": res["fills"] if res else []}

def portfolio(results, cap, start_ms, pts=None):
    st = [r for r in results if r["started"]]
    k = len(st); rest = cap - sum(r["alloc"] for r in st)
    tm = {}
    for r in ([] if pts is not None else st):
        for t, v, b in zip(r["eq_t"], r["eq_v"], r["bh_v"]):
            e = tm.setdefault(t, [0.0, 0.0, 0]); e[0] += v; e[1] += b; e[2] += 1
    if pts is None: pts = [(t, e[0] + rest, e[1] + rest) for t, e in sorted(tm.items()) if e[2] == k]
    else: pts = [(t, v, b) for (t, v, b) in sorted({p[0]: p for p in pts}.values())]
    if not pts: pts = [(start_ms, cap, cap)]
    eqv = [p[1] for p in pts]; pk = -1e18; mdd = 0.0; mdd_usd = 0.0
    for v in eqv:
        pk = max(pk, v); mdd = min(mdd, v / pk - 1); mdd_usd = max(mdd_usd, pk - v)
    trades = sorted([t for r in results for t in r["trades"]], key=lambda x: x["exit_t"])
    wins = [t["pnl"] for t in trades if t["pnl"] > 0]; losses = [t["pnl"] for t in trades if t["pnl"] <= 0]
    gl = -sum(losses)
    tot_bars = sum(r["bars_total"] for r in st)
    return {"equity": [{"t": p[0], "eq": round(p[1], 6), "bh": round(p[2], 6)} for p in pts],
            "balance_close": eqv[-1], "bh_close": pts[-1][2], "max_dd_pct": mdd, "max_dd_usd": mdd_usd,
            "trades": trades, "n_trades": len(trades), "wins": len(wins), "losses": len(losses),
            "win_rate": (len(wins) / len(trades)) if trades else None,
            "profit_factor": (sum(wins) / gl) if gl > 0 else (None if not wins else 99.0),
            "avg_win": (sum(wins) / len(wins)) if wins else None, "avg_loss": (sum(losses) / len(losses)) if losses else None,
            "best": max(trades, key=lambda x: x["pnl"]) if trades else None, "worst": min(trades, key=lambda x: x["pnl"]) if trades else None,
            "fees": sum(r["fees"] for r in results), "exposure_pct": (sum(r["bars_in_pos"] for r in st) / tot_bars) if tot_bars else 0.0,
            "realised_pnl": sum(t["pnl"] for t in trades)}

# ---------------------------------------------------------------- multi-timeframe state_long engine (switchable 15m/1h/4h)
TFS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}

def seg_frm(tf, from_ms):
    B = TFS[tf]; return -(-from_ms // B) * B - PRE_BARS * B

def coin_segments(store, coin, segs, start_ms, cap, now, pauses):
    """Replay one coin through the timeframe segments. Segment 0 starts at the start time (entry at the first bar open
    of its timeframe if bull). Each later segment starts at a logged switch: the logged switch fills (live price) are
    applied as-is, then decisions are made on every close of that segment's timeframe and filled at the next bar open.
    Long-only: target = LONG while the baseline state is bull, cash while bear (neutral keeps the state)."""
    res = {"coin": coin, "alloc": cap, "started": False, "trades": [], "fills": [], "pos": None, "eq_t": [], "eq_v": [], "bh_v": [],
           "value": cap, "cash": cap, "fees": 0.0, "pending": None, "bars_in_pos": 0, "bars_total": 0, "skipped": [], "mode": segs[-1].get("mode", "state_long")}
    cash, units, ent, c0 = cap, 0.0, None, None; md = "state_long"
    df = ind = None; last_px = None; last_skip = None
    def buy(tm, px, tf, why, sig_t):
        nonlocal cash, units, ent
        fee = cash * FEE; units = (cash - fee) / px
        ent = {"t": tm, "px": px, "cost": cash, "fee": fee, "tf": tf, "side": "long", "sig_t": sig_t, "mode": md}; cash -= fee
        res["fills"].append({"coin": coin, "side": "BUY", "t": tm, "px": px, "fee": fee, "sig_t": sig_t, "value": cash, "tf": tf, "why": why, "mode": md})
    def sell(tm, px, tf, why, sig_t):
        nonlocal cash, units, ent
        gross = units * px; fee = gross * FEE; cash = gross - fee
        tr = {"coin": coin, "side": "long", "entry_t": ent["t"], "entry_px": ent["px"], "exit_t": tm, "exit_px": px, "cost": ent["cost"], "proceeds": cash,
              "pnl": cash - ent["cost"], "ret": cash / ent["cost"] - 1, "fee": ent["fee"] + fee, "entry_fee": ent["fee"], "exit_fee": fee,
              "bars": round((tm - ent["t"]) / BAR, 2), "entry_tf": ent["tf"], "exit_tf": tf, "entry_mode": ent.get("mode", "state_long"), "exit_mode": md}
        res["trades"].append(tr); res["fees"] += ent["fee"] + fee
        res["fills"].append({"coin": coin, "side": "SELL", "t": tm, "px": px, "fee": fee, "sig_t": sig_t, "pnl": tr["pnl"], "value": cash, "tf": tf, "why": why, "mode": md})
        units = 0.0; ent = None
    def mark(tm, px):
        res["eq_t"].append(int(tm)); res["eq_v"].append(units * px if ent else cash); res["bh_v"].append(cap * px / c0)
    for k, sg in enumerate(segs):
        tf = sg["tf"]; md = sg.get("mode", "state_long"); B = TFS[tf]; a = sg["from_ms"]; b = segs[k + 1]["from_ms"] if k + 1 < len(segs) else None
        rows = store.get(coin + "USDT", tf, seg_frm(tf, a))
        opens = {r[0]: r[1] for r in rows if r[0] <= now}
        df = closed_df(rows, now)
        if len(df) < WARMUP + 60:
            if k == 0: res["error"] = "not enough data"; return res, None, df
            continue
        ind = core.indicator(df); pst = seg_state(ind, md); tv = df.t.values; cl = df.c.values
        if k == 0:
            t0 = -(-a // B) * B; res["first_fill_ms"] = int(t0)
            w = np.where(tv == t0 - B)[0]
            if not len(w) or w[0] < WARMUP: return res, ind, df
            res["started"] = True; first = int(w[0]); c0 = float(cl[first]); res["bh_c0"] = c0; res["t_start"] = int(t0)
            idx = range(first, len(tv))
        else:
            if c0 is None: continue
            sy = (sg.get("sync") or {}).get(coin); spx = (sg.get("px") or {}).get(coin)
            why = f"switch {sg.get('from_tf')}→{tf}" if sg.get("from_tf") != tf or not sg.get("from_mode") else f"switch mode {sg.get('from_mode')}→{md}"
            if sy and sy["side"] == "BUY" and not ent: buy(a, sy["px"], tf, why, a)
            elif sy and sy["side"] == "SELL" and ent: sell(a, sy["px"], tf, why, a)
            if spx: mark(a, spx); last_px = spx
            idx = [i for i in range(len(tv)) if tv[i] + B > a]
        for i in idx:
            ct = int(tv[i]) + B
            if b is not None and ct >= b: break
            mark(ct, float(cl[i])); last_px = float(cl[i])
            tgt = 1 if pst[i] > 0 else 0; pos = 1 if ent else 0
            if tgt == pos: continue
            px = opens.get(ct)
            if px is None: res["pending"] = ("BUY" if tgt else "SELL (exit to cash)") + f" at the next {tf} bar open"; continue
            if tgt == 1:
                p = in_pause(ct, pauses)
                if p:
                    if last_skip != (k, i - 1): res["skipped"].append({"sig_t": int(tv[i]), "fill_t": ct, "side": "long", "tf": tf, "reason": p.get("reason")})
                    last_skip = (k, i); continue
                if md == "arrow_long": buy(ct, px, tf, "up arrow" if ind["arrow"][i] > 0 else "up-arrow state", int(tv[i]))
                else: buy(ct, px, tf, "long signal" if i > 0 and pst[i - 1] <= 0 else "bull state", int(tv[i]))
            else:
                sell(ct, px, tf, ("down arrow" if ind["arrow"][i] < 0 else "down-arrow state") if md == "arrow_long" else "short signal", int(tv[i]))
            last_px = px
    if not res["started"]: return res, ind, df
    if ent:
        vc = units * last_px
        res["pos"] = {"side": "long", "entry_t": ent["t"], "entry_px": ent["px"], "units": units, "collateral": 0.0, "cost": ent["cost"], "entry_fee": ent["fee"],
                      "bars": round((now - ent["t"]) / TFS[segs[-1]["tf"]], 2), "close_px": last_px, "value_close": vc, "tf": ent["tf"]}
        res["fees"] += ent["fee"]; res["value"] = vc; res["cash"] = 0.0
    else: res["value"] = cash; res["cash"] = cash
    held = sum(tr["exit_t"] - tr["entry_t"] for tr in res["trades"]) + ((now - ent["t"]) if ent else 0)
    res["bars_in_pos"], res["bars_total"] = held / BAR, max(1e-9, (now - res["t_start"]) / BAR)
    if not ent and in_pause(now, pauses) and ind is not None and seg_state(ind, md)[-1] > 0:
        res["pending"] = ("up-arrow" if md == "arrow_long" else "bull") + " state, BUY blocked while the bot is paused"
    res["state_now"] = int(seg_state(ind, md)[-1]) if ind is not None else 0
    res["color_now"] = int(ind["color"][-1]) if ind is not None else 0
    res["tf"] = segs[-1]["tf"]
    return res, ind, df

def run_engine(store, start_ms, cap, coins, now, pauses=(), beat=lambda: None, mode=DEFAULT_MODE, segs=None, exits=()):
    if mode in SEG_MODES and pool_index(segs) is not None:
        return run_pool(RunStore(store), start_ms, cap, coins, now, pauses, beat, segs, exits)
    if mode in SEG_MODES:
        segs = segs or [{"tf": "15m", "from_ms": start_ms, "mode": mode}]; cc = cap / len(coins); out = {}
        for c in coins:
            res, ind, df = coin_segments(store, c, segs, start_ms, cc, now, pauses); beat()
            out[c] = {"res": res, "ind": ind, "df": df}
        return out
    t0 = -(-start_ms // BAR) * BAR; frm = t0 - PRE_BARS * BAR
    cc = cap / len(coins); out = {}
    for c in coins:
        df = closed_df(store.get(c + "USDT", "15m", frm), now)
        res, ind = paper_coin(df, start_ms, cc, c, pauses, mode); beat()
        out[c] = {"res": res, "ind": ind, "df": df}
    return out

# ---------------------------------------------------------------- shared-pool sizing (config "sizing": "pool")
# One cash pool = the balance carried over at the switch; at most max_positions longs. A fresh up arrow (arrow_long) or
# a fresh bull flip (state_long) on a free slot buys cash/free_slots at the next bar open; more candidates than free
# slots on the same bar -> the larger (close - exit line)/ATR14 wins (tie: coin name); the others are skipped (logged,
# never queued). A down arrow (or bear state) sells the coin back to the pool at the next bar open.
# Take profit / stop loss (tp_usd / sl_usd, per position, net of the entry fee and the 0.1% exit fee):
#   tp_px = (cost + tp_usd) / (units * (1 - FEE)),  sl_px = (cost - sl_usd) / (units * (1 - FEE))
#   live: the bot polls Binance every 5 s; a price at/through a level closes the position AT THE LEVEL PRICE (the observed
#         price is logged too) and the exit is appended to tpsl_exits.jsonl, which the deterministic replay applies as-is.
#   catch-up (bars with no logged live exit, e.g. the bot was down or a wick fell between polls): bar low <= sl_px -> SL,
#         else bar high >= tp_px -> TP (SL checked first); filled at the level, or at the bar open if the bar gapped through it.
#   After a TP/SL exit the coin stays in cash and needs a NEW up arrow to re-enter.
POOL_DEFAULT_MAX = 2

def pool_index(segs):
    for k, g in enumerate(segs or []):
        if g.get("sizing") == "pool": return k
    return None

def tpsl_levels(units, cost, tp, sl):
    k = units * (1 - FEE)
    if k <= 0: return None, None
    return ((cost + tp) / k if tp else None), ((cost - sl) / k if sl and cost > sl else None)

def strength_at(df, ind, i):
    a, ex, c = float(ind["atr"][i]), float(ind["ex_line"][i]), float(df.c.values[i])
    return (c - ex) / a if a > 0 and math.isfinite(a) and math.isfinite(ex) else -1e9

class RunStore:
    """Per-run memo over KStore (one incremental fetch per symbol/timeframe per engine run)."""
    def __init__(self, s): self.s = s; self.m = {}
    def get(self, sym, tf, frm):
        k = (sym, tf)
        if k not in self.m or self.m[k][0] > frm: self.m[k] = (frm, self.s.get(sym, tf, frm))
        return [r for r in self.m[k][1] if r[0] >= frm]

class PoolOut(dict):
    pool = None; pts = None

def run_pool(store, start_ms, cap, coins, now, pauses, beat, segs, exits=()):
    p0 = pool_index(segs); T = segs[p0]["from_ms"]; cc = cap / len(coins)
    R = {}; P = {}; cash = 0.0; pts = []; bh_c0 = {}; last_px = {}; sw_log = []; pool_skips = []
    pre_tm = {}
    for c in coins:
        r, ind, df = coin_segments(store, c, segs[:p0], start_ms, cc, T, pauses); beat()
        R[c] = r
        if r.get("bh_c0"): bh_c0[c] = r["bh_c0"]
        for t, v, b in zip(r["eq_t"], r["eq_v"], r["bh_v"]):
            e = pre_tm.setdefault(t, [0.0, 0.0, 0]); e[0] += v; e[1] += b; e[2] += 1
        if r.get("pos"):
            p = r["pos"]; P[c] = {"units": p["units"], "cost": p["cost"], "entry_t": p["entry_t"], "entry_px": p["entry_px"], "fee": p["entry_fee"],
                                  "tf": p.get("tf"), "mode": "arrow_long", "lv_from": p["entry_t"]}
            r["fees"] -= p["entry_fee"]; last_px[c] = p.get("close_px")
        else: cash += r["cash"]
        r["pos"] = None; r["eq_t"], r["eq_v"], r["bh_v"] = [], [], []; r["pending"] = None
    k_st = sum(1 for c in coins if R[c]["started"])
    pts = [(t, e[0], e[1]) for t, e in sorted(pre_tm.items()) if e[2] == k_st]
    def bh_of(prices):
        return sum(cc * prices[c] / bh_c0[c] for c in coins if c in bh_c0 and prices.get(c)) + cc * sum(1 for c in coins if c not in bh_c0)
    def mark(t, prices):
        pts.append((int(t), cash + sum(P[c]["units"] * prices.get(c, last_px.get(c) or 0) for c in P), bh_of(prices)))
    def buy(c, tm, px, amt, tf, why, sig_t, md, kind="BUY"):
        nonlocal cash
        fee = amt * FEE; u = (amt - fee) / px; cash -= amt
        if kind == "RESIZE":
            p = P[c]; p["entry_px"] = (p["units"] * p["entry_px"] + u * px) / (p["units"] + u); p["units"] += u; p["cost"] += amt; p["fee"] += fee; p["lv_from"] = tm
        else:
            P[c] = {"units": u, "cost": amt, "entry_t": tm, "entry_px": px, "fee": fee, "tf": tf, "mode": md, "sig_t": sig_t, "lv_from": tm}
        R[c]["fills"].append({"coin": c, "side": "BUY", "t": tm, "px": px, "fee": fee, "sig_t": sig_t, "value": amt - fee, "size": amt, "units": u,
                              "tf": tf, "why": why, "mode": md, "sizing": "pool", "kind": "resize" if kind == "RESIZE" else "entry"})
    def sell(c, tm, px, tf, why, reason, sig_t, md, observed=None):
        nonlocal cash
        p = P.pop(c); gross = p["units"] * px; fee = gross * FEE; pr = gross - fee; cash += pr
        tr = {"coin": c, "side": "long", "entry_t": p["entry_t"], "entry_px": p["entry_px"], "exit_t": tm, "exit_px": px, "cost": p["cost"], "proceeds": pr,
              "pnl": pr - p["cost"], "ret": pr / p["cost"] - 1, "fee": p["fee"] + fee, "entry_fee": p["fee"], "exit_fee": fee,
              "bars": round((tm - p["entry_t"]) / BAR, 2), "entry_tf": p.get("tf"), "exit_tf": tf, "entry_mode": p.get("mode", "arrow_long"), "exit_mode": md,
              "exit_reason": reason, "sizing": "pool", "units": p["units"]}
        if observed is not None: tr["observed_px"] = observed
        R[c]["trades"].append(tr); R[c]["fees"] += p["fee"] + fee
        R[c]["fills"].append({"coin": c, "side": "SELL", "t": tm, "px": px, "fee": fee, "sig_t": sig_t, "pnl": tr["pnl"], "value": pr, "tf": tf, "why": why,
                              "mode": md, "sizing": "pool", "reason": reason, **({"observed_px": observed} if observed is not None else {})})
        last_px[c] = px
    EX = sorted([e for e in exits if e.get("coin") in coins], key=lambda e: e["t"])
    def logged(c, lo, hi):
        p = P.get(c)
        if not p: return None
        for e in EX:
            if e["coin"] == c and e.get("entry_t") == p["entry_t"] and lo <= e["t"] < hi and e["t"] >= p["lv_from"]: return e
        return None
    D = {}; md = segs[-1].get("mode", "arrow_long"); tf = segs[p0]["tf"]; tp = sl = None; maxp = POOL_DEFAULT_MAX
    for k in range(p0, len(segs)):
        sg = segs[k]; tf = sg["tf"]; md = sg.get("mode", "arrow_long"); B = TFS[tf]; a = sg["from_ms"]; b = segs[k + 1]["from_ms"] if k + 1 < len(segs) else None
        maxp = int(sg.get("max_positions") or POOL_DEFAULT_MAX); tp = sg.get("tp_usd"); sl = sg.get("sl_usd")
        D = {}
        for c in coins:
            rows = store.get(c + "USDT", tf, seg_frm(tf, a)); df = closed_df(rows, now)
            if len(df) < WARMUP + 60: continue
            ind = core.indicator(df)
            D[c] = {"df": df, "ind": ind, "st": seg_state(ind, md), "tix": {int(t): i for i, t in enumerate(df.t.values)}, "opens": {r[0]: r[1] for r in rows if r[0] <= now}}
        beat()
        spx = dict(sg.get("px") or {}); sync = sg.get("sync") or {}
        for c in coins: spx.setdefault(c, last_px.get(c))
        why = (f"switch {sg.get('from_tf')}→{tf}" if sg.get("from_tf") != tf else
               ("switch to pool sizing" if sg.get("resize") else (f"switch mode {sg.get('from_mode')}→{md}" if sg.get("from_mode") != md else "settings change")))
        eqv = lambda: cash + sum(P[c]["units"] * (spx.get(c) or 0) for c in P)
        before = eqv(); fees0 = sum(R[c]["fees"] for c in coins) + sum(P[c]["fee"] for c in P)
        for c in sorted(sync):
            if sync[c]["side"] == "SELL" and c in P: sell(c, a, sync[c]["px"], tf, why, "SWITCH", a, md)
        if sg.get("resize") and P:
            E = eqv()
            for c in sorted(P):
                gap = E / maxp - P[c]["units"] * spx[c]
                if gap > 1e-9 and cash > 1e-9: buy(c, a, spx[c], min(gap, cash), tf, "resize to 1/%d of the pool" % maxp, a, md, kind="RESIZE")
        for c in sg.get("buy_order", []):
            if (sync.get(c) or {}).get("side") == "BUY" and c not in P and len(P) < maxp and cash > 1e-9:
                buy(c, a, sync[c]["px"], cash / (maxp - len(P)), tf, why, a, md)
        after = eqv(); fees1 = sum(R[c]["fees"] for c in coins) + sum(P[c]["fee"] for c in P)
        if k > p0 or sg.get("resize"):
            sw_log.append({"at_ms": a, "at_wib": ms_wib(a), "why": why, "equity_before": before, "equity_after": after, "fees": fees1 - fees0,
                           "cash_after": cash, "held_after": sorted(P)})
        if any(spx.values()): mark(a, {c: v for c, v in spx.items() if v})
        for c, v in spx.items():
            if v: last_px[c] = v
        cts = sorted({int(t) + B for c in D for t in D[c]["df"].t.values if int(t) + B > a and (b is None or int(t) + B < b)})
        prev = a
        for ct in cts:
            bo = ct - B
            # 1) intrabar TP / SL inside the bar [bo, ct)
            for c in sorted(list(P)):
                lx = logged(c, max(bo, prev), ct)
                if lx: sell(c, lx["t"], lx["px"], tf, (f"{lx['kind']} (live, trigger level)" if lx['kind'] in ("TP", "SL") else f"{lx['kind']} close (live price)"), lx["kind"], lx["t"], md, lx.get("observed")); continue
                if not (tp or sl) or c not in D: continue
                i = D[c]["tix"].get(bo)
                if i is None or bo < P[c]["lv_from"]: continue
                tpx, slx = tpsl_levels(P[c]["units"], P[c]["cost"], tp, sl)
                o, h, l = (float(D[c]["df"][x].values[i]) for x in "ohl")
                if slx is not None and l <= slx: sell(c, ct, o if o <= slx else slx, tf, "SL (bar low" + (", gap: open)" if o <= slx else ")"), "SL", int(bo), md)
                elif tpx is not None and h >= tpx: sell(c, ct, o if o >= tpx else tpx, tf, "TP (bar high" + (", gap: open)" if o >= tpx else ")"), "TP", int(bo), md)
            prev = ct
            # 2) mark at the close
            cl = {}
            for c in coins:
                i = D[c]["tix"].get(bo) if c in D else None
                if i is not None: cl[c] = float(D[c]["df"].c.values[i]); last_px[c] = cl[c]
            mark(ct, {c: last_px.get(c) for c in coins})
            # 3) exits on the arrow (or bear state)
            for c in sorted(list(P)):
                i = D[c]["tix"].get(bo) if c in D else None
                if i is None or D[c]["st"][i] > 0: continue
                px = D[c]["opens"].get(ct)
                if px is None: R[c]["pending"] = f"SELL (exit to cash) at the next {tf} bar open"; continue
                ind = D[c]["ind"]
                sell(c, ct, px, tf, ("down arrow" if ind["arrow"][i] < 0 else "down-arrow state") if md == "arrow_long" else "short signal", "ARROW", int(bo), md)
                last_px[c] = px
            # 4) entries on a fresh up arrow / bull flip, best (close - exit)/ATR first
            cand = []
            for c in D:
                if c in P: continue
                i = D[c]["tix"].get(bo)
                if i is None: continue
                ind = D[c]["ind"]; st = D[c]["st"]
                fresh = ind["arrow"][i] > 0 if md == "arrow_long" else (st[i] > 0 and (i == 0 or st[i - 1] <= 0))
                if fresh: cand.append((-strength_at(D[c]["df"], ind, i), c, i))
            for ng, c, i in sorted(cand):
                free = maxp - len(P); sk = {"sig_t": int(bo), "fill_t": ct, "side": "long", "tf": tf, "strength": -ng}
                if free <= 0: R[c]["skipped"].append(dict(sk, reason="slots full", held=sorted(P))); pool_skips.append(dict(sk, coin=c, reason="slots full", held=sorted(P))); continue
                pz = in_pause(ct, pauses)
                if pz: R[c]["skipped"].append(dict(sk, reason=pz.get("reason"))); continue
                px = D[c]["opens"].get(ct)
                if px is None: R[c]["pending"] = f"BUY at the next {tf} bar open (pool: {free} free slot{'s' if free > 1 else ''})"; continue
                buy(c, ct, px, cash / free, tf, "up arrow" if md == "arrow_long" else "long signal", int(bo), md)
                last_px[c] = px
        # live TP/SL exits after the last closed bar of this segment (the bar still forming, or up to the next switch)
        hi = b if b is not None else now + 1
        for c in sorted(list(P)):
            lx = logged(c, prev, hi)
            if lx: sell(c, lx["t"], lx["px"], tf, (f"{lx['kind']} (live, trigger level)" if lx['kind'] in ("TP", "SL") else f"{lx['kind']} close (live price)"), lx["kind"], lx["t"], md, lx.get("observed"))
    # results in the per-coin shape the daemon already writes
    out = PoolOut(); B = TFS[tf]
    for c in coins:
        r = R[c]; d = D.get(c) or {}
        ind, df = d.get("ind"), d.get("df")
        lp = last_px.get(c)
        if df is not None and len(df): lp = float(df.c.values[-1]) if not P.get(c) or lp is None else lp
        if c in P:
            p = P[c]; tpx, slx = tpsl_levels(p["units"], p["cost"], tp, sl); vc = p["units"] * (lp or p["entry_px"])
            r["pos"] = {"side": "long", "entry_t": p["entry_t"], "entry_px": p["entry_px"], "units": p["units"], "collateral": 0.0, "cost": p["cost"],
                        "entry_fee": p["fee"], "bars": round((now - p["entry_t"]) / B, 2), "close_px": lp, "value_close": vc, "tf": p.get("tf"),
                        "tp_px": tpx, "sl_px": slx, "tp_usd": tp, "sl_usd": sl, "resized_at": p["lv_from"] if p["lv_from"] != p["entry_t"] else None}
            r["fees"] += p["fee"]; r["value"] = vc
        else: r["value"] = 0.0
        r["cash"] = 0.0; r["alloc"] = cc
        held = sum(t["exit_t"] - t["entry_t"] for t in r["trades"]) + ((now - P[c]["entry_t"]) if c in P else 0)
        if r.get("t_start"): r["bars_in_pos"], r["bars_total"] = held / BAR, max(1e-9, (now - r["t_start"]) / BAR)
        if ind is not None:
            r["state_now"] = int(seg_state(ind, md)[-1]); r["color_now"] = int(ind["color"][-1])
        r["tf"] = tf; r["mode"] = md; r["sizing"] = "pool"
        out[c] = {"res": r, "ind": ind, "df": df if df is not None else pd.DataFrame(columns=["t", "o", "h", "l", "c", "v", "ct"])}
    free = maxp - len(P)
    slots = [{"coin": c, "units": P[c]["units"], "cost": P[c]["cost"], "entry_t": P[c]["entry_t"], "entry_px": P[c]["entry_px"],
              "value_close": out[c]["res"]["pos"]["value_close"], "tp_px": out[c]["res"]["pos"]["tp_px"], "sl_px": out[c]["res"]["pos"]["sl_px"]} for c in sorted(P, key=lambda x: P[x]["entry_t"])]
    out.pool = {"sizing": "pool", "max_positions": maxp, "cash": cash, "slots_used": len(P), "free_slots": free,
                "next_trade_size": (cash / free) if free > 0 else 0.0, "slots": slots, "tp_usd": tp, "sl_usd": sl,
                "since_ms": T, "since_wib": ms_wib(T), "switches": sw_log, "skipped": pool_skips[-200:], "n_skipped": len(pool_skips)}
    out.pts = pts
    return out

# ---------------------------------------------------------------- daemon
class Bot:
    def __init__(self, d, start_wib, cap, coins, mode=DEFAULT_MODE, cfg=None):
        self.trade_mode = mode; cfg = cfg or {}
        self.sizing = cfg.get("sizing", "coin") if cfg.get("sizing") in ("coin", "pool") else "coin"
        self.max_pos = int(cfg.get("max_positions") or POOL_DEFAULT_MAX)
        self.tp_usd = float(cfg["tp_usd"]) if cfg.get("tp_usd") else None; self.sl_usd = float(cfg["sl_usd"]) if cfg.get("sl_usd") else None
        self._tpsl_done = set()
        self.d = d; os.makedirs(os.path.join(d, "data"), exist_ok=True)
        self.start_wib, self.start_ms, self.cap, self.coins = start_wib, wib_to_ms(start_wib), cap, coins
        self.store = KStore(os.path.join(d, "data"))
        self.p = lambda f: os.path.join(d, f)
        self.boot = now_ms(); self.bars_done = 0
        self.seen = set(); self.fill_px = {}; self.watch = {}; self.watch_bar = {}
        try:
            st = json.load(open(self.p("state.json")))
            sm = st.get("trade_mode", "cross_long")
            if sm != mode and not (sm in SEG_MODES and mode in SEG_MODES): raise ValueError("other mode")
            self.seen = set(st.get("_seen", [])); self.fill_px = st.get("_first_trade_px", {})
        except Exception: pass
        self.last = None; self.live = {}; self._beat_t = 0; self.paused = None
        self.segs = [{"tf": "15m", "from_ms": self.start_ms, "mode": mode}]
        try:
            sj = json.load(open(self.p("segments.json")))
            same = sj.get("mode") == mode or (sj.get("mode") in SEG_MODES and mode in SEG_MODES)
            if sj.get("start_ms") == self.start_ms and same and sj.get("segments"):
                self.segs = sj["segments"]; self.req_handled = sj.get("req_handled", 0)
                for g in self.segs: g.setdefault("mode", sj.get("mode") if sj.get("mode") in SEG_MODES else "state_long")
        except Exception: pass
        try:
            self.state = json.load(open(self.p("state.json")))
            sm = self.state.get("trade_mode", "cross_long")
            if self.state.get("start_ms") != self.start_ms or (sm != mode and not (sm in SEG_MODES and mode in SEG_MODES)): self.state = None
        except Exception: self.state = None

    def exits(self):
        out = []
        try:
            for l in open(self.p("tpsl_exits.jsonl")):
                l = l.strip()
                if l:
                    try: out.append(json.loads(l))
                    except Exception: pass
        except FileNotFoundError: pass
        return out
    @property
    def seg_sizing(self): return self.segs[-1].get("sizing", "coin")
    def run(self, now, beat=None):
        return run_engine(self.store, self.start_ms, self.cap, self.coins, now, pausectl.history(), beat or self.beat, self.trade_mode, self.segs, self.exits())
    def event(self, kind, msg, key=None, **kw):
        if key and key in self.seen: return
        if key: self.seen.add(key)
        row = {"ts": now_ms(), "ts_wib": ms_wib(now_ms()), "kind": kind, "msg": msg, **kw}
        append_jsonl(self.p("events.jsonl"), row)
        (log.warning if kind == "error" else log.info)(f"[{kind}] {msg}")

    def first_trade_px(self, coin, t):
        k = f"{coin}|{t}"
        if k not in self.fill_px:
            try:
                a = api("/api/v3/aggTrades", {"symbol": coin + "USDT", "startTime": t, "endTime": t + 60_000, "limit": 1})
                self.fill_px[k] = float(a[0]["p"]) if a else None
            except Exception as e:
                log.warning(f"aggTrades {coin} {t}: {e}"); return None
        return self.fill_px[k]

    def beat(self):
        if now_ms() - self._beat_t >= 20_000: self.write_live()
    def check_pause(self):
        cur = pausectl.current()
        if cur and not self.paused:
            self.event("pause", f"PAUSED: no new entries from now ({cur.get('reason')}); open positions still exit on the rules", key=f"pa|{cur['since_ms']}", since_ms=cur["since_ms"], reason=cur.get("reason"))
        elif not cur and self.paused:
            self.event("resume", "RESUMED: new entries allowed again", key=f"re|{self.paused['since_ms']}")
        changed = (cur or {}).get("since_ms") != (self.paused or {}).get("since_ms")
        self.paused = cur
        return changed
    @property
    def tf(self): return self.segs[-1]["tf"] if self.trade_mode in SEG_MODES else "15m"
    @property
    def seg_mode(self): return self.segs[-1].get("mode", "state_long")
    @property
    def B(self): return TFS[self.tf]
    def save_segs(self):
        write_json(self.p("segments.json"), {"mode": self.trade_mode, "start_ms": self.start_ms, "segments": self.segs, "req_handled": getattr(self, "req_handled", 0)})
    def check_tf(self):
        try: req = json.load(open(self.p("run/tf_request.json")))
        except Exception: return False
        new = req.get("tf"); at = int(req.get("at_ms") or 0)
        if at <= getattr(self, "req_handled", 0) or at < self.start_ms: return False   # requests are handled once, by time (never written back: no race with the server)
        self.req_handled = at
        if new == self.tf or new not in TFS: self.save_segs(); return False
        if self.trade_mode not in SEG_MODES:
            self.event("error", f"timeframe switch to {new} ignored: only the state_long / arrow_long modes support switching"); return False
        self.do_switch(new); return True
    def archive_mode(self, old_mode, n):
        """Snapshot the outgoing mode's logs to archive/<mode>_<YYYYmmdd_HHMM>/ (the live files keep the full, mode-tagged run)."""
        import shutil
        d = self.p(f"archive/{old_mode}_{datetime.fromtimestamp(n / 1000, WIB):%Y%m%d_%H%M}"); os.makedirs(os.path.join(d, "data"), exist_ok=True)
        for f in ("state.json", "trades.jsonl", "equity.jsonl", "events.jsonl", "summary.json", "live.json", "segments.json", "config.json", "run/pauses.json", "tpsl_exits.jsonl"):
            if os.path.exists(self.p(f)): shutil.copy2(self.p(f), os.path.join(d, os.path.basename(f)))
        for c in self.coins:
            f = self.p(f"data/chart_{c}.json")
            if os.path.exists(f): shutil.copy2(f, os.path.join(d, "data"))
        return d
    def check_mode(self):
        """config.json mode / sizing / max_positions / tp_usd / sl_usd differ from the active segment -> live switch
        (balance and timeframe carried over; a mode or sizing change archives the old logs first)."""
        if self.trade_mode not in SEG_MODES: return False
        g = self.segs[-1]
        if self.seg_mode != self.trade_mode or (self.sizing == "pool" and self.seg_sizing != "pool"):
            old = self.seg_mode; tag = old if self.seg_mode != self.trade_mode else f"{old}_per_coin"
            d = self.archive_mode(tag, now_ms())
            self.event("mode", f"MODE {old}/{self.seg_sizing} sizing → {self.trade_mode}/{self.sizing} sizing" + (f" (max {self.max_pos} positions, TP ${self.tp_usd:g} / SL ${self.sl_usd:g})" if self.sizing == "pool" and self.tp_usd and self.sl_usd else "")
                       + f": old logs archived to {os.path.relpath(d, self.d)}; balance and the {self.tf} timeframe carry over", key=f"m|{now_ms()}")
            self.do_switch(self.tf, self.trade_mode); return True
        if self.seg_sizing == "pool" and self.sizing != "pool":
            self.event("error", "config sizing 'coin' ignored: switching a live run back from pool to per-coin sizing is not supported (start a fresh run instead)", key=f"szerr|{g['from_ms']}"); return False
        if self.seg_sizing == "pool" and (int(g.get("max_positions") or POOL_DEFAULT_MAX) != self.max_pos or g.get("tp_usd") != self.tp_usd or g.get("sl_usd") != self.sl_usd):
            self.event("mode", f"POOL settings: max {g.get('max_positions')}→{self.max_pos} positions, TP {g.get('tp_usd')}→{self.tp_usd}, SL {g.get('sl_usd')}→{self.sl_usd}", key=f"ps|{now_ms()}")
            self.do_switch(self.tf, self.trade_mode); return True
        return False
    def do_switch(self, new, new_mode=None):
        old = self.tf; old_mode = self.seg_mode; new_mode = new_mode or old_mode; n = now_ms(); pauses = pausectl.history()
        cur = self.run(n)
        try:
            px = api("/api/v3/ticker/price", {"symbols": json.dumps([c + "USDT" for c in self.coins], separators=(",", ":"))})
            live = {q["symbol"][:-4]: float(q["price"]) for q in px}
        except Exception as e:
            live = {}; log.warning(f"switch live price: {e}")
        n = now_ms()
        seg = {"tf": new, "from_tf": old, "mode": new_mode, "from_mode": old_mode, "from_ms": n, "from_wib": ms_wib(n), "px": {}, "state": {}, "sync": {}, "skipped": []}
        paused = pausectl.current() is not None
        if self.sizing == "pool" or self.seg_sizing == "pool":
            return self.do_switch_pool(seg, cur, live, n, paused, old, new, old_mode, new_mode)
        for c in self.coins:
            rows = self.store.get(c + "USDT", new, seg_frm(new, n)); df = closed_df(rows, n)
            if len(df) < WARMUP + 60: continue
            st = int(seg_state(core.indicator(df), new_mode)[-1]); p = live.get(c) or float(df.c.values[-1])
            seg["px"][c] = p; seg["state"][c] = st
            inpos = bool(cur[c]["res"].get("pos")); started = cur[c]["res"].get("started")
            if not started: continue
            if st > 0 and not inpos:
                if paused: seg["skipped"].append(c)
                else: seg["sync"][c] = {"side": "BUY", "px": p}
            elif st <= 0 and inpos: seg["sync"][c] = {"side": "SELL", "px": p}
        self.segs.append(seg); self.save_segs()
        what = f"TIMEFRAME {old} → {new}" if new != old else f"MODE {old_mode} → {new_mode} on {new}"
        self.event("switch", f"{what} at {ms_wib(n)} WIB: " + (", ".join(f"{c} {v['side']} @ {v['px']:.6g}" for c, v in seg["sync"].items()) or "no fills needed (positions already match the new state)")
                   + (f"; BUY blocked (paused) for {', '.join(seg['skipped'])}" if seg["skipped"] else ""), key=f"tf|{n}", tf=new, from_tf=old, mode=new_mode, from_mode=old_mode)
        self.process_bar(watch=False)
    def do_switch_pool(self, seg, cur, live, n, paused, old, new, old_mode, new_mode):
        """Pool re-sync on a timeframe / mode / sizing switch: the coins whose latest arrow on the new timeframe is up are
        'long'; more than max_positions -> keep the strongest ((close - exit line)/ATR on the last closed bar). Held coins
        outside that set are sold, the others are bought with cash/free_slots (sells first). A coin closed by TP/SL after its
        latest up arrow is not re-bought (it needs a new arrow). Coming from per-coin sizing, kept positions are topped up
        to equity/max_positions (a paper 'resize' buy with fee), the rest stays as pool cash."""
        maxp = self.max_pos; seg.update(sizing="pool", max_positions=maxp, tp_usd=self.tp_usd, sl_usd=self.sl_usd, from_sizing=self.seg_sizing,
                                         resize=self.seg_sizing != "pool", strength={})
        held = [c for c in self.coins if cur[c]["res"].get("pos")]; longs = []
        for c in self.coins:
            rows = self.store.get(c + "USDT", new, seg_frm(new, n)); df = closed_df(rows, n)
            if len(df) < WARMUP + 60: continue
            ind = core.indicator(df); st = int(seg_state(ind, new_mode)[-1]); p = live.get(c) or float(df.c.values[-1])
            seg["px"][c] = p; seg["state"][c] = st; s = strength_at(df, ind, len(df) - 1); seg["strength"][c] = round(s, 4)
            if not cur[c]["res"].get("started"): continue
            if st > 0:
                ups = np.where(ind["arrow"] > 0)[0] if new_mode == "arrow_long" else np.where((ind["long_sig"]))[0]
                up_t = int(df.t.values[ups[-1]]) + TFS[new] if len(ups) else 0
                tpsl_after = any(t.get("exit_reason") in ("TP", "SL") and t["exit_t"] >= up_t for t in cur[c]["res"]["trades"])
                if tpsl_after and c not in held: seg["skipped"].append(c + " (TP/SL exit since its latest up arrow)"); continue
                longs.append((-s, c))
        desired = [c for _, c in sorted(longs)][:maxp]
        for c in held:
            if c not in desired: seg["sync"][c] = {"side": "SELL", "px": seg["px"].get(c) or cur[c]["res"]["pos"]["close_px"]}
        order = []
        for c in desired:
            if c in held: continue
            if paused: seg["skipped"].append(c + " (paused)"); continue
            seg["sync"][c] = {"side": "BUY", "px": seg["px"][c]}; order.append(c)
        seg["buy_order"] = order
        dropped = [c for _, c in sorted(longs)][maxp:]
        if dropped: seg["skipped"] += [c + " (slots full: weaker cross)" for c in dropped]
        self.segs.append(seg); self.save_segs()
        what = f"TIMEFRAME {old} → {new}" if new != old else (f"MODE {old_mode} → {new_mode} on {new}" if old_mode != new_mode else f"POOL sizing on {new}")
        self.event("switch", f"{what} at {ms_wib(n)} WIB · pool max {maxp} positions" + (f", TP ${self.tp_usd:g} / SL ${self.sl_usd:g}" if self.tp_usd or self.sl_usd else "") + ": "
                   + (", ".join(f"{c} {v['side']} @ {v['px']:.6g}" for c, v in seg["sync"].items()) or "no sync fills")
                   + (f"; resize kept positions ({', '.join(c for c in held if c in desired)}) to 1/{maxp} of equity" if seg["resize"] and any(c in desired for c in held) else "")
                   + (f"; skipped: {', '.join(seg['skipped'])}" if seg["skipped"] else ""), key=f"tf|{n}", tf=new, from_tf=old, mode=new_mode, from_mode=old_mode)
        self.process_bar(watch=False)
        for s in (self.state.get("pool") or {}).get("switches", []):
            if s["at_ms"] == n:
                self.event("resize" if seg["resize"] else "switch", f"pool at the switch: equity ${s['equity_before']:.4f} before → ${s['equity_after']:.4f} after (fees ${s['fees']:.4f}); cash ${s['cash_after']:.2f}, held {', '.join(s['held_after']) or 'none'}", key=f"rs|{n}")
    def check_tpsl(self):
        """Live TP/SL: called after every 5 s price poll. A price at/through a level closes the position at the level price."""
        pool = (self.state or {}).get("pool")
        if not pool or not self.live or not (pool.get("tp_usd") or pool.get("sl_usd")): return False
        hit = False; t = getattr(self, "live_t", now_ms())
        for s in pool.get("slots", []):
            c = s["coin"]; px = self.live.get(c); key = (c, s["entry_t"])
            if px is None or key in self._tpsl_done: continue
            kind = "SL" if s.get("sl_px") and px <= s["sl_px"] else ("TP" if s.get("tp_px") and px >= s["tp_px"] else None)
            if not kind: continue
            lvl = s["sl_px"] if kind == "SL" else s["tp_px"]
            if any(e["coin"] == c and e.get("entry_t") == s["entry_t"] for e in self.exits()): self._tpsl_done.add(key); continue
            append_jsonl(self.p("tpsl_exits.jsonl"), {"coin": c, "entry_t": s["entry_t"], "t": t, "t_wib": ms_wib(t), "kind": kind, "px": lvl, "observed": px,
                                                     "units": s["units"], "cost": s["cost"], "tp_px": s.get("tp_px"), "sl_px": s.get("sl_px")})
            self._tpsl_done.add(key); hit = True
            pnl = s["units"] * lvl * (1 - FEE) - s["cost"]
            self.event("tpsl", f"{c}: {kind} hit live at {ms_wib(t)} WIB (price {px:.6g}, level {lvl:.6g}) → paper SELL at the level, net P&L ${pnl:+.2f}; slot freed, re-entry needs a new up arrow",
                       key=f"x|{c}|{s['entry_t']}", coin=c, side=kind)
        if hit: self.process_bar(watch=False)
        return hit
    def update_watch(self, now):
        for tf, iv in WATCH_TFS.items():
            cur_bar = now // iv
            if self.watch_bar.get(tf) == cur_bar: continue
            ok = 0
            for c in WATCH:
                self.beat()
                try:
                    if tf == self.tf and c in self.coins and self.last and c in self.last and self.last[c]["ind"] is not None:
                        df = self.last[c]["df"]; ind = self.last[c]["ind"]
                    else:
                        df = closed_df(klines(c + "USDT", tf, None, 500), now)
                        if len(df) < WARMUP + 60: self.watch.setdefault(c, {})[tf] = {"n/a": True}; continue
                        ind = core.indicator(df)
                    ro = readout(df, ind, c); prev = self.watch.get(c, {}).get(tf)
                    self.watch.setdefault(c, {})[tf] = {k: ro[k] for k in ("color", "close", "upper", "lower", "exit_state", "last_signal", "last_signal_t", "bars_since_signal", "fresh", "bar_t", "dist_upper_pct", "dist_lower_pct", "last_arrow", "last_arrow_t", "bars_since_arrow")}
                    if ro["fresh"] and ro["bars_since_signal"] == 0:
                        self.event("watch_flip", f"{c} {tf}: fresh {ro['last_signal']} flip on the bar closing {ms_wib(ro['bar_t'] + iv)} WIB", key=f"w|{c}|{tf}|{ro['last_signal_t']}", coin=c, tf=tf, side=ro["last_signal"])
                    ok += 1
                except Exception as e:
                    self.event("error", f"watchlist {c} {tf}: {e}", coin=c, tf=tf)
            self.watch_bar[tf] = cur_bar
            log.info(f"watchlist {tf}: {ok}/{len(WATCH)} coins")

    def process_bar(self, watch=True):
        B = self.B; now = now_ms(); expect = now // B * B - B   # open time of the active-timeframe bar that just closed
        for attempt in range(6):
            out = self.run(now)
            lag = [c for c, v in out.items() if len(v["df"]) and int(v["df"].t.values[-1]) < expect]
            if not lag: break
            log.info(f"bar {ms_wib(expect)} not yet available for {lag}; retry"); time.sleep(3); now = now_ms()
        self.last = out
        # events: signals since the start, fills
        for c, v in out.items():
            r, ind, df = v["res"], v["ind"], v["df"]
            if ind is None: continue
            tv = df.t.values
            seg0 = self.segs[-1]["from_ms"] if self.trade_mode in SEG_MODES else self.start_ms
            if self.seg_mode == "arrow_long":
                for i in np.where(ind["arrow"] != 0)[0]:
                    t = int(tv[i])
                    if t + B <= seg0: continue
                    side = "up" if ind["arrow"][i] > 0 else "down"
                    self.event("signal", f"{c}: {'▲ up (BUY)' if side == 'up' else '▼ down (SELL)'} arrow on the {self.tf} bar {ms_wib(t)}–{ms_wib(t + B)[11:16]} WIB (close {df.c.values[i]:.6g}, exit line {ind['ex_line'][i]:.6g})", key=f"a|{self.tf}|{c}|{t}", coin=c, side=side, bar_t=t, tf=self.tf, arrow=True)
            for i in (np.where(ind["long_sig"] | ind["short_sig"])[0] if self.seg_mode != "arrow_long" else []):
                t = int(tv[i])
                if t + B <= seg0: continue   # only signals on bars closing after the (segment) start
                side = "long" if ind["long_sig"][i] else "short"
                self.event("signal", f"{c}: {side} signal on the {self.tf} bar {ms_wib(t)}–{ms_wib(t + B)[11:16]} WIB (close {df.c.values[i]:.6g})", key=f"s|{self.tf}|{c}|{t}", coin=c, side=side, bar_t=t, tf=self.tf)
            for f in r["fills"]:
                ft = self.first_trade_px(c, f["t"]); f["first_trade_px"] = ft
                f["slippage_bps"] = ((ft / f["px"] - 1) * 1e4) if ft else None
                self.event("fill", f"{c}: paper {('RESIZE ' if f.get('kind') == 'resize' else '') + f['side']}{(' ' + f['reason']) if f.get('reason') in ('TP', 'SL', 'MANUAL') else ''}{(' $' + format(f['size'], '.2f')) if f.get('size') else ''} [{f.get('tf', '15m')}{' · ' + f['why'] if f.get('why') else ''}] at {f['px']:.6g} ({'live price' if str(f.get('why','')).startswith(('switch', 'resize')) else ('trigger level' if f.get('reason') in ('TP', 'SL') else ('live price' if f.get('reason') == 'MANUAL' else 'bar open'))} {ms_wib(f['t'])} WIB; first trade {ft if ft is not None else 'n/a'}, slippage {f['slippage_bps'] if f['slippage_bps'] is not None else 'n/a'} bps), fee ${f['fee']:.4f}" + (f", P&L ${f['pnl']:.2f}" if 'pnl' in f else ""),
                           key=f"f|{c}|{f['t']}|{f['side']}" + ("|rs" if f.get("kind") == "resize" else ""), reason=f.get("reason"), tf=f.get("tf", "15m"), coin=c, side=f["side"], px=f["px"], fill_t=f["t"], first_trade_px=ft, slippage_bps=f["slippage_bps"])
            for sk in r.get("skipped", []):
                if sk.get("reason") == "slots full":
                    self.event("skip", f"{c}: ▲ up arrow on the {sk.get('tf', self.tf)} bar {ms_wib(sk['sig_t'])[11:16]} WIB skipped: slots full ({'+'.join(sk.get('held') or [])} held, strength {sk.get('strength', 0):.2f} ATR); not queued, a later up arrow can enter", key=f"k|{c}|{sk['fill_t']}", coin=c, reason="slots full")
                else:
                    self.event("skip", f"{c}: {sk.get('side', 'long')} entry at the {ms_wib(sk['fill_t'])} WIB open NOT taken, bot paused ({sk.get('reason')})", key=f"k|{c}|{sk['fill_t']}", coin=c)
            if r.get("pending"): self.event("pending", f"{c}: {r['pending']}", key=f"p|{c}|{int(tv[-1])}", coin=c)
        self.bars_done += 1
        self.write_all(now)   # publish the paper results first; the watchlist (display only) follows
        if watch:
            try: self.update_watch(now)
            except Exception as e: self.event("error", f"watchlist: {e}")
            self.write_all(now)
        last_bar = max(int(v["df"].t.values[-1]) for v in out.values() if len(v["df"]))
        self.event("bar", f"processed {self.tf} bar {ms_wib(last_bar)} WIB · balance ${self.state['balance_close']:.2f}", key=f"b|{self.tf}|{last_bar}", bar_t=last_bar, tf=self.tf)

    def tick_live(self):
        try:
            px = api("/api/v3/ticker/price", {"symbols": json.dumps([c + "USDT" for c in self.coins], separators=(",", ":"))})
            self.live = {p["symbol"][:-4]: float(p["price"]) for p in px}; self.live_t = now_ms()
        except Exception as e:
            log.warning(f"live price: {e}")
        self.write_live()

    def write_all(self, now):
        out = self.last; res = [out[c]["res"] for c in self.coins]
        pool = getattr(out, "pool", None)
        pf = portfolio(res, self.cap, self.start_ms, getattr(out, "pts", None))
        coins = []
        for c in self.coins:
            v = out[c]; r = v["res"]
            ro = readout(v["df"], v["ind"], c) if v["ind"] is not None else {}
            coins.append({"coin": c, "alloc": r["alloc"], "started": r["started"], "value_close": r["value"], "cash": r["cash"], "fees": r["fees"],
                          "pos": r["pos"], "pending": r["pending"], "n_trades": len(r["trades"]), "pnl_realised": sum(t["pnl"] for t in r["trades"]),
                          "bh_value_close": (r["alloc"] * float(v["df"].c.values[-1]) / r["bh_c0"]) if r.get("bh_c0") else r["alloc"],
                          "bh_c0": r.get("bh_c0"), "exposure_pct": (r["bars_in_pos"] / r["bars_total"]) if r["bars_total"] else 0.0, "ind": ro,
                          "fills": r["fills"], "sizing": r.get("sizing", "coin")})
            write_json(self.p(f"data/chart_{c}.json"), chart_payload(v["df"], v["ind"], r) if v["ind"] is not None else {})
        last_bar = max(int(out[c]["df"].t.values[-1]) for c in self.coins if len(out[c]["df"]))
        self.state = {"mode": "PAPER SIMULATION - no real orders, no API keys", "trade_mode": self.trade_mode, "trade_mode_text": MODES[self.trade_mode], "active_tf": self.tf, "segments": self.segs, "mode_switches": [{"at_ms": g["from_ms"], "at_wib": g.get("from_wib"), "from": g.get("from_mode") + ("/" + g.get("from_sizing", "coin") if g.get("sizing") else ""), "to": g.get("mode") + ("/pool max %s" % g.get("max_positions") if g.get("sizing") == "pool" else ""), "tf": g["tf"], "fills": g.get("sync")} for g in self.segs[1:] if g.get("from_mode") and (g.get("from_mode") != g.get("mode") or (g.get("sizing") or "coin") != (g.get("from_sizing") or "coin"))], "sizing": pool["sizing"] if pool else "coin", "max_positions": pool["max_positions"] if pool else None, "pool": pool,
                      "sizing_text": (f"Max positions {pool['max_positions']} · pool sizing" + (f" · TP ${pool['tp_usd']:g} / SL ${pool['sl_usd']:g}" if pool.get("tp_usd") or pool.get("sl_usd") else "")) if pool else "Per-coin sizing (each coin compounds its own $100 slice)",
                      "rules": {"tf": self.tf, "sizing": ("shared pool, max %d open longs; size = cash / free slots (strongest (close-exit)/ATR first, extra arrows skipped, not queued)" % pool["max_positions"]) if pool else "per coin, $100 slice each", "tp_usd": pool.get("tp_usd") if pool else None, "sl_usd": pool.get("sl_usd") if pool else None, "tpsl": "net of the entry fee and the 0.1% exit fee; live 5 s price checks fill at the trigger level; catch-up on bar high/low, SL first" if pool and (pool.get("tp_usd") or pool.get("sl_usd")) else None, "direction": "long+short" if self.trade_mode == "state_ls" else "long-only", "entry": ("Exit Arrow up (close crosses above the Exit SSL HMA15)" if self.trade_mode == "arrow_long" else "follow baseline state (enter at start)") if self.trade_mode != "cross_long" else "fresh crossover", "exit": "Exit Arrow down (close crosses below the Exit SSL)" if self.trade_mode == "arrow_long" else "baseline (opposite state)", "fee_per_side": FEE, "compounding": True, "fill": "next bar open (Binance kline open = first trade of the bar)"},
                      "start_wib": self.start_wib, "start_ms": self.start_ms, "first_fill_ms": -(-self.start_ms // BAR) * BAR, "capital": self.cap, "coins_traded": self.coins,
                      "last_bar_t": last_bar, "last_bar_wib": ms_wib(last_bar), "last_bar_close_wib": ms_wib(last_bar + self.B), "processed_at": now_ms(), "processed_at_wib": ms_wib(now_ms()),
                      "boot_ms": self.boot, "bars_processed_since_boot": self.bars_done, "source": SRC["name"],
                      "balance_close": pf["balance_close"], "bh_close": pf["bh_close"], "pnl_close": pf["balance_close"] - self.cap,
                      "stats": {k: pf[k] for k in ("max_dd_pct", "max_dd_usd", "n_trades", "wins", "losses", "win_rate", "profit_factor", "avg_win", "avg_loss", "best", "worst", "fees", "exposure_pct", "realised_pnl")},
                      "paused": self.paused, "pauses": pausectl.history(), "skipped_entries": [dict(s, coin=c) for c in self.coins for s in out[c]["res"].get("skipped", [])],
                      "coins": coins, "watch": self.watch, "_seen": sorted(self.seen), "_first_trade_px": self.fill_px}
        write_json(self.p("state.json"), self.state)
        write_jsonl(self.p("trades.jsonl"), [dict(t, entry_wib=ms_wib(t["entry_t"]), exit_wib=ms_wib(t["exit_t"]), exit_reason=t.get("exit_reason") or ("SWITCH" if t.get("exit_t") in {g["from_ms"] for g in self.segs} else "ARROW")) for t in pf["trades"]])
        write_jsonl(self.p("equity.jsonl"), [dict(e, wib=ms_wib(e["t"])) for e in pf["equity"]])
        self.tick_live()

    def marked(self):
        st = self.state; bal = float((st.get("pool") or {}).get("cash") or 0.0); bh = 0.0; coins = {}
        for c in st["coins"]:
            px = self.live.get(c["coin"]); p = c["pos"]
            if p and px: val = p["units"] * px if p.get("side", "long") == "long" else core.cash_short_val(p["collateral"], p["units"], px)
            else: val = c["value_close"]
            bhv = (c["alloc"] * px / c["bh_c0"]) if (px and c.get("bh_c0")) else c["bh_value_close"]
            coins[c["coin"]] = {"price": px, "value": val, "bh_value": bhv, "unreal": (val - p["cost"]) if p else None, "unreal_pct": (val / p["cost"] - 1) if p else None}
            if p and p.get("units"):
                pr = px or p.get("close_px"); coins[c["coin"]].update(net_pnl=p["units"] * pr * (1 - FEE) - p["cost"], tp_px=p.get("tp_px"), sl_px=p.get("sl_px"),
                    to_tp_pct=(p["tp_px"] / pr - 1) if p.get("tp_px") and pr else None, to_sl_pct=(p["sl_px"] / pr - 1) if p.get("sl_px") and pr else None)
            bal += val; bh += bhv
        return bal, bh, coins

    def write_live(self):
        if not getattr(self, "state", None): return
        self._beat_t = now_ms()
        bal, bh, coins = self.marked(); n = now_ms()
        nxt = (n // self.B + 1) * self.B
        live = {"heartbeat_ms": n, "heartbeat_wib": ms_wib(n), "pid": os.getpid(), "boot_ms": self.boot, "uptime_s": (n - self.boot) / 1000,
                "next_bar_close_ms": nxt, "next_bar_close_wib": ms_wib(nxt), "source": SRC["name"], "prices_t": getattr(self, "live_t", None),
                "paused": self.paused, "state": "PAUSED" if self.paused else "RUNNING", "active_tf": self.tf, "bar_ms": self.B,
                "balance": bal, "pnl": bal - self.cap, "pnl_pct": bal / self.cap - 1, "bh_balance": bh, "coins": coins,
                "pool_cash": (self.state.get("pool") or {}).get("cash"), "sizing": self.state.get("sizing", "coin")}
        write_json(self.p("live.json"), live)
        st = self.state; s = st["stats"]
        summary = {"generated_wib": ms_wib(n), "mode": "PAPER SIMULATION", "trade_mode": self.trade_mode, "trade_mode_text": MODES[self.trade_mode], "active_tf": self.tf, "next_bar_close_wib": ms_wib(nxt),
                   "tf_switches": [{"at_wib": s.get("from_wib"), "from": s.get("from_tf"), "to": s["tf"], "fills": s.get("sync")} for s in self.segs[1:] if s.get("from_tf") != s["tf"]],
                   "mode_switches": st.get("mode_switches", []), "status": "paused" if self.paused else "running",
                   "bot_state": "PAUSED" if self.paused else "RUNNING", "status_detail": (f"paused since {ms_wib(self.paused['since_ms'])} WIB: {self.paused.get('reason')}" if self.paused else f"heartbeat {ms_wib(n)} WIB"),
                   "heartbeat_wib": ms_wib(n), "paused_since_wib": ms_wib(self.paused["since_ms"]) if self.paused else None, "start_wib": st["start_wib"], "capital": st["capital"],
                   "balance": round(bal, 4), "pnl_usd": round(bal - self.cap, 4), "pnl_pct": round(bal / self.cap - 1, 6), "buy_hold_balance": round(bh, 4),
                   "balance_at_last_close": round(st["balance_close"], 4), "last_bar_close_wib": st["last_bar_close_wib"],
                   "trades": s["n_trades"], "win_rate": s["win_rate"], "profit_factor": s["profit_factor"], "fees": round(s["fees"], 4), "max_dd_pct": s["max_dd_pct"], "exposure_pct": s["exposure_pct"],
                   "sizing": st.get("sizing", "coin"), "sizing_text": st.get("sizing_text"),
                   "pool": {k: v for k, v in (st.get("pool") or {}).items() if k not in ("skipped", "switches")} or None,
                   "open_positions": [{"coin": c["coin"], "side": c["pos"].get("side", "long").upper(), "entry_wib": ms_wib(c["pos"]["entry_t"]), "entry_px": c["pos"]["entry_px"], "price": coins[c["coin"]]["price"],
                                       "unreal_usd": coins[c["coin"]]["unreal"], "unreal_pct": coins[c["coin"]]["unreal_pct"], "bars": c["pos"]["bars"], "cost": c["pos"]["cost"], "units": c["pos"]["units"],
                                       "net_pnl": coins[c["coin"]].get("net_pnl"), "tp_px": c["pos"].get("tp_px"), "sl_px": c["pos"].get("sl_px"),
                                       "to_tp_pct": coins[c["coin"]].get("to_tp_pct"), "to_sl_pct": coins[c["coin"]].get("to_sl_pct")} for c in st["coins"] if c["pos"]],
                   "pending": [{"coin": c["coin"], "action": c["pending"]} for c in st["coins"] if c["pending"]],
                   "fresh_watchlist_flips_15m": [c for c, v in self.watch.items() if v.get("15m", {}).get("fresh")]}
        write_json(self.p("summary.json"), summary)

    def loop(self):
        open(self.p("live.json"), "a").close(); os.utime(self.p("live.json"))   # fresh mtime for the watchdog while the first bar is processed
        self.event("start", f"paper bot started (pid {os.getpid()}), mode {self.trade_mode} ({MODES[self.trade_mode]}), sizing {self.sizing}" + (f" max {self.max_pos} positions, TP ${self.tp_usd:g} / SL ${self.sl_usd:g}" if self.sizing == "pool" and self.tp_usd and self.sl_usd else "") + f", start {self.start_wib} WIB, ${self.cap:.0f} across {'+'.join(self.coins)}")
        stop = {"v": False}
        def term(*_): stop["v"] = True
        signal.signal(signal.SIGTERM, term); signal.signal(signal.SIGINT, term)
        next_bar = 0; last_hb = 0; last_px = 0
        self.check_pause()
        if self.state: self.write_live()
        try:
            self.check_mode()   # next_bar stays 0: the regular first pass (with the watchlist) runs right after
        except Exception as e:
            self.event("error", f"mode switch failed: {e}"); log.error(traceback.format_exc())
        while not stop["v"]:
            n = now_ms()
            if self.check_pause(): self.write_live()
            try:
                if self.check_tf():
                    lb = (self.state or {}).get("last_bar_t"); next_bar = (lb + 2 * self.B + 5000) if lb else 0
            except Exception as e:
                self.event("error", f"timeframe switch failed: {e}"); log.error(traceback.format_exc())
            if n >= next_bar:
                try:
                    self.process_bar()
                    # close of the bar after the one just processed + 5 s; if that bar already closed while we
                    # were busy (processing can take ~1-2 min), this is in the past and it is processed right away
                    lb = (self.state or {}).get("last_bar_t")
                    next_bar = (lb + 2 * self.B + 5000) if lb else (now_ms() // self.B + 1) * self.B + 5000
                except Exception as e:
                    self.event("error", f"bar processing failed: {e}"); log.error(traceback.format_exc())
                    next_bar = now_ms() + 30_000
            if n - last_px >= 5_000 and self.state and (self.state.get("pool") or {}).get("slots"):
                last_px = n
                try:
                    self.tick_live()
                    if self.check_tpsl():
                        lb = (self.state or {}).get("last_bar_t"); next_bar = max(next_bar, (lb + 2 * self.B + 5000) if lb else 0)
                except Exception as e:
                    self.event("error", f"live TP/SL check failed: {e}"); log.error(traceback.format_exc())
            if n - last_hb >= 30_000:
                self.tick_live(); last_hb = n
                if (n // 60_000) % 15 == 0 and getattr(self, "_hb_min", None) != n // 60_000:
                    self._hb_min = n // 60_000; self.event("heartbeat", f"alive, uptime {(n - self.boot) / 3.6e6:.2f} h, balance ${self.marked()[0]:.2f}")
            time.sleep(1)
        self.event("stop", "paper bot stopped (SIGTERM)")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=START_WIB); ap.add_argument("--capital", type=float, default=CAPITAL)
    ap.add_argument("--coins", default=",".join(COINS)); ap.add_argument("--dir", default=HERE)
    ap.add_argument("--once", action="store_true", help="process the latest closed bar once and exit (tests)")
    ap.add_argument("--no-watch", action="store_true")
    ap.add_argument("--mode", choices=list(MODES), default=None, help="default: config.json 'mode', else " + DEFAULT_MODE)
    a = ap.parse_args()
    try: cfg = json.load(open(os.path.join(a.dir, "config.json")))
    except Exception: cfg = {}
    if not a.mode: a.mode = cfg.get("mode") or DEFAULT_MODE
    if a.mode not in MODES: a.mode = DEFAULT_MODE
    os.makedirs(a.dir, exist_ok=True)
    h = RotatingFileHandler(os.path.join(a.dir, "bot.log"), maxBytes=5_000_000, backupCount=3)
    logging.basicConfig(level=logging.INFO, handlers=[h, logging.StreamHandler(sys.stdout)], format="%(asctime)s WIB %(levelname)s %(message)s")
    logging.Formatter.converter = lambda *x: datetime.now(WIB).timetuple()
    if cfg.get("signal_timing") and a.mode == "arrow_long" and cfg.get("sizing") == "pool" and cfg.get("coins"):
        import live_engine   # live pool engine: intrabar/close signals, config-driven universe (see live_engine.py)
        b = live_engine.LiveBot(a.dir, a.start, a.capital, [c.upper() for c in cfg["coins"]], a.mode, cfg)
    else:
        b = Bot(a.dir, a.start, a.capital, a.coins.split(","), a.mode, cfg)
    if a.no_watch: b.update_watch = lambda now: None
    if a.once: b.check_mode() or b.process_bar(); print(json.dumps(json.load(open(os.path.join(a.dir, "summary.json"))), indent=1)); return
    b.loop()

if __name__ == "__main__": main()
