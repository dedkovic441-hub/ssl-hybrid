"""arrow_long backtest: per-coin $100 slices vs one shared pool (max N open longs, size = cash/free slots), optional net $ TP/SL.
Rules mirror paper_bot.py: signals at bar close, fills at next open, FEE 0.1%/side; pool picks the strongest (close-exit)/ATR when
more up arrows than free slots (rest skipped, not queued); down arrow (arrow_state<=0) exits; TP/SL on bar high/low, SL first,
fill at the level (or the open on a gap); after TP/SL a coin needs a NEW up arrow."""
import sys, json, numpy as np, pandas as pd
sys.path.insert(0, "/workspace/ssl-hybrid-bot")
from ssl_core import indicator, arrow_ind, backtest, FEE, WARMUP
DATA = "/workspace/ssl_hybrid/data"; COINS = ["ZEC", "NEAR", "PEPE", "SOL", "XRP"]; CAP = 500.0

def load(tf):
    dfs = {c: pd.read_csv(f"{DATA}/{c}USDT_{tf}.csv").drop_duplicates("t").set_index("t") for c in COINS}
    idx = sorted(set.intersection(*[set(d.index) for d in dfs.values()]))
    out = {}
    for c, d in dfs.items():
        d = d.loc[idx].reset_index(); out[c] = (d, indicator(d))
    return idx, out

def stats(t, eq, tf):
    eq = np.asarray(eq); s = pd.Series(eq, index=pd.to_datetime(np.asarray(t), unit="ms"))
    m = s.resample("30D").last().dropna(); mr = m.pct_change().dropna()
    mr = pd.concat([pd.Series([m.iloc[0] / CAP - 1]), mr]) if len(m) else mr
    dd = float((s / s.cummax() - 1).min())
    return dict(total=eq[-1] / CAP - 1, median_30d=float(mr.median()), max_dd=dd)

def per_coin(idx, D, tf):
    eqs = []; coin_ret = []; ntr = 0; wins = 0
    for c in COINS:
        df, ind = D[c]; eq, tr, orr, pos, ev = backtest(df, arrow_ind(ind), "long", "baseline")
        eqs.append(eq * CAP / len(COINS)); coin_ret.append(eq[-1] - 1); ntr += len(tr); wins += sum(x > 0 for x in tr)
    r = stats(idx[WARMUP:], np.sum(eqs, 0)[WARMUP:], tf)
    r.update(median_coin=float(np.median(coin_ret)), trades=ntr, win=wins / max(ntr, 1), skipped=0, tp=0, sl=0); return r

def pool(idx, D, tf, maxp=2, tp=0.0, sl=0.0):
    n = len(idx); cash = CAP; P = {}; eq = []; trades = []; skipped = 0; ntp = nsl = 0; pend_sell = set(); pend_buy = []
    O = {c: D[c][0].o.values for c in COINS}; H = {c: D[c][0].h.values for c in COINS}; L = {c: D[c][0].l.values for c in COINS}; C = {c: D[c][0].c.values for c in COINS}
    A = {c: D[c][1]["arrow"] for c in COINS}; AS = {c: D[c][1]["arrow_state"] for c in COINS}; ATR = {c: D[c][1]["atr"] for c in COINS}; EX = {c: D[c][1]["ex_line"] for c in COINS}
    def close(c, px, why):
        nonlocal cash; p = P.pop(c); v = p["u"] * px * (1 - FEE); cash += v; trades.append((v - p["cost"], why))
    for i in range(WARMUP, n):
        for c in sorted(pend_sell):
            if c in P: close(c, O[c][i], "ARROW")
        pend_sell = set()
        for c in pend_buy:
            free = maxp - len(P)
            if free <= 0 or c in P: skipped += 1; continue
            amt = cash / free; cash -= amt; u = amt * (1 - FEE) / O[c][i]; k = u * (1 - FEE)
            P[c] = dict(u=u, cost=amt, tp=(amt + tp) / k if tp else None, sl=(amt - sl) / k if sl and amt > sl else None)
        pend_buy = []
        for c in list(P):   # intrabar TP/SL, SL first
            p = P[c]
            if p["sl"] and L[c][i] <= p["sl"]: close(c, min(O[c][i], p["sl"]), "SL"); nsl += 1
            elif p["tp"] and H[c][i] >= p["tp"]: close(c, max(O[c][i], p["tp"]), "TP"); ntp += 1
        eq.append(cash + sum(p["u"] * C[c][i] for c, p in P.items()))
        if i == n - 1: break
        pend_sell = {c for c in P if AS[c][i] <= 0}
        cand = [c for c in COINS if A[c][i] > 0 and c not in P]
        cand.sort(key=lambda c: -((C[c][i] - EX[c][i]) / ATR[c][i] if ATR[c][i] > 0 else -1e9))
        free = maxp - (len(P) - len(pend_sell)); pend_buy = cand[:max(free, 0)]; skipped += len(cand) - len(pend_buy)
    pnl = [x for x, _ in trades]
    r = stats(idx[WARMUP:], eq, tf); r.update(median_coin=None, trades=len(trades), win=float(np.mean([x > 0 for x in pnl])) if pnl else 0, skipped=skipped, tp=ntp, sl=nsl)
    return r

if __name__ == "__main__":
    rows = []
    for tf in ("15m", "1h", "4h"):
        idx, D = load(tf)
        span = f"{pd.to_datetime(idx[WARMUP], unit='ms'):%Y-%m-%d}..{pd.to_datetime(idx[-1], unit='ms'):%Y-%m-%d}"
        for name, f in (("per-coin $100", lambda: per_coin(idx, D, tf)), ("pool max-2", lambda: pool(idx, D, tf)), ("pool max-2 TP5/SL2", lambda: pool(idx, D, tf, 2, 5, 2))):
            r = f(); r.update(tf=tf, variant=name, span=span); rows.append(r); print(tf, name, json.dumps({k: (round(v, 4) if isinstance(v, float) else v) for k, v in r.items()}), flush=True)
    pd.DataFrame(rows).to_csv("/workspace/poolwork/bt_pool_results.csv", index=False)
