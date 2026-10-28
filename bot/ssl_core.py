"""
SSL Hybrid (Mihkel00, TradingView) - Python port + backtest.
Source of truth: the current public TradingView script (Pine v6, "SSL Hybrid" by Mihkel00, updated 2025-08-05,
saved at /workspace/ssl_hybrid/ssl_hybrid_tv_v6.pine). Defaults:
  Baseline (BBMC/Keltma): HMA 60 of close; channel = Keltma +/- 0.2 * EMA60(ta.tr)        -> base, upper, lower, color
  SSL1:  HMA 60 of high / low; Hlv flips on close > HMA(high) / close < HMA(low); plotted sslDown = Hlv<0 ? HMA(high) : HMA(low)
  SSL2:  JMA 5 (phase 3, power 1) of high / low, same SSL logic                             -> ssl2_line (plotted circles)
  Exit:  HMA 15 of high / low, same SSL logic -> sslExit (ex_line); Exit Arrows = codiff:
         base_cross_Long  = ta.crossover(close, sslExit)  -> up arrow   (+1)
         base_cross_Short = ta.crossover(sslExit, close)  -> down arrow (-1)
  ATR:   WMA 14 of ta.tr(true), multiplier 1 -> +ATR/-ATR bands (v6 default smoothing is WMA; the old v4 copy used SMA)
  SSL2 continuation (atr_crit 0.9): buy_atr / sell_atr;  Signal Diamonds: |close-open| > ATR and the ATR band contains the baseline
  HMA = wma(2*wma(src, len/2) - wma(src, len), round(sqrt(len))); TradingView truncates the fractional length 15/2 = 7.5 to 7.
Backtest rules (research): long_sig/short_sig = close crosses the upper/lower channel; see backtest().
"""
import numpy as np, pandas as pd, json, glob, os, math

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DATA = os.path.join(ROOT, "data")
OUT = ROOT
FEE = 0.001
WARMUP = 120
BARS_PER_YEAR = {"15m": 365*96, "1h": 365*24, "4h": 365*6, "1d": 365, "1w": 52.14}
MIN_TRADES = 15

def wma(x, n):
    n = int(n); w = np.arange(1, n+1, dtype=float)
    out = np.full(len(x), np.nan)
    if len(x) < n: return out
    v = np.lib.stride_tricks.sliding_window_view(x, n)
    out[n-1:] = (v * w).sum(1) / w.sum()
    return out

def hma(x, n):
    # Pine: wma(2*wma(src, len/2) - wma(src, len), round(sqrt(len))) ; len/2 int-division in v4
    a = 2*wma(x, n//2) - wma(x, n)
    m = int(round(math.sqrt(n)))
    out = np.full(len(x), np.nan)
    ok = ~np.isnan(a)
    if ok.sum() >= m:
        idx = np.argmax(ok); out[idx:] = wma(a[idx:], m)
    return out

def ema(x, n):
    return pd.Series(x).ewm(span=n, adjust=False).mean().values

def true_range(h, l, c):
    pc = np.r_[np.nan, c[:-1]]
    tr = np.nanmax(np.vstack([h-l, np.abs(h-pc), np.abs(l-pc)]), axis=0)
    return tr

def jma(src, length=5, phase=3, power=1):
    phaseRatio = 0.5 if phase < -100 else (2.5 if phase > 100 else phase/100 + 1.5)
    beta = 0.45*(length-1)/(0.45*(length-1)+2); alpha = beta**power
    e0 = e1 = e2 = j = 0.0; out = np.zeros(len(src))
    for i, s in enumerate(src):
        e0 = (1-alpha)*s + alpha*e0
        e1 = (s-e0)*(1-beta) + beta*e1
        e2 = (e0 + phaseRatio*e1 - j)*(1-alpha)**2 + alpha**2*e2
        j = e2 + j; out[i] = j
    return out

def ssl_state(c, mh, ml):
    st = np.zeros(len(c)); s = 0
    for i in range(len(c)):
        if not np.isnan(mh[i]):
            if c[i] > mh[i]: s = 1
            elif c[i] < ml[i]: s = -1
        st[i] = s
    line = np.where(st < 0, mh, ml)
    return st, line

def wma_tr_atr(h, l, c, n=14):
    tr = true_range(h, l, c); tr[0] = h[0] - l[0]          # ta.tr(true): first bar = high - low
    return wma(tr, n)

def last_state(ev):
    """+1/-1 of the most recent non-zero event, 0 before the first one."""
    st = np.zeros(len(ev)); cur = 0
    for i, v in enumerate(ev):
        if v: cur = v
        st[i] = cur
    return st

def indicator(df):
    o, h, l, c = (df[k].values for k in "ohlc")
    base = hma(c, 60)
    rangema = ema(np.nan_to_num(true_range(h, l, c), nan=h[0]-l[0]), 60)
    upper, lower = base + 0.2*rangema, base - 0.2*rangema
    ex_st, ex_line = ssl_state(c, hma(h, 15), hma(l, 15))
    ssl1_st, ssl1_line = ssl_state(c, hma(h, 60), hma(l, 60))
    ssl2_st, ssl2_line = ssl_state(c, jma(h), jma(l))
    color = np.where(c > upper, 1, np.where(c < lower, -1, 0))
    pu, pl, pc = np.r_[np.nan, upper[:-1]], np.r_[np.nan, lower[:-1]], np.r_[np.nan, c[:-1]]
    long_sig = (c > upper) & (pc <= pu)
    short_sig = (c < lower) & (pc >= pl)
    pe = np.r_[np.nan, ex_line[:-1]]
    exit_long = (c < ex_line) & (pc >= pe)    # ta.crossover(sslExit, close) = base_cross_Short -> DOWN arrow (EXIT LONG)
    exit_short = (c > ex_line) & (pc <= pe)   # ta.crossover(close, sslExit) = base_cross_Long  -> UP arrow (EXIT SHORT)
    arrow = np.where(exit_short, 1, np.where(exit_long, -1, 0))   # Pine codiff (plotarrow "Exit Arrows")
    atr = wma_tr_atr(h, l, c, 14)
    atr_up, atr_lo = c + atr, c - atr
    in_range = (atr_up > base) & (atr_lo < base)
    diamond = (np.abs(c - o) > atr) & in_range                       # "Signal Diamonds" (candlesize_violation)
    buy_atr = (c - 0.9*atr < ssl2_line) & (c > base) & (c > ssl2_line)
    sell_atr = (c + 0.9*atr > ssl2_line) & (c < base) & (c < ssl2_line)
    return dict(base=base, upper=upper, lower=lower, ex_line=ex_line, ex_st=ex_st, color=color,
                long_sig=long_sig, short_sig=short_sig, exit_long=exit_long, exit_short=exit_short,
                ssl1=ssl1_st, ssl2=ssl2_st, ssl1_line=ssl1_line, ssl2_line=ssl2_line,
                arrow=arrow, arrow_state=last_state(arrow), atr=atr, atr_up=atr_up, atr_lo=atr_lo,
                diamond=diamond, buy_atr=buy_atr, sell_atr=sell_atr)

def arrow_ind(ind):
    """arrow_long research variant: up arrow = long signal, down arrow = exit to cash."""
    return dict(ind, long_sig=ind["arrow"] > 0, short_sig=ind["arrow"] < 0)

def backtest(df, ind, direction, exitv):
    o, c = df.o.values, df.c.values; n = len(c)
    pos = 0; target = 0
    eq = np.ones(n); cash = 1.0
    trades = []; entry_eq = None; entry_px = None
    units = 0.0
    events = []
    for i in range(WARMUP, n):
        # execute order decided at close of i-1 at open of i
        if i > WARMUP and target != pos:
            px = o[i]
            if pos != 0:  # close
                val = units*px if pos > 0 else cash_short_val(cash, units, px)
                val *= (1-FEE)
                trades.append(val/entry_eq - 1); events.append((i, "x"))
                cash = val; pos = 0
            if target != 0:
                entry_eq = cash; entry_px = px
                cash_after = cash*(1-FEE)
                units = cash_after/px
                cash = cash_after; pos = target; events.append((i, "L" if target > 0 else "S"))
        # mark to market at close
        if pos > 0: eq[i] = units*c[i]
        elif pos < 0: eq[i] = cash_short_val(cash, units, c[i])
        else: eq[i] = cash
        # decide target at close i
        ls, ss = ind["long_sig"][i], ind["short_sig"][i]
        t = pos
        if ls: t = 1
        elif ss: t = -1 if direction == "ls" else 0
        if exitv == "exitssl":
            if pos > 0 and ind["exit_long"][i] and not ls: t = 0 if not ss else t
            if pos < 0 and ind["exit_short"][i] and not ss: t = 0 if not ls else t
        target = t
    eq[:WARMUP] = 1.0
    # open trade counted at last close
    open_ret = (eq[-1]/entry_eq - 1) if pos != 0 else None
    return eq, trades, open_ret, pos, events

def cash_short_val(cash, units, px):
    # short with collateral `cash`, sold `units` at entry: value = cash + units*(entry - px) = 2*cash - units*px
    return max(2*cash - units*px, 0.0)

def metrics(df, eq, trades, open_ret, tf):
    e = eq[WARMUP:]; c = df.c.values; o = df.o.values
    allt = trades + ([open_ret] if open_ret is not None else [])
    years = (df.t.values[-1] - df.t.values[WARMUP]) / (365.25*86400000)
    tot = e[-1]/e[0] - 1
    bh = c[-1]/o[WARMUP] - 1
    cagr = (e[-1]/e[0])**(1/years) - 1 if years > 0 and e[-1] > 0 else -1
    peak = np.maximum.accumulate(e); mdd = (e/peak - 1).min()
    bpk = np.maximum.accumulate(c[WARMUP:]); bh_mdd = (c[WARMUP:]/bpk - 1).min()
    r = np.diff(e)/e[:-1]
    sharpe = r.mean()/r.std()*math.sqrt(BARS_PER_YEAR[tf]) if r.std() > 0 else 0
    br = np.diff(c[WARMUP:])/c[WARMUP:-1]
    bh_sharpe = br.mean()/br.std()*math.sqrt(BARS_PER_YEAR[tf]) if br.std() > 0 else 0
    w = [t for t in allt if t > 0]; L = [t for t in allt if t <= 0]
    pf = (sum(w)/-sum(L)) if L and sum(L) < 0 else (99.0 if w else 0)
    return dict(total_return=tot, bh_return=bh, cagr=cagr, max_dd=mdd, bh_max_dd=bh_mdd,
                win_rate=len(w)/len(allt) if allt else 0, profit_factor=pf, trades=len(allt),
                sharpe=sharpe, bh_sharpe=bh_sharpe, years=years,
                calmar=cagr/abs(mdd) if mdd < 0 else 0,
                start=pd.to_datetime(df.t.values[WARMUP], unit="ms").strftime("%Y-%m-%d"),
                end=pd.to_datetime(df.t.values[-1], unit="ms").strftime("%Y-%m-%d"))

def downsample(arr, k):
    idx = np.unique(np.linspace(0, len(arr)-1, k).round().astype(int)); return idx

def main():
    meta = json.load(open(f"{DATA}/meta.json"))
    syms = [s for s, _ in meta["symbols"]]
    rows = []; charts = {}; states = {}
    for tf in ["15m", "1h", "4h", "1d", "1w"]:
        for s in syms:
            f = f"{DATA}/{s}_{tf}.csv"
            if not os.path.exists(f): continue
            df = pd.read_csv(f)
            if len(df) < WARMUP + 60: continue
            ind = indicator(df)
            key = f"{tf}|{s}"
            ch = {"eq": {}}
            idx = downsample(np.arange(len(df)-WARMUP), 120) + WARMUP
            c0 = df.o.values[WARMUP]
            ch["eq"]["bh"] = [round(float(df.c.values[i]/c0*100), 1) for i in idx]
            ch["eqt"] = [int(df.t.values[i]//1000) for i in idx]
            posnow = {}
            for d in ["long", "ls"]:
                for ev in ["baseline", "exitssl"]:
                    eq, tr, orr, pos, events = backtest(df, ind, d, ev)
                    m = metrics(df, eq, tr, orr, tf)
                    m.update(symbol=s, coin=s[:-4], tf=tf, direction=d, exit=ev)
                    rows.append(m)
                    ch["eq"][f"{d}_{ev}"] = [round(float(eq[i]*100), 1) for i in idx]
                    posnow[f"{d}_{ev}"] = int(pos)
            # price window: last 150 bars
            N = 150; sl = slice(len(df)-N, len(df))
            ch["pt"] = [int(df.t.values[len(df)-N]//1000), int(df.t.values[-1]//1000)]
            def r4(a): return [float(f"{x:.5g}") if not np.isnan(x) else None for x in a[sl]]
            ch["c"] = r4(df.c.values); ch["b"] = r4(ind["base"]); ch["u"] = r4(ind["upper"]); ch["l"] = r4(ind["lower"]); ch["x"] = r4(ind["ex_line"])
            mk = []
            for j, i in enumerate(range(len(df)-N, len(df))):
                if ind["long_sig"][i]: mk.append([j, "L"])
                if ind["short_sig"][i]: mk.append([j, "S"])
                if ind["exit_long"][i]: mk.append([j, "xl"])
                if ind["exit_short"][i]: mk.append([j, "xs"])
            ch["m"] = mk
            # current state
            last = len(df)-1
            ls_idx = np.where(ind["long_sig"] | ind["short_sig"])[0]
            li = ls_idx[-1] if len(ls_idx) else None
            ch["st"] = dict(color=int(ind["color"][last]), exit=int(ind["ex_st"][last]), ssl2=int(ind["ssl2"][last]),
                            last_sig=("long" if ind["long_sig"][li] else "short") if li is not None else None,
                            bars_since=int(last-li) if li is not None else None,
                            last_close_t=int(df.t.values[last]//1000), pos=posnow)
            charts[key] = ch
        print(tf, "done", flush=True)
    res = pd.DataFrame(rows)
    # Ranking score (0-100): average of GLOBAL percentile ranks (across every eligible coin/timeframe/variant row)
    # of profit factor (capped at 5) and CAGR / |max drawdown|. Global ranks keep a losing group's "best" row from scoring high.
    res["eligible"] = res.trades >= MIN_TRADES
    res["score"] = np.nan
    el = res[res.eligible]
    pf = el.profit_factor.clip(upper=5).rank(pct=True); cal = el.calmar.rank(pct=True)
    res.loc[el.index, "score"] = ((pf + cal) / 2 * 100).round(1)
    res["beats_bh"] = res.total_return > res.bh_return
    res.to_csv(f"{OUT}/ssl_hybrid_results.csv", index=False)
    json.dump({"charts": charts}, open(f"{OUT}/charts.json", "w"), separators=(",", ":"))
    print(res.groupby(["tf", "direction", "exit"])[["total_return", "bh_return", "profit_factor", "trades", "sharpe", "beats_bh"]].median())

def arrow_report(tfs=("15m", "1h", "4h")):
    """arrow_long (buy on up Exit Arrow, cash on down arrow) vs the baseline-crossover long mode, fees included."""
    meta = json.load(open(f"{DATA}/meta.json")); syms = [s for s, _ in meta["symbols"]]; rows = []
    for tf in tfs:
        for s in syms:
            f = f"{DATA}/{s}_{tf}.csv"
            if not os.path.exists(f): continue
            df = pd.read_csv(f)
            if len(df) < WARMUP + 60: continue
            ind = indicator(df)
            for name, ii in (("baseline_long", ind), ("arrow_long", arrow_ind(ind))):
                eq, tr, orr, pos, events = backtest(df, ii, "long", "baseline")
                m = metrics(df, eq, tr, orr, tf); n = m["trades"]
                gross = float(np.prod([(1 + t) / (1 - FEE) ** 2 for t in tr + ([orr] if orr is not None else [])])) - 1 if n else 0.0
                m.update(symbol=s, coin=s[:-4], tf=tf, mode=name, fees_paid_pct=n * 2 * FEE, gross_return_no_fees=gross)
                rows.append(m)
    res = pd.DataFrame(rows); res.to_csv(f"{OUT}/arrow_long_results.csv", index=False)
    g = res.groupby(["tf", "mode"]).agg(median_return=("total_return", "median"), median_gross=("gross_return_no_fees", "median"),
                                         median_trades=("trades", "median"), median_pf=("profit_factor", "median"), median_bh=("bh_return", "median"),
                                         beats_bh=("total_return", lambda x: float((x > res.loc[x.index, "bh_return"]).mean())), years=("years", "median"))
    print(g.to_string()); return res

if __name__ == "__main__":
    import sys
    arrow_report() if "--arrow" in sys.argv else main()
