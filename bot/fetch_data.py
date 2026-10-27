"""Fetch Binance spot klines (MEXC fallback) for top USDT pairs."""
import requests, time, json, os, sys
import pandas as pd
BASES = ["https://api.binance.com", "https://data-api.binance.vision"]
MEXC = "https://api.mexc.com"
OUT = "/workspace/ssl_hybrid/data"; os.makedirs(OUT, exist_ok=True)
STABLE = {"USDC","FDUSD","TUSD","BUSD","DAI","USDP","EUR","AEUR","EURI","USD1","XUSD","USDE","PAXG","RLUSD","BFUSD","U","WBTC","WBETH","USDS","PYUSD","FRAX","USTC","XAUT"}
# tokenized stocks / ETFs listed on Binance spot (not crypto)
STOCKS = {"SNDKB","SPCXB","SOXLB","CRCLB","MUB","MSTRB","SKHYB","TSLAB","SNXXB","NVDAB","AAPLB","COINB","HOODB","QQQB","SPYB","GOOGLB","METAB","AMZNB"}
LOOKBACK_DAYS = {"15m": 365 + 2, "1h": 3*365 + 5, "4h": 3*365 + 20, "1d": 3*365 + 120, "1w": 9*365}  # + warmup bars (120)
MS = {"15m": 9e5, "1h": 3.6e6, "4h": 1.44e7, "1d": 8.64e7, "1w": 6.048e8}

def get(url, params):
    for i in range(5):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200: return r.json()
            time.sleep(1 + i)
        except Exception: time.sleep(1 + i)
    raise RuntimeError(f"fail {url} {params}")

def pick_base():
    for b in BASES:
        try:
            r = requests.get(b + "/api/v3/ping", timeout=10)
            if r.status_code == 200: return b, "binance"
        except Exception: pass
    return MEXC, "mexc"

def top_symbols(base, n=30):
    tick = get(base + "/api/v3/ticker/24hr", {})
    info = get(base + "/api/v3/exchangeInfo", {"permissions": "SPOT"}) if "binance" in base or "vision" in base else get(base + "/api/v3/exchangeInfo", {})
    trading = {s["symbol"] for s in info["symbols"] if s.get("status") in ("TRADING", "1", 1, "ENABLED") and s.get("quoteAsset") == "USDT"}
    rows = []
    for t in tick:
        s = t["symbol"]
        if s not in trading or not s.endswith("USDT"): continue
        b = s[:-4]
        if b in STABLE or b in STOCKS or b.endswith(("UP", "DOWN", "BULL", "BEAR")) and len(b) > 4: continue
        rows.append((s, float(t["quoteVolume"])))
    rows.sort(key=lambda x: -x[1])
    return rows[:n]

def klines(base, sym, tf):
    end = int(time.time() * 1000)
    start = end - int(LOOKBACK_DAYS[tf] * 86400000)
    out = []; cur = start
    while cur < end:
        d = get(base + "/api/v3/klines", {"symbol": sym, "interval": tf, "startTime": cur, "limit": 1000})
        if not d: break
        out += d
        cur = int(d[-1][0] + MS[tf])
        if len(d) < 1000: break
        time.sleep(0.05)
    df = pd.DataFrame([r[:6] for r in out], columns=["t", "o", "h", "l", "c", "v"]).astype(float)
    df = df.drop_duplicates("t").sort_values("t")
    return df[df.t + MS[tf] <= end]  # closed bars only

if __name__ == "__main__":
    base, ex = pick_base(); print("source", base, ex, flush=True)
    syms = top_symbols(base)
    json.dump({"source": base, "exchange": ex, "symbols": syms, "fetched_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}, open(OUT + "/meta.json", "w"), indent=1)
    print(syms, flush=True)
    from concurrent.futures import ThreadPoolExecutor
    kbase = "https://data-api.binance.vision" if ex == "binance" else base
    def job(a):
        s, tf = a
        f = f"{OUT}/{s}_{tf}.csv"
        if os.path.exists(f): return
        try:
            df = klines(kbase, s, tf)
        except Exception:
            df = klines(base, s, tf)
        df.to_csv(f, index=False); print(s, tf, len(df), flush=True)
    with ThreadPoolExecutor(8) as p:
        list(p.map(job, [(s, tf) for s, _ in syms for tf in LOOKBACK_DAYS]))
