#!/usr/bin/env python3
"""
Live pool engine (epoch 1, from 2026-10-27 ~19:4x WIB): SSL Hybrid Exit Arrows, long-only, shared cash pool, low-cap universe.

Why a new engine: intrabar fills depend on live prices seen by the 5 s poller, so they cannot be re-derived from klines.
The run is therefore event-sourced: every paper fill is appended to ledger.jsonl and the state (cash, positions, trades)
is always the fold of epoch0.json (the frozen earlier run, balance carried over) + ledger.jsonl. Restarts replay the
ledger, then catch up bar by bar (bar-based rules) for the bars that closed while the bot was down.

config.json:
  signal_timing  "intrabar" (default here) | "close"
     intrabar: every ~5 s the indicator is recomputed on the FORMING candle with the live price as its close (live high/low =
               max/min of the forming kline and the polled prices). Up arrow = live close crosses above the Exit SSL (HMA15)
               versus the previous closed bar  -> BUY at once at the live price (reason ARROW_INTRABAR) if a slot is free.
               Down arrow intrabar on a held coin -> SELL at once (ARROW_INTRABAR). Max one entry and one exit per coin per
               candle; a coin exited in a candle is not re-entered in that candle. An arrow that was never seen intrabar but is
               on the closed bar (crossing in the last seconds) is acted on at the next bar open (ARROW_CLOSE). The TradingView
               arrow may disappear before close; such an entry is kept (exits only on a down arrow, TP or SL).
     close:    arrows on the closed bar -> fill at the next bar open (reason ARROW).
  coins            the universe (Binance spot USDT pairs)
  exclude_top_mcap 50: coins ranked 1..N by CoinGecko /coins/markets are dropped from NEW entries (checked at start + every 24 h;
                   a held coin still exits on its rules)
  max_positions  open-long slots. Entry size = equity / max_positions (equity = cash + held coins at the live price),
                 capped at the pool cash (from 2026-10-27 20:3x WIB; before: cash / free slots). When max_positions changes, the
                 bot at start trims held positions above equity / max_positions with a paper partial sell (ledger side RESIZE,
                 live price, 0.1% fee; entry price kept, cost basis + entry fee reduced pro rata, TP/SL recomputed for the smaller
                 size) and logs a live_child segment (from_max_positions -> max_positions) + a mode_switch event.
  tp_usd, sl_usd as before (TP/SL net of the entry fee and the 0.1% exit fee; live 5 s checks fill at the level;
                   catch-up on bar high/low, SL first).
Data: one /api/v3/ticker/price call for all symbols every 5 s; klines at candle boundaries + one coin's forming kline every 5 s
(round robin), i.e. far below Binance's 6000 weight/min.
"""
import os, json, time, math, threading, traceback
import numpy as np, pandas as pd, requests
import ssl_core as core
import pausectl
import paper_bot as pb
from paper_bot import (api, klines, closed_df, ms_wib, now_ms, write_json, write_jsonl, append_jsonl, readout, chart_payload,
                       portfolio, tpsl_levels, FEE, WARMUP, TFS, MODES, log)

TIMING_TEXT = {"intrabar": "intrabar (live)", "close": "on candle close"}

def jsonl_read(p):
    out = []
    try:
        for l in open(p):
            l = l.strip()
            if l:
                try: out.append(json.loads(l))
                except Exception: pass
    except FileNotFoundError: pass
    return out

def live_exit(df, o, h, l, c, tail=600):
    """Exit SSL on the forming bar (closed bars + one forming row). Returns arrow (+1/-1/0), exit line, strength (c-ex)/ATR."""
    d = df.iloc[-tail:] if len(df) > tail else df
    H = np.r_[d.h.values, h]; L = np.r_[d.l.values, l]; C = np.r_[d.c.values, c]
    st, ex = core.ssl_state(C, core.hma(H, 15), core.hma(L, 15))
    up = C[-1] > ex[-1] and C[-2] <= ex[-2]; dn = C[-1] < ex[-1] and C[-2] >= ex[-2]
    atr = core.wma_tr_atr(H, L, C, 14)[-1]
    s = (C[-1] - ex[-1]) / atr if atr > 0 and math.isfinite(atr) and math.isfinite(ex[-1]) else -1e9
    return (1 if up else (-1 if dn else 0)), float(ex[-1]), float(s)

def top_mcap_symbols(n):
    r = requests.get("https://api.coingecko.com/api/v3/coins/markets", params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": max(n, 1), "page": 1},
                     headers={"User-Agent": "ssl-hybrid-paper-bot/1.0"}, timeout=20)
    r.raise_for_status()
    return {c["symbol"].upper(): {"rank": c.get("market_cap_rank"), "id": c["id"], "mcap": c.get("market_cap")} for c in r.json()[:n]}

class LiveBot(pb.Bot):
    def __init__(self, d, start_wib, cap, coins, mode, cfg):
        super().__init__(d, start_wib, cap, coins, mode, cfg)
        self.cfg = cfg
        self.timing = cfg.get("signal_timing", "intrabar") if cfg.get("signal_timing") in ("intrabar", "close") else "intrabar"
        self.universe = [c.upper() for c in (cfg.get("coins") or [])]
        self.excl_n = int(cfg.get("exclude_top_mcap") or 0)
        self.excluded = {}
        self.epoch0 = None; self.ep = None
        self.forming = {}      # coin -> {"bo","o","h","l"}
        self.seen = set(self.seen)
        self.rr = 0; self.D = {}; self.watch_busy = False
        self.cover_from = now_ms()
        try: ev = json.load(open(self.p("run/intrabar_eval.json")))
        except Exception: ev = {}
        self.flags = ev.get("flags", {})         # "COIN|bar_t" -> "up"/"down" seen intrabar
        self.last_ct = ev.get("last_ct")         # close time of the last processed bar (active tf)
        self.last_eval = ev.get("last_eval_ms")

    # ------------------------------------------------------------ epoch / ledger
    @property
    def coins_all(self):
        held = sorted(self.P) if hasattr(self, "P") else []
        return self.universe + [c for c in held if c not in self.universe]
    @property
    def active(self): return [c for c in self.universe if c not in self.excluded]
    def tf_now(self): return (self.ep_segs[-1] if self.ep_segs else self.ep)["tf"]
    @property
    def tf(self): return self.tf_now() if getattr(self, "ep", None) else "15m"
    def ensure_epoch(self):
        """Freeze the earlier run (pool cash, no open positions) into epoch0.json and start the live_pool segment."""
        for g in self.segs:
            if g.get("engine") == "live_pool": self.ep = g
        if self.ep:
            self.epoch0 = json.load(open(self.p("epoch0.json"))); self.ep_segs = [g for g in self.segs if g.get("engine") == "live_pool" or g.get("live_child")]
            return
        st = json.load(open(self.p("state.json"))); pool = st.get("pool") or {}
        if pool.get("slots"): raise RuntimeError("cannot start the live pool epoch with open positions in the old engine: " + ",".join(s["coin"] for s in pool["slots"]))
        n = now_ms(); L = json.load(open(self.p("live.json")))
        px = self.prices(self.universe)
        ep0 = {"frozen_ms": n, "frozen_wib": ms_wib(n), "cash": pool["cash"], "balance": pool["cash"], "bh": L.get("bh_balance") or st["bh_close"],
               "coins_old": st["coins_traded"], "trades": jsonl_read(self.p("trades.jsonl")), "equity": jsonl_read(self.p("equity.jsonl")),
               "fills": {c["coin"]: c["fills"] for c in st["coins"]}, "fees": sum(t.get("fee", 0) for t in jsonl_read(self.p("trades.jsonl"))),
               "segments": self.segs, "switches": pool.get("switches", []), "skipped": pool.get("skipped", []), "n_skipped": pool.get("n_skipped", 0),
               "pool_since_ms": pool.get("since_ms"), "watch": st.get("watch", {})}
        write_json(self.p("epoch0.json"), ep0); self.epoch0 = ep0
        g = {"tf": self.segs[-1]["tf"], "from_tf": self.segs[-1]["tf"], "mode": "arrow_long", "from_mode": self.segs[-1].get("mode"), "sizing": "pool", "from_sizing": "pool",
             "engine": "live_pool", "signal_timing": self.timing, "from_timing": "close", "coins": self.universe, "from_coins": st["coins_traded"],
             "exclude_top_mcap": self.excl_n, "max_positions": self.max_pos, "tp_usd": self.tp_usd, "sl_usd": self.sl_usd,
             "from_ms": n, "from_wib": ms_wib(n), "balance": pool["cash"], "bh": ep0["bh"], "px": px, "sync": {}, "skipped": [],
             "why": "switch to intrabar signals + low-cap universe (user 19:32/19:34 WIB)"}
        self.segs.append(g); self.ep = g; self.ep_segs = [g]; self.save_segs()
        self.event("mode_switch", f"MODE arrow_long/close → arrow_long/intrabar · universe {'+'.join(st['coins_traded'])} → {len(self.universe)} low-cap coins ({', '.join(self.universe)}); "
                   f"balance ${pool['cash']:.2f} carried over (all cash, NEAR/XRP closed MANUAL before the switch); old run frozen to epoch0.json", key=f"ep|{n}")
    def fold(self):
        cash = self.epoch0["cash"]; P = {}; trades = []; fills = {}; fees = 0.0
        for f in jsonl_read(self.p("ledger.jsonl")):
            c = f["coin"]; fills.setdefault(c, []).append(f)
            if f["side"] == "BUY":
                fee = f["amt"] * FEE; u = (f["amt"] - fee) / f["px"]; cash -= f["amt"]; fees += fee
                tp, sl = tpsl_levels(u, f["amt"], f.get("tp_usd"), f.get("sl_usd"))
                P[c] = {"units": u, "cost": f["amt"], "entry_t": f["t"], "entry_px": f["px"], "fee": fee, "bar_t": f["bar_t"], "tf": f.get("tf"),
                        "reason": f.get("reason"), "tp_px": tp, "sl_px": sl, "tp_usd": f.get("tp_usd"), "sl_usd": f.get("sl_usd")}
            elif f["side"] == "RESIZE" and c in P:
                p = P[c]; u = min(f["units"], p["units"]); k = u / p["units"]; gross = u * f["px"]; fee = gross * FEE; pr = gross - fee; cash += pr; fees += fee
                cpart = p["cost"] * k; fpart = p["fee"] * k
                p["units"] -= u; p["cost"] -= cpart; p["fee"] -= fpart
                p["tp_px"], p["sl_px"] = tpsl_levels(p["units"], p["cost"], p.get("tp_usd"), p.get("sl_usd"))
                p.setdefault("resized", []).append({"t": f["t"], "units": u, "px": f["px"]})
                trades.append({"coin": c, "side": "long", "entry_t": p["entry_t"], "entry_px": p["entry_px"], "exit_t": f["t"], "exit_px": f["px"], "cost": cpart,
                               "proceeds": pr, "pnl": pr - cpart, "ret": pr / cpart - 1 if cpart else 0.0, "fee": fpart + fee, "entry_fee": fpart, "exit_fee": fee,
                               "bars": round((f["t"] - p["entry_t"]) / pb.BAR, 2), "entry_tf": p["tf"], "exit_tf": f.get("tf"), "entry_mode": "arrow_long", "exit_mode": "arrow_long",
                               "exit_reason": "RESIZE", "entry_reason": p.get("reason"), "sizing": "pool", "units": u, "timing": f.get("timing"), "partial": True})
            elif c in P:
                p = P.pop(c); gross = p["units"] * f["px"]; fee = gross * FEE; pr = gross - fee; cash += pr; fees += fee
                trades.append({"coin": c, "side": "long", "entry_t": p["entry_t"], "entry_px": p["entry_px"], "exit_t": f["t"], "exit_px": f["px"], "cost": p["cost"],
                               "proceeds": pr, "pnl": pr - p["cost"], "ret": pr / p["cost"] - 1, "fee": p["fee"] + fee, "entry_fee": p["fee"], "exit_fee": fee,
                               "bars": round((f["t"] - p["entry_t"]) / pb.BAR, 2), "entry_tf": p["tf"], "exit_tf": f.get("tf"), "entry_mode": "arrow_long", "exit_mode": "arrow_long",
                               "exit_reason": f.get("reason"), "entry_reason": p.get("reason"), "sizing": "pool", "units": p["units"], "timing": f.get("timing"),
                               **({"observed_px": f["observed"]} if f.get("observed") is not None else {})})
        self.cash, self.P, self.trades1, self.fills1, self.fees1 = cash, P, trades, fills, fees
    def bar_of(self, t): B = TFS[self.tf]; return t // B * B
    def did(self, c, side, bo):
        return any(f["side"] == side and f.get("bar_t") == bo for f in self.fills1.get(c, []))
    def fill(self, c, side, px, reason, why, t=None, amt=None, observed=None):
        t = t or now_ms(); bo = self.bar_of(t)
        row = {"t": t, "t_wib": ms_wib(t), "coin": c, "side": side, "px": px, "reason": reason, "why": why, "bar_t": bo, "tf": self.tf, "timing": self.timing}
        if side == "BUY": row.update(amt=amt, tp_usd=self.tp_usd, sl_usd=self.sl_usd)
        if observed is not None: row["observed"] = observed
        append_jsonl(self.p("ledger.jsonl"), row)
        before = dict(self.P.get(c) or {})
        self.fold()
        if side == "BUY":
            p = self.P[c]
            self.event("fill", f"{c}: paper BUY ${amt:.2f} [{self.tf} · {reason}] at {px:.6g} ({why}) {ms_wib(t)} WIB, fee ${amt * FEE:.4f}; TP {p['tp_px']:.6g} / SL {p['sl_px']:.6g}",
                       key=f"f|{c}|{t}|BUY", coin=c, side="BUY", px=px, fill_t=t, reason=reason, tf=self.tf)
        else:
            tr = self.trades1[-1]
            self.event("fill", f"{c}: paper SELL [{self.tf} · {reason}] at {px:.6g} ({why}) {ms_wib(t)} WIB, fee ${tr['exit_fee']:.4f}, P&L ${tr['pnl']:+.2f}",
                       key=f"f|{c}|{t}|SELL", coin=c, side="SELL", px=px, fill_t=t, reason=reason, tf=self.tf, pnl=tr["pnl"])
    def entry_amt(self, prices=None):
        """Entry size: equity / max_positions (equity at the live price), capped at the pool cash."""
        pr = dict(self.live or {}); pr.update(prices or {})
        return max(0.0, min(self.cash, self.equity_now(pr) / self.max_pos))
    def check_slots(self):
        """max_positions in config differs from the active segment -> new live_child segment; held positions above
        equity / max_positions are trimmed (paper partial sell RESIZE at the live price, 0.1% fee)."""
        last = self.ep_segs[-1] if self.ep_segs else self.ep
        old = int(last.get("max_positions") or pb.POOL_DEFAULT_MAX)
        if old == self.max_pos: return
        n = now_ms(); px = self.prices(self.coins_all); self.live.update(px)
        eq0 = self.equity_now(px); cash0 = self.cash; target = eq0 / self.max_pos; fees0 = self.fees1
        g = {"tf": self.tf, "from_tf": self.tf, "mode": "arrow_long", "from_mode": "arrow_long", "sizing": "pool", "from_sizing": "pool", "live_child": True,
             "signal_timing": self.timing, "from_ms": n, "from_wib": ms_wib(n), "px": px, "sync": {}, "skipped": [], "max_positions": self.max_pos, "from_max_positions": old,
             "tp_usd": self.tp_usd, "sl_usd": self.sl_usd, "sizing_rule": "equity/max_positions capped at cash", "balance": eq0, "equity_before": eq0, "cash_before": cash0,
             "why": f"max positions {old} → {self.max_pos} (user 20:32 WIB: expand to {self.max_pos} slots)"}
        self.segs.append(g); self.ep_segs.append(g); self.save_segs()
        for c in sorted(self.P):
            p = self.P[c]; v = p["units"] * px[c]
            if v - target <= 0.01: continue
            u = (v - target) / px[c]; t = now_ms()
            append_jsonl(self.p("ledger.jsonl"), {"t": t, "t_wib": ms_wib(t), "coin": c, "side": "RESIZE", "px": px[c], "units": u, "reason": "RESIZE",
                                                  "why": f"trim to equity/{self.max_pos} = ${target:.2f} (max positions {old} → {self.max_pos})", "bar_t": self.bar_of(t), "tf": self.tf, "timing": self.timing, "observed": px[c]})
            self.fold(); q = self.P[c]; tr = self.trades1[-1]
            g["sync"][c] = {"side": "RESIZE", "px": px[c], "units_sold": u, "proceeds": tr["proceeds"], "fee": tr["exit_fee"], "units_after": q["units"], "cost_after": q["cost"],
                            "value_after": q["units"] * px[c], "tp_px": q["tp_px"], "sl_px": q["sl_px"]}
            self.event("fill", f"{c}: paper RESIZE (partial SELL) {u:.6g} units ${u * px[c]:.2f} [{self.tf} · RESIZE] at {px[c]:.6g} (live price, trim to equity/{self.max_pos} ${target:.2f}) {ms_wib(t)} WIB, "
                               f"fee ${tr['exit_fee']:.4f}, realised P&L ${tr['pnl']:+.2f}; now {q['units']:.6g} units, cost ${q['cost']:.2f}, entry {q['entry_px']:.6g}, TP {q['tp_px']:.6g} / SL {q['sl_px']:.6g}",
                       key=f"f|{c}|{t}|RESIZE", coin=c, side="RESIZE", px=px[c], fill_t=t, reason="RESIZE", tf=self.tf, pnl=tr["pnl"])
        eq1 = self.equity_now(px)
        g.update(equity_after=eq1, fees=self.fees1 - fees0, cash_after=self.cash, held_after=sorted(self.P)); self.save_segs()
        self.event("mode_switch", f"MAX POSITIONS {old} → {self.max_pos} at {ms_wib(n)} WIB: entry size = equity/{self.max_pos} (≈${target:.2f}) capped at cash; "
                   + (", ".join(f"{c} trimmed {v['units_sold']:.6g} @ {v['px']:.6g} → ${v['value_after']:.2f}" for c, v in g["sync"].items()) or "no trims")
                   + f"; balance ${eq0:.2f} carried over → ${eq1:.2f} (trim fees ${self.fees1 - fees0:.4f}); cash ${cash0:.2f} → ${self.cash:.2f}, {self.max_pos - len(self.P)} free slots",
                   key=f"mp|{n}", from_max=old, to_max=self.max_pos, equity_before=eq0, equity_after=eq1)
        self.mark(n, px)
    def save_eval(self):
        cut = now_ms() - 3 * 86_400_000
        self.flags = {k: v for k, v in self.flags.items() if int(k.split("|")[1]) > cut}
        write_json(self.p("run/intrabar_eval.json"), {"last_eval_ms": self.last_eval, "last_ct": self.last_ct, "flags": self.flags, "tf": self.tf})

    # ------------------------------------------------------------ data
    def prices(self, coins):
        q = api("/api/v3/ticker/price", {"symbols": json.dumps([c + "USDT" for c in coins], separators=(",", ":"))})
        return {x["symbol"][:-4]: float(x["price"]) for x in q}
    def refresh(self, coins, n):
        B = TFS[self.tf]
        for c in coins:
            try:
                rows = self.store.get(c + "USDT", self.tf, pb.seg_frm(self.tf, n))
                df = closed_df(rows, n)
                if len(df) < WARMUP + 60: continue
                self.D[c] = {"df": df, "ind": core.indicator(df), "opens": {r[0]: r[1] for r in rows}, "rows": rows}
                fr = [r for r in rows if r[0] <= n < r[0] + B]
                if fr: self.forming[c] = {"bo": fr[0][0], "o": fr[0][1], "h": fr[0][2], "l": fr[0][3]}
            except Exception as e:
                self.event("error", f"klines {c} {self.tf}: {e}", coin=c)
            self.beat()
    def refresh_forming(self, c):
        B = TFS[self.tf]; n = now_ms()
        try:
            r = klines(c + "USDT", self.tf, None, 1)[-1]
            if r[0] <= n < r[0] + B:
                f = self.forming.get(c)
                if f and f["bo"] == r[0]: f["h"] = max(f["h"], r[2]); f["l"] = min(f["l"], r[3])
                else: self.forming[c] = {"bo": r[0], "o": r[1], "h": r[2], "l": r[3]}
        except Exception as e: log.warning(f"forming {c}: {e}")
    def check_universe(self, force=False):
        if not self.excl_n: return
        try: u = json.load(open(self.p("run/universe.json")))
        except Exception: u = {}
        if not force and u.get("checked_ms", 0) > now_ms() - 86_400_000 and u.get("n") == self.excl_n:
            self.excluded = u.get("excluded", {}); return
        try:
            top = top_mcap_symbols(self.excl_n)
            ex = {c: top[c] for c in self.universe if c in top}
            if set(ex) != set(self.excluded): self.event("universe", f"top-{self.excl_n} mcap check (CoinGecko): " + (", ".join(f"{c} #{v['rank']}" for c, v in ex.items()) + " dropped from new entries" if ex else f"none of the {len(self.universe)} coins is in the top {self.excl_n}"), key=f"u|{now_ms()//3600000}")
            self.excluded = ex
            write_json(self.p("run/universe.json"), {"checked_ms": now_ms(), "checked_wib": ms_wib(now_ms()), "n": self.excl_n, "excluded": ex, "top": sorted(top), "source": "CoinGecko /coins/markets"})
        except Exception as e:
            self.excluded = u.get("excluded", {}); log.warning(f"universe check: {e}")

    # ------------------------------------------------------------ rules
    def process_closed(self, n):
        """Handle every closed bar of the active timeframe since last_ct (normally just one): catch-up TP/SL, ARROW(_CLOSE) exits
        and entries at the next bar open. Then mark equity."""
        B = TFS[self.tf]; cur_bo = n // B * B
        if self.last_ct is None or self.last_ct < self.ep["from_ms"] // B * B + B: self.last_ct = max(self.last_ct or 0, self.ep["from_ms"] // B * B + B)
        bars = list(range(self.last_ct - B + B, cur_bo + 1, B)) if self.last_ct else []
        bars = [ct for ct in range(self.last_ct, cur_bo + 1, B)]
        for ct in bars:
            bo = ct - B
            idx = {c: self.D[c]["df"].index[self.D[c]["df"].t.values == bo] for c in self.D}
            idx = {c: int(v[0]) for c, v in idx.items() if len(v)}
            # 1) catch-up TP/SL on bar high/low for bars not covered live
            if bo < self.cover_from:
                for c in sorted(self.P):
                    p = self.P[c]; i = idx.get(c)
                    if i is None or p["entry_t"] > bo: continue
                    df = self.D[c]["df"]; o, h, l = (float(df[x].values[i]) for x in "ohl")
                    if p["sl_px"] and l <= p["sl_px"]: self.fill(c, "SELL", o if o <= p["sl_px"] else p["sl_px"], "SL", "catch-up: bar low", t=max(bo, p["entry_t"] + 1))
                    elif p["tp_px"] and h >= p["tp_px"]: self.fill(c, "SELL", o if o >= p["tp_px"] else p["tp_px"], "TP", "catch-up: bar high", t=max(bo, p["entry_t"] + 1))
            # 2) exits on a down arrow on the closed bar (not already acted on intrabar)
            rs = "ARROW" if self.timing == "close" else "ARROW_CLOSE"
            for c in sorted(self.P):
                i = idx.get(c)
                if i is None or self.D[c]["ind"]["arrow"][i] >= 0: continue
                if self.timing == "intrabar" and (self.flags.get(f"{c}|{bo}") == "down" or self.did(c, "SELL", bo)): continue
                px = self.D[c]["opens"].get(ct)
                if px is None: continue
                self.fill(c, "SELL", px, rs, f"▼ down arrow on the closed {self.tf} bar {ms_wib(bo)[11:16]}, next bar open", t=ct)
            # 3) entries on an up arrow on the closed bar
            cand = []
            for c in self.active:
                i = idx.get(c)
                if c in self.P or i is None or self.D[c]["ind"]["arrow"][i] <= 0: continue
                if self.timing == "intrabar" and (self.flags.get(f"{c}|{bo}") or self.did(c, "BUY", bo) or self.did(c, "SELL", bo)): continue
                cand.append((-pb.strength_at(self.D[c]["df"], self.D[c]["ind"], i), c))
            for ng, c in sorted(cand):
                free = self.max_pos - len(self.P)
                if free <= 0: self.skip(c, bo, -ng, "slots full"); continue
                if pausectl.current(): self.skip(c, bo, -ng, "paused"); continue
                if self.cash < 1.0: self.skip(c, bo, -ng, "no cash"); continue
                px = self.D[c]["opens"].get(ct)
                if px is None: continue
                self.fill(c, "BUY", px, rs, f"▲ up arrow on the closed {self.tf} bar {ms_wib(bo)[11:16]}, next bar open", t=ct, amt=self.entry_amt({c: px}))
            # 4) signal events + equity mark at the close
            for c, i in idx.items():
                a = self.D[c]["ind"]["arrow"][i]
                if a: self.event("signal", f"{c}: {'▲ up' if a > 0 else '▼ down'} arrow on the closed {self.tf} bar {ms_wib(bo)}–{ms_wib(ct)[11:16]} WIB (close {self.D[c]['df'].c.values[i]:.6g}, exit line {self.D[c]['ind']['ex_line'][i]:.6g})", key=f"a|{self.tf}|{c}|{bo}", coin=c, side="up" if a > 0 else "down", bar_t=bo, tf=self.tf, arrow=True)
            cl = {c: float(self.D[c]["df"].c.values[i]) for c, i in idx.items()}
            self.mark(ct, cl)
            self.last_ct = ct + B
        self.last_ct = cur_bo + B if self.last_ct is None or self.last_ct <= cur_bo else self.last_ct
        self.save_eval()
    def skip(self, c, bo, s, why):
        sk = {"coin": c, "sig_t": bo, "fill_t": bo + TFS[self.tf], "side": "long", "tf": self.tf, "strength": s, "reason": why, "held": sorted(self.P), "t": now_ms()}
        k = f"k|{c}|{bo}"
        if k not in self.seen: append_jsonl(self.p("skips.jsonl"), sk)
        self.event("skip", f"{c}: ▲ up arrow ({self.tf} bar {ms_wib(bo)[11:16]} WIB) skipped: {why}" + (f" ({'+'.join(sorted(self.P))} held, strength {s:.2f} ATR)" if why == "slots full" else "") + "; not queued", key=k, coin=c, reason=why)
    def bh_value(self, prices):
        px0 = self.ep.get("px") or {}; ks = [c for c in self.universe if px0.get(c) and prices.get(c)]
        return self.ep["bh"] * (sum(prices[c] / px0[c] for c in ks) / len(ks)) if ks else self.ep["bh"]
    def equity_now(self, prices):
        return self.cash + sum(p["units"] * (prices.get(c) or p["entry_px"]) for c, p in self.P.items())
    def mark(self, t, prices):
        pr = dict(self.live); pr.update(prices)
        append_jsonl(self.p("equity1.jsonl"), {"t": int(t), "eq": round(self.equity_now(pr), 6), "bh": round(self.bh_value(pr), 6)})
    def tick(self):
        """5 s: one price call for all symbols, live TP/SL, intrabar arrows."""
        coins = self.coins_all
        try: self.live = self.prices(coins); self.live_t = now_ms()
        except Exception as e: log.warning(f"live price: {e}"); return
        n = self.live_t; B = TFS[self.tf]; bo = n // B * B; self.last_eval = n
        if coins:
            self.refresh_forming(coins[self.rr % len(coins)]); self.rr += 1
        for c, px in self.live.items():
            f = self.forming.get(c)
            if f and f["bo"] == bo: f["h"] = max(f["h"], px); f["l"] = min(f["l"], px)
        # live TP / SL (at the level)
        for c in sorted(self.P):
            p = self.P[c]; px = self.live.get(c)
            if px is None: continue
            k = "SL" if p["sl_px"] and px <= p["sl_px"] else ("TP" if p["tp_px"] and px >= p["tp_px"] else None)
            if k: self.fill(c, "SELL", p["sl_px"] if k == "SL" else p["tp_px"], k, f"live 5 s check, price {px:.6g} at/through the level", t=n, observed=px)
        if self.timing != "intrabar": return
        ups = []; nd = 0
        for c in coins:
            d = self.D.get(c); f = self.forming.get(c); px = self.live.get(c)
            if not d or not f or px is None or f["bo"] != bo or int(d["df"].t.values[-1]) != bo - B: continue
            a, ex, s = live_exit(d["df"], f["o"], max(f["h"], px), min(f["l"], px), px); nd += 1
            self.liveind[c] = {"arrow": a, "exit_line": ex, "strength": s, "price": px, "bar_t": bo, "t": n}
            key = f"{c}|{bo}"
            if a < 0 and c in self.P and not self.did(c, "SELL", bo):
                self.flags[key] = "down"
                self.fill(c, "SELL", px, "ARROW_INTRABAR", f"▼ down arrow intrabar: live {px:.6g} < exit line {ex:.6g}", t=n, observed=px)
            elif a > 0 and c not in self.P and c in self.active and not self.flags.get(key):
                if self.did(c, "BUY", bo) or self.did(c, "SELL", bo): self.flags[key] = "up"; continue
                ups.append((-s, c, px, ex))
        for ng, c, px, ex in sorted(ups):
            self.flags[f"{c}|{bo}"] = "up"; free = self.max_pos - len(self.P)
            if free <= 0: self.skip(c, bo, -ng, "slots full"); continue
            if pausectl.current(): self.skip(c, bo, -ng, "paused"); continue
            if self.cash < 1.0: self.skip(c, bo, -ng, "no cash"); continue
            self.fill(c, "BUY", px, "ARROW_INTRABAR", f"▲ up arrow intrabar: live {px:.6g} > exit line {ex:.6g}, strength {-ng:.2f} ATR", t=n, amt=self.entry_amt(), observed=px)
        if ups: self.save_eval()
        if n - getattr(self, "_eval_log", 0) >= 60_000:
            self._eval_log = n; log.info(f"intrabar eval {self.tf} bar {ms_wib(bo)[11:16]}: {nd}/{len(coins)} coins, up {sum(1 for v in self.liveind.values() if v['arrow'] > 0 and v['bar_t'] == bo)}, held {'+'.join(sorted(self.P)) or 'none'}, cash ${self.cash:.2f}")
            self.save_eval()

    # ------------------------------------------------------------ tf switch (dashboard 15m/1h/4h)
    def do_switch(self, new, new_mode=None):
        old = self.tf; n = now_ms(); self.check_pause()
        px = self.prices(self.coins_all); self.live.update(px)
        g = {"tf": new, "from_tf": old, "mode": "arrow_long", "from_mode": "arrow_long", "sizing": "pool", "live_child": True, "signal_timing": self.timing,
             "from_ms": n, "from_wib": ms_wib(n), "px": px, "sync": {}, "skipped": [], "max_positions": self.max_pos, "tp_usd": self.tp_usd, "sl_usd": self.sl_usd}
        self.segs.append(g); self.ep_segs.append(g); self.save_segs()
        self.D = {}; self.forming = {}; self.refresh(self.coins_all, n)
        why = f"switch {old}→{new}"
        for c in sorted(self.P):
            d = self.D.get(c)
            if d and d["ind"]["arrow_state"][-1] <= 0: g["sync"][c] = {"side": "SELL", "px": px[c]}; self.fill(c, "SELL", px[c], "SWITCH", why + ": latest arrow down", t=n)
        cand = sorted((-pb.strength_at(self.D[c]["df"], self.D[c]["ind"], len(self.D[c]["df"]) - 1), c) for c in self.active if c in self.D and c not in self.P and self.D[c]["ind"]["arrow_state"][-1] > 0)
        for ng, c in cand:
            free = self.max_pos - len(self.P)
            if free <= 0: g["skipped"].append(c + " (slots full)"); continue
            if self.paused: g["skipped"].append(c + " (paused)"); continue
            g["sync"][c] = {"side": "BUY", "px": px[c]}; self.fill(c, "BUY", px[c], "SWITCH", why + ": latest arrow up (strongest first)", t=n + 1, amt=self.entry_amt())
        self.save_segs(); B = TFS[new]; self.last_ct = n // B * B + B; self.cover_from = n
        self.event("switch", f"TIMEFRAME {old} → {new} at {ms_wib(n)} WIB: " + (", ".join(f"{c} {v['side']} @ {v['px']:.6g}" for c, v in g["sync"].items()) or "no sync fills") + (f"; skipped {', '.join(g['skipped'])}" if g["skipped"] else ""), key=f"tf|{n}", tf=new, from_tf=old)
        self.mark(n, px); self.write_all(now_ms())

    # ------------------------------------------------------------ outputs
    def beat(self):
        if threading.current_thread() is threading.main_thread() and now_ms() - self._beat_t >= 20_000: self.write_live()
    def write_all(self, now):
        ep0 = self.epoch0; coins = []; B = TFS[self.tf]
        trades = sorted(ep0["trades"] + self.trades1, key=lambda t: t["exit_t"])
        eqs = [{"t": e["t"], "eq": e["eq"], "bh": e["bh"]} for e in ep0["equity"]] + jsonl_read(self.p("equity1.jsonl"))
        pts = [(e["t"], e["eq"], e["bh"]) for e in eqs]
        for c in self.coins_all:
            d = self.D.get(c) or {}; df, ind = d.get("df"), d.get("ind"); p = self.P.get(c)
            lp = self.live.get(c) or (float(df.c.values[-1]) if df is not None else None)
            tr = [t for t in self.trades1 if t["coin"] == c]; pn = {t["exit_t"]: t["pnl"] for t in tr}
            fl = [dict(f, fee=(f["amt"] if f["side"] == "BUY" else (f["units"] * f["px"] if f["side"] == "RESIZE" else 0)) * FEE, **({"pnl": pn.get(f["t"])} if f["side"] in ("SELL", "RESIZE") else {"size": f.get("amt")}), **({"size": f["units"] * f["px"]} if f["side"] == "RESIZE" else {})) for f in self.fills1.get(c, [])]
            pos = None
            if p:
                vc = p["units"] * (lp or p["entry_px"])
                pos = {"side": "long", "entry_t": p["entry_t"], "entry_px": p["entry_px"], "units": p["units"], "collateral": 0.0, "cost": p["cost"], "entry_fee": p["fee"],
                       "bars": round((now - p["entry_t"]) / B, 2), "close_px": lp, "value_close": vc, "tf": p["tf"], "tp_px": p["tp_px"], "sl_px": p["sl_px"],
                       "tp_usd": p.get("tp_usd"), "sl_usd": p.get("sl_usd"), "entry_reason": p.get("reason")}
            ro = readout(df, ind, c) if ind is not None else {}
            li = self.liveind.get(c)
            if li and li["bar_t"] == now // B * B: ro["live"] = li
            alloc = self.ep["bh"] / len(self.universe) if c in self.universe else 0.0
            coins.append({"coin": c, "alloc": alloc, "started": True, "value_close": pos["value_close"] if pos else 0.0, "cash": 0.0, "fees": sum(t["fee"] for t in tr) + (p["fee"] if p else 0),
                          "pos": pos, "pending": None, "n_trades": len(tr), "pnl_realised": sum(t["pnl"] for t in tr),
                          "bh_value_close": alloc * (lp / self.ep["px"][c]) if lp and (self.ep.get("px") or {}).get(c) else alloc, "bh_c0": (self.ep.get("px") or {}).get(c),
                          "exposure_pct": 0.0, "ind": ro, "fills": fl, "sizing": "pool", "in_universe": c in self.universe, "excluded": self.excluded.get(c),
                          "tradable": c in self.active})
            if ind is not None: write_json(self.p(f"data/chart_{c}.json"), chart_payload(df, ind, {"fills": fl}))
        res = [{"trades": [], "fees": 0.0, "started": False, "alloc": 0, "bars_total": 0, "bars_in_pos": 0}]
        pf = portfolio(res, self.cap, self.start_ms, pts or [(now, self.cash, self.ep["bh"])])
        pf.update({k: v for k, v in portfolio([{"trades": trades, "fees": 0, "started": False, "alloc": 0, "bars_total": 0, "bars_in_pos": 0}], self.cap, self.start_ms, pts).items() if k not in ("equity", "balance_close", "bh_close", "max_dd_pct", "max_dd_usd")})
        pf["fees"] = ep0["fees"] + self.fees1
        bal = self.equity_now({c: x["pos"]["close_px"] for c, x in zip(self.coins_all, coins) if x["pos"]})
        free = self.max_pos - len(self.P)
        skips = [s for s in jsonl_read(self.p("skips.jsonl"))]
        slots = [{"coin": c, "units": p["units"], "cost": p["cost"], "entry_t": p["entry_t"], "entry_px": p["entry_px"], "value_close": p["units"] * (self.live.get(c) or p["entry_px"]),
                  "tp_px": p["tp_px"], "sl_px": p["sl_px"], "reason": p.get("reason")} for c, p in sorted(self.P.items(), key=lambda x: x[1]["entry_t"])]
        pool = {"sizing": "pool", "max_positions": self.max_pos, "cash": self.cash, "slots_used": len(self.P), "free_slots": free, "next_trade_size": self.entry_amt() if free > 0 else 0.0, "sizing_rule": "equity / max_positions, capped at cash",
                "slots": slots, "tp_usd": self.tp_usd, "sl_usd": self.sl_usd, "since_ms": ep0.get("pool_since_ms"), "since_wib": ms_wib(ep0["pool_since_ms"]) if ep0.get("pool_since_ms") else None,
                "switches": ep0.get("switches", []) + [{"at_ms": self.ep["from_ms"], "at_wib": self.ep["from_wib"], "why": "switch to intrabar + low-cap universe", "equity_before": self.ep["balance"], "equity_after": self.ep["balance"], "fees": 0.0, "cash_after": self.ep["balance"], "held_after": []}]
                            + [{"at_ms": g["from_ms"], "at_wib": g["from_wib"], "why": g.get("why") or f"max positions {g['from_max_positions']} → {g['max_positions']}", "equity_before": g.get("equity_before"), "equity_after": g.get("equity_after"),
                                "fees": g.get("fees", 0.0), "cash_after": g.get("cash_after"), "held_after": g.get("held_after", []), "trims": g.get("sync")} for g in self.ep_segs if g.get("from_max_positions")],
                "skipped": (ep0.get("skipped", []) + skips)[-200:], "n_skipped": ep0.get("n_skipped", 0) + len(skips)}
        last_bar = max((int(d["df"].t.values[-1]) for d in self.D.values()), default=now // B * B - B)
        tpsl = f" · TP ${self.tp_usd:g} / SL ${self.sl_usd:g}" if self.tp_usd or self.sl_usd else ""
        ms = [{"at_ms": g["from_ms"], "at_wib": g.get("from_wib"), "from": (g.get("from_mode") or "") + ("/" + g.get("from_sizing", "coin") if g.get("sizing") else ""), "to": g.get("mode") + ("/pool max %s" % g.get("max_positions") if g.get("sizing") == "pool" else "") + (" · " + g["signal_timing"] if g.get("engine") == "live_pool" else "") + (" (was max %s)" % g["from_max_positions"] if g.get("from_max_positions") else ""), "tf": g["tf"], "fills": g.get("sync")}
              for g in self.segs[1:] if g.get("from_mode") and (g.get("from_mode") != g.get("mode") or (g.get("sizing") or "coin") != (g.get("from_sizing") or "coin") or g.get("engine") == "live_pool" or (g.get("from_max_positions") and g.get("from_max_positions") != g.get("max_positions")))]
        self.state = {"mode": "PAPER SIMULATION - no real orders, no API keys", "trade_mode": "arrow_long", "trade_mode_text": MODES["arrow_long"] if self.timing == "close" else "Long-only · Exit Arrows evaluated intrabar on the live price (BUY the moment an up arrow appears, SELL on a down arrow)",
                      "signal_timing": self.timing, "signal_timing_text": "Signals: " + TIMING_TEXT[self.timing], "engine": "live_pool",
                      "universe": self.universe, "universe_active": self.active, "excluded": self.excluded, "exclude_top_mcap": self.excl_n,
                      "universe_info": (self.cfg.get("_universe") or {}), "legacy_coins": ep0["coins_old"],
                      "active_tf": self.tf, "segments": self.segs, "mode_switches": ms, "sizing": "pool", "max_positions": self.max_pos, "pool": pool,
                      "sizing_text": f"Max positions {self.max_pos} · pool sizing{tpsl} · Signals: {TIMING_TEXT[self.timing]}",
                      "rules": {"tf": self.tf, "sizing": f"shared pool, max {self.max_pos} open longs; size = equity / {self.max_pos} (≈${self.entry_amt() if self.cash > 0 else self.equity_now(self.live) / self.max_pos:.0f}) capped at cash (strongest (close-exit)/ATR first, extra arrows skipped, not queued)",
                                "tp_usd": self.tp_usd, "sl_usd": self.sl_usd, "tpsl": "net of the entry fee and the 0.1% exit fee; live 5 s price checks fill at the trigger level; catch-up on bar high/low, SL first",
                                "direction": "long-only", "signal_timing": self.timing,
                                "entry": "Exit Arrow up, evaluated intrabar every ~5 s on the forming candle (live price as close): BUY at the live price at once" if self.timing == "intrabar" else "Exit Arrow up (close crosses above the Exit SSL HMA15) on the closed bar",
                                "exit": "Exit Arrow down intrabar (live price crosses below the Exit SSL): SELL at once; or TP/SL" if self.timing == "intrabar" else "Exit Arrow down on the closed bar; or TP/SL",
                                "fee_per_side": FEE, "compounding": True,
                                "fill": "intrabar: live Binance price at the 5 s poll (max 1 entry + 1 exit per coin per candle, no re-entry in the exit candle); arrows only visible on the closed bar: next bar open" if self.timing == "intrabar" else "next bar open (Binance kline open)"},
                      "start_wib": self.start_wib, "start_ms": self.start_ms, "first_fill_ms": -(-self.start_ms // pb.BAR) * pb.BAR, "capital": self.cap, "coins_traded": self.coins_all,
                      "epoch_from_ms": self.ep["from_ms"], "epoch_from_wib": self.ep["from_wib"], "epoch_balance": self.ep["balance"],
                      "last_bar_t": last_bar, "last_bar_wib": ms_wib(last_bar), "last_bar_close_wib": ms_wib(last_bar + B), "processed_at": now_ms(), "processed_at_wib": ms_wib(now_ms()),
                      "boot_ms": self.boot, "bars_processed_since_boot": self.bars_done, "source": pb.SRC["name"],
                      "balance_close": bal, "bh_close": pts[-1][2] if pts else self.ep["bh"], "pnl_close": bal - self.cap,
                      "stats": {k: pf.get(k) for k in ("max_dd_pct", "max_dd_usd", "n_trades", "wins", "losses", "win_rate", "profit_factor", "avg_win", "avg_loss", "best", "worst", "fees", "exposure_pct", "realised_pnl")},
                      "paused": self.paused, "pauses": pausectl.history(), "skipped_entries": skips[-50:],
                      "coins": coins, "watch": self.watch, "_seen": sorted(self.seen), "_first_trade_px": self.fill_px}
        write_json(self.p("state.json"), self.state)
        write_jsonl(self.p("trades.jsonl"), [dict(t, entry_wib=ms_wib(t["entry_t"]), exit_wib=ms_wib(t["exit_t"]), exit_reason=t.get("exit_reason") or "ARROW") for t in trades])
        write_jsonl(self.p("equity.jsonl"), [dict(e, wib=ms_wib(e["t"])) for e in eqs])
        self.write_live()
    def write_live(self):
        if not getattr(self, "state", None): return
        super().write_live()
        try:
            s = json.load(open(self.p("summary.json")))
            s.update(signal_timing=self.timing, signal_timing_text="Signals: " + TIMING_TEXT[self.timing], universe=self.universe, excluded=self.excluded,
                     coins_traded=self.coins_all, trade_mode_text=self.state["trade_mode_text"])
            write_json(self.p("summary.json"), s)
            L = json.load(open(self.p("live.json"))); L.update(signal_timing=self.timing, last_eval_ms=self.last_eval, last_eval_wib=ms_wib(self.last_eval) if self.last_eval else None)
            write_json(self.p("live.json"), L)
        except Exception as e: log.warning(f"summary extras: {e}")
    def watch_async(self, n):
        if self.watch_busy: return
        self.watch_busy = True
        def run():
            try: self.update_watch(n)
            except Exception as e: self.event("error", f"watchlist: {e}")
            finally: self.watch_busy = False
        threading.Thread(target=run, daemon=True).start()

    def loop(self):
        import signal as sg
        open(self.p("live.json"), "a").close(); os.utime(self.p("live.json"))
        self.liveind = {}; self.live = {}
        self.check_pause()
        self.ensure_epoch(); self.fold(); self.check_slots(); self.check_universe(force=True)
        self.event("start", f"paper bot started (pid {os.getpid()}), live pool engine · arrow_long · Signals: {TIMING_TEXT[self.timing]} · {self.tf} · max {self.max_pos} positions, TP ${self.tp_usd:g} / SL ${self.sl_usd:g} · "
                   f"universe {len(self.universe)} coins ({', '.join(self.universe)})" + (f" · excluded (top {self.excl_n} mcap): {', '.join(self.excluded)}" if self.excluded else f" · none in the top {self.excl_n} mcap") +
                   f" · held {', '.join(sorted(self.P)) or 'none'} · cash ${self.cash:.2f}")
        stop = {"v": False}
        def term(*_): stop["v"] = True
        sg.signal(sg.SIGTERM, term); sg.signal(sg.SIGINT, term)
        n = now_ms(); self.refresh(self.coins_all, n)
        # catch-up from where the previous process left off (bar-based), but only bars of the live epoch
        if self.last_eval: self.cover_from = self.last_eval
        self.process_closed(n); self.cover_from = now_ms()
        self.tick(); self.write_all(now_ms()); self.watch_async(n)
        last_tick = now_ms(); last_hb = 0; last_write = now_ms(); last_uni = now_ms()
        while not stop["v"]:
            n = now_ms(); B = TFS[self.tf]
            try:
                if self.check_pause(): self.write_live()
                self.check_tf()
                if n >= self.last_ct + 3000:           # a candle closed: klines for all coins, bar-close rules, full write
                    self.refresh(self.coins_all, n); self.process_closed(n); self.bars_done += 1
                    self.tick(); self.write_all(now_ms()); last_write = now_ms(); last_tick = now_ms()
                    lb = max((int(d["df"].t.values[-1]) for d in self.D.values()), default=0)
                    self.event("bar", f"processed {self.tf} bar {ms_wib(lb)} WIB · balance ${self.equity_now(self.live):.2f} · signals {self.timing}", key=f"b|{self.tf}|{lb}", bar_t=lb, tf=self.tf)
                    self.watch_async(n)
                elif n - last_tick >= 5000:
                    last_tick = n
                    nf = sum(len(v) for v in self.fills1.values()); self.tick()
                    if sum(len(v) for v in self.fills1.values()) != nf or n - last_write >= 60_000: self.write_all(now_ms()); last_write = now_ms()
                    else: self.write_live()
                if n - last_uni >= 86_400_000: last_uni = n; self.check_universe(force=True)
            except Exception as e:
                self.event("error", f"loop: {e}"); log.error(traceback.format_exc()); time.sleep(5)
            if n - last_hb >= 30_000:
                last_hb = n
                if (n // 60_000) % 15 == 0 and getattr(self, "_hb_min", None) != n // 60_000:
                    self._hb_min = n // 60_000; self.event("heartbeat", f"alive, uptime {(n - self.boot) / 3.6e6:.2f} h, balance ${self.marked()[0]:.2f}")
            time.sleep(0.5)
        self.save_eval(); self.event("stop", "paper bot stopped (SIGTERM)")
