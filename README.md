SSL Hybrid (Mihkel00) - Exit Arrows long-only paper bot
=======================================================
PAPER TRADING ONLY. No exchange keys, no real orders.

1) dashboard/index.html
   Open it in any browser (PC, phone, iPad). It downloads live Binance prices,
   computes the SSL Hybrid indicator + arrows, shows Signals, Coin charts and a
   paper simulation. Runs automatically, no install needed.
   Online version: https://dedkovic441-hub.github.io/ssl-hybrid-dashboard/

2) tradingview/ssl_hybrid_tv_v6.pine
   The Mihkel00 SSL Hybrid indicator (Pine v6) that the bot's maths matches.
   TradingView > Pine Editor > paste > Add to chart. (Or just search
   "SSL Hybrid" by Mihkel00 in Indicators.) The BUY/SELL triggers are the
   "Exit Arrows" (close crossing the HMA 15 exit line).

3) bot/  (the 24/7 paper bot, Python 3.10+)
   pip install numpy pandas requests
   Rules: $500 split $100 per coin (ZEC, NEAR, PEPE, SOL, XRP), 15m bars,
   buy next open after an up arrow, sell to cash after a down arrow,
   0.1% fee per side, each coin compounds separately.
   Settings: HMA60 baseline, SSL2 JMA5, Exit HMA15, ATR14 WMA.
   Linux/Mac/WSL: bash start.sh  (stop: bash stop.sh) -> http://127.0.0.1:8811/
   The bot now stores market data in `ssl-hybrid/data` automatically (relative to the repository); no manual DATA/OUT path edits are needed. In WSL, create/activate `bot/.venv`, install `numpy pandas requests`, run `python bot/fetch_data.py` once, then `cd bot && bash start.sh`. The supervisor uses `bot/.venv/bin/python` when available. Native Windows needs WSL.

4) Pool sizing + TP/SL (since 2026-10-27 17:50 WIB)
   bot/config.json:
     "sizing": "pool"      one shared cash pool (the balance carried over);
                           "coin" = old mode, each coin its own $100 slice
     "max_positions": 5    at most 5 open longs (2 until 2026-10-27 20:35 WIB); trade
                           size = equity / max_positions, capped at the pool cash
                           (compounding; until 20:35 WIB: pool cash / free slots). More up arrows than free slots on one bar:
                           the larger (close - exit line)/ATR wins, the rest are
                           logged "skipped: slots full" (not queued).
     "tp_usd": 5, "sl_usd": 5   per position (SL was $2 until 18:12 WIB), net of the entry fee and the 0.1% exit
                           fee. Checked live every 5 s from Binance prices; a hit sells
                           at the TP/SL level price (the observed price is logged) and
                           is recorded in tpsl_exits.jsonl. After a restart the bar
                           high/low is used (SL first). After a TP/SL exit the coin
                           needs a NEW up arrow to re-enter. 0/null = off.
   A down arrow still sells to the pool. Switching to pool sizing keeps the
   strongest max_positions longs, sells the rest and resizes the kept ones to
   equity/max_positions (paper fills at live price, fees charged).
   Pool -> coin sizing on a running bot is not supported (logged, ignored).
   Config is read at start: restart (stop.sh, start.sh) after editing.
   Backtest: python3 bot/bt_pool.py (per-coin $100 vs pool max-2 vs pool + TP/SL).

5) Intrabar signals + low-cap universe (since 2026-10-27 19:45 WIB)
   bot/config.json:
     "signal_timing": "intrabar"  every ~5 s the Exit SSL (HMA15 of highs/lows) is
                           recomputed on the FORMING candle with the live Binance price
                           as its close and the live high/low. Up arrow -> BUY at once at
                           the live price (if a slot is free); down arrow on a held coin
                           -> SELL at once. Max one entry and one exit per coin per
                           candle, no re-entry in the candle the coin was exited.
                           Fill reason ARROW_INTRABAR. An arrow that disappears before
                           the candle closes does not undo the fill (the position then
                           waits for a down arrow, TP or SL). An arrow only visible on
                           the closed bar (e.g. while the bot was down) fills at the
                           next open, reason ARROW_CLOSE. "close" = old behaviour
                           (decide on bar close, fill at the next open).
     "coins": [...]        the traded universe (22 low-cap Binance USDT coins), picked
                           19:40 WIB: CoinGecko rank > 50 and mcap < $1B, Binance 24h
                           volume >= ~$3.4M, >= 60 days history, no stable / leveraged /
                           wrapped / stock tokens; top 22 by volume. "_universe" holds
                           rank, mcap and volume at selection.
     "exclude_top_mcap": 50  coins in the CoinGecko top 50 by market cap
                           (/coins/markets) are never bought. Re-checked at start and
                           every 24 h (run/universe.json); a held coin that enters the
                           top 50 can still exit, it just gets no new entries.
   Engine: bot/live_engine.py (selected by paper_bot.py when signal_timing is set with
   arrow_long + pool sizing + coins). Event-sourced: the earlier run is frozen in
   epoch0.json; every paper fill since is appended to ledger.jsonl and the state is
   epoch0 + the ledger, so restarts replay the same fills; missed closed bars are
   caught up from klines (TP/SL on bar high/low, SL first).
   Rate limits: one /api/v3/ticker/price call for all coins every 5 s; klines for the
   forming candle are refreshed round-robin (one coin per tick) and fully at each bar
   boundary.
   NEAR and XRP were closed MANUAL at the live price at 19:35 WIB before the switch
   (XRP +$1.48, NEAR +$0.05 net); the balance ($494.66, all cash) carried over.
   The 15m/1h/4h timeframe selector still works (a switch sells held coins whose
   state is down and buys the strongest up-state coins, reason SWITCH).
   No backtest of the intrabar rules was run before going live (user asked to go live
   at once).

6) 5 slots, entry size = equity / max_positions (since 2026-10-27 20:35 WIB)
   "max_positions" went from 2 to 5. Entry size is now equity / max_positions (equity =
   pool cash + held coins at the live price), capped at the available cash, so each
   slot is about $100 on a ~$494 balance (it used to be cash / free slots).
   When max_positions changes, live_engine.py at start adds a segment (from_max_positions
   -> max_positions, equity before/after) and trims any held position worth more than
   equity / max_positions with a paper partial sell: ledger side RESIZE, live price,
   0.1% fee. The entry price stays the same; the cost basis and entry fee shrink pro
   rata, so the net +$tp_usd / -$sl_usd TP/SL still holds on the smaller position (the
   TP/SL prices widen). Each trim is logged as a partial trade (exit_reason RESIZE).
   At the switch (20:35:05 WIB) OSMO had already been closed by SL at 20:32:58 WIB (-$5.00).
   CAKE was trimmed 73.1076 units @ 2.056 ($150.31, fee $0.1503). It now holds 48.0468
   units, cost $99.08, entry 2.06, TP 2.1683 / SL 1.95996. Equity went $493.92 -> $493.77
   (only the trim fee) and cash $244.83 -> $394.99, with 4 slots free.

5) Local SSL Hybrid dashboard (GitHub Pages + PC bot)
   Open https://dedkovic441-hub.github.io/ssl-hybrid-dashboard/local.html
   This page is copied from the existing dashboard and adds a Local PC Bot panel.
   The Python bot and its API still run on your own PC/WSL; GitHub Pages does not run Python.

   Secure setup (WSL, from the repository's bot folder):
     cd /path/to/ssl-hybrid/bot
     bash stop.sh
     export LOCAL_DASHBOARD_TOKEN="$(openssl rand -hex 32)"
     echo "$LOCAL_DASHBOARD_TOKEN"
     bash start.sh
   Copy the printed token into the Local SSL Hybrid dashboard's Private access token field.
   Keep that token private. Do not commit it, put it in a file in the public repository,
   or share it. If you restart the bot later, set the same token in that terminal before
   running bash start.sh; if you generate a new token, paste the new value into the page.
   If your browser asks permission for the page to access devices on your local network,
   allow it for this page only. The API remains bound to 127.0.0.1 and should not be
   exposed with port forwarding or a public tunnel.

   This cross-origin connection requires a modern browser that permits a GitHub Pages
   site to access the local loopback address. If your browser blocks local-network access,
   use the local dashboard at http://127.0.0.1:8811/ instead. Paper trading only.
