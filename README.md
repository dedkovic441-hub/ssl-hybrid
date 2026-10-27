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
   Note: ssl_core.py has data paths set to /workspace/ssl_hybrid; edit DATA/OUT
   at the top to a folder on your machine. Native Windows needs WSL.

4) Pool sizing + TP/SL (since 2026-10-27 17:50 WIB)
   bot/config.json:
     "sizing": "pool"      one shared cash pool (the balance carried over);
                           "coin" = old mode, each coin its own $100 slice
     "max_positions": 2    at most 2 open longs; trade size = pool cash / free slots
                           (compounding). More up arrows than free slots on one bar:
                           the larger (close - exit line)/ATR wins, the rest are
                           logged "skipped: slots full" (not queued).
     "tp_usd": 5, "sl_usd": 2   per position, net of the entry fee and the 0.1% exit
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
   Backtest: python3 bot/bt_pool.py (per-coin $100 vs pool max-2 vs pool + TP5/SL2).
