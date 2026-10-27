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
