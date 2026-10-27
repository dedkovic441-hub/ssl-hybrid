#!/usr/bin/env bash
# Watchdog: keeps paper_bot.py, server.py and publish_loop.sh (GitHub Pages bot.json pusher) alive; checks every 10 s. Started detached by start.sh.
cd "$(dirname "$0")"; D=$PWD; R=$D/run; mkdir -p "$R"
echo $$ > "$R/supervisor.pid"
PORT=${PORT:-8811}
alive() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }
log() { echo "$(TZ=Asia/Jakarta date '+%F %T') WIB [supervisor] $*" >> "$D/supervisor.log"; }
start_bot() { setsid nohup python3 -u "$D/paper_bot.py" >> "$D/bot.stdout.log" 2>&1 < /dev/null & echo $! > "$R/bot.pid"; log "started bot pid $(cat "$R/bot.pid")"; }
start_pub() { setsid nohup bash "$D/publish_loop.sh" >> "$D/publish.log" 2>&1 < /dev/null & echo $! > "$R/publisher.pid"; log "started publisher pid $(cat "$R/publisher.pid") (bot.json -> GitHub Pages every 10 min)"; }
start_srv() { PORT=$PORT setsid nohup python3 -u "$D/server.py" >> "$D/server.log" 2>&1 < /dev/null & echo $! > "$R/server.pid"; log "started server pid $(cat "$R/server.pid") on 127.0.0.1:$PORT"; }
trap 'log "supervisor stopping"; rm -f "$R/supervisor.pid"; exit 0' TERM INT
log "supervisor up pid $$"
while true; do
  touch "$R/supervisor.hb"
  if [ -f "$R/STOPPED_BY_USER" ]; then :   # intentional stop: keep the dashboard up, do not restart the bot
  else alive "$R/bot.pid" || { [ -f "$R/bot.pid" ] && log "bot not running (crash?), restarting"; start_bot; }; fi
  # the publisher keeps running even after a user stop, so the public page shows STOPPED; PUBLISH_DISABLED file turns it off
  [ -f "$R/PUBLISH_DISABLED" ] || alive "$R/publisher.pid" || { [ -f "$R/publisher.pid" ] && log "publisher not running, restarting"; start_pub; }
  alive "$R/server.pid" || { [ -f "$R/server.pid" ] && log "server not running, restarting"; start_srv; }
  # a hung bot (no heartbeat for 5 min) is restarted too
  if [ ! -f "$R/STOPPED_BY_USER" ] && [ -f "$D/live.json" ] && [ $(( $(date +%s) - $(stat -c %Y "$D/live.json") )) -gt 300 ] && alive "$R/bot.pid"; then
    log "bot heartbeat stale (>300 s), restarting"; kill "$(cat "$R/bot.pid")" 2>/dev/null; sleep 5; kill -9 "$(cat "$R/bot.pid")" 2>/dev/null; start_bot
  fi
  sleep 10 & wait $!
done
