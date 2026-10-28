#!/usr/bin/env bash
# Idempotent: clears an intentional stop and makes sure the watchdog (which runs the bot + dashboard) is up.
cd "$(dirname "$0")"; R=$PWD/run; mkdir -p "$R"
# Load private local dashboard token from ignored bot/.env, if present.
if [ -f "$PWD/.env" ]; then
  set -a
  . "$PWD/.env"
  set +a
fi
rm -f "$R/STOPPED_BY_USER"
if [ -f "$R/supervisor.pid" ] && kill -0 "$(cat "$R/supervisor.pid")" 2>/dev/null; then
  echo "watchdog already running ($(cat "$R/supervisor.pid")); bot $(cat "$R/bot.pid" 2>/dev/null) / server $(cat "$R/server.pid" 2>/dev/null) are kept alive (a stopped bot restarts within 10 s)"; exit 0
fi
setsid nohup bash "$PWD/supervisor.sh" > /dev/null 2>&1 < /dev/null &
sleep 3
echo "started: supervisor $(cat "$R/supervisor.pid" 2>/dev/null), bot $(cat "$R/bot.pid" 2>/dev/null), server $(cat "$R/server.pid" 2>/dev/null)  ->  http://127.0.0.1:${PORT:-8811}/"
