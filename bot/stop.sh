#!/usr/bin/env bash
# stop.sh        -> intentional stop of the BOT; the watchdog keeps the dashboard up and shows "Stopped by user"
# stop.sh --all  -> stop everything (watchdog, bot, dashboard, GitHub Pages publisher)
cd "$(dirname "$0")"; R=$PWD/run; mkdir -p "$R"
echo "{\"at_ms\": $(date +%s%3N)}" > "$R/STOPPED_BY_USER"
stopit() { if [ -f "$R/$1.pid" ] && kill -0 "$(cat "$R/$1.pid")" 2>/dev/null; then kill "$(cat "$R/$1.pid")"; echo "stopped $1 $(cat "$R/$1.pid")"; fi; }
[ "$1" = "--all" ] && stopit supervisor
stopit bot
[ "$1" = "--all" ] && stopit server
[ "$1" = "--all" ] && stopit publisher
sleep 2
for n in bot $([ "$1" = "--all" ] && echo server publisher); do [ -f "$R/$n.pid" ] && kill -0 "$(cat "$R/$n.pid")" 2>/dev/null && kill -9 "$(cat "$R/$n.pid")"; done
[ "$1" = "--all" ] && rm -f "$R/supervisor.pid" "$R/publisher.pid"
echo "bot stopped by user$([ "$1" = "--all" ] && echo '; dashboard, watchdog and publisher stopped too')"
