#!/usr/bin/env python3
"""Pause / resume the paper bot. While paused the bot takes no new entries (exits still follow the rules).
Files: run/PAUSED (current pause: since_ms, reason) and run/pauses.json (full history, used by the
deterministic recompute so restarts reproduce skipped entries). Usage: pausectl.py pause [reason] | resume | status"""
import os, sys, json, time, fcntl
HERE = os.path.dirname(os.path.abspath(__file__)); R = os.path.join(HERE, "run")
FLAG, HIST, LOCK = os.path.join(R, "PAUSED"), os.path.join(R, "pauses.json"), os.path.join(R, ".pause.lock")
def _w(p, o):
    t = p + ".tmp"; open(t, "w").write(json.dumps(o)); os.replace(t, p)
def history():
    try: return json.load(open(HIST))
    except Exception: return []
def current():
    try: return json.load(open(FLAG))
    except Exception: return None
def pause(reason="paused from the dashboard"):
    os.makedirs(R, exist_ok=True)
    with open(LOCK, "w") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        cur = current()
        if cur: return cur
        now = int(time.time() * 1000); cur = {"since_ms": now, "reason": (reason or "paused")[:200]}
        h = history(); h.append({"from_ms": now, "to_ms": None, "reason": cur["reason"]}); _w(HIST, h); _w(FLAG, cur)
        return cur
def resume():
    os.makedirs(R, exist_ok=True)
    with open(LOCK, "w") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        now = int(time.time() * 1000); h = history()
        for p in h:
            if p.get("to_ms") is None: p["to_ms"] = now
        _w(HIST, h)
        if os.path.exists(FLAG): os.remove(FLAG)
        return {"resumed_ms": now}
if __name__ == "__main__":
    a = sys.argv[1:] or ["status"]
    if a[0] == "pause": print(pause(" ".join(a[1:]) or "paused from the command line"))
    elif a[0] == "resume": print(resume())
    else: print({"paused": current(), "history": history()})
