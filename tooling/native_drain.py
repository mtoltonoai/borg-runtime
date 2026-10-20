#!/usr/bin/env python3
"""Automatic bootstrap-to-native rolling drain, without cancelling tool receipts.

Compatibility bridge for the original runtime, whose HTTP stop cancels tools.
Set its existing durable stop flag WITHOUT sending a cancellation notification.
A running or raced original tool then finishes normally. The legacy bridge is
passive-only: a progress snapshot is not atomic exclusion of dispatch, so it
NEVER sends HTTP stop/cancellation. Execution resumes after
activation, using a durable deployment journal. Unknown writes are never replayed.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import urllib.error
import urllib.request

sys.dont_write_bytecode = True
from native_release import atomic_json

ACTIVE = {"ready", "running", "retrying", "stopping"}


def api(url, session_id, action=None):
    url = url.rstrip("/") + "/api/sessions/" + session_id
    if action:
        url += "/" + action
    request = urllib.request.Request(url, data=b"{}" if action else None,
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def rows(connection):
    result = []
    for sid, raw, stop in connection.execute("SELECT id,body,stop_requested FROM sessions"):
        body = json.loads(raw)
        tools = []
        for raw_tool, in connection.execute("SELECT body FROM tool_calls WHERE session_id=?", (sid,)):
            record = json.loads(raw_tool)
            tools.append(record.get("call", record))
        result.append({"id": sid, "status": body["status"], "progress": body.get("progress"),
                       "stop_requested": bool(stop),
                       "tools": {t["call_id"]: t["status"] for t in tools}})
    return result


def request_drain(database, journal):
    """Journal intent before atomically setting flags; don't rewrite session bodies."""
    if journal.exists():
        value = json.loads(journal.read_text())
        if value.get("database") != str(database):
            raise RuntimeError("drain journal belongs to another database")
        return value
    with sqlite3.connect(database, timeout=30) as db:
        db.execute("PRAGMA synchronous=FULL")
        db.execute("BEGIN IMMEDIATE")
        sessions = rows(db)
        candidates = [s for s in sessions if s["status"] in ACTIVE and not s["stop_requested"]]
        value = {"schema": 1, "database": str(database), "state": "requested",
                 "requested_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                 "sessions": [s["id"] for s in candidates], "before": sessions,
                 "resumed": [], "notified": []}
        atomic_json(journal, value)
        for session in candidates:
            db.execute("UPDATE sessions SET stop_requested=1 WHERE id=?", (session["id"],))
        db.commit()
    return value


def drain(database, runtime_url, journal, wait_seconds=300, interval=0.2):
    value = request_drain(database, journal)
    deadline = time.monotonic() + wait_seconds
    while True:
        with sqlite3.connect(database, timeout=30) as db:
            db.execute("BEGIN IMMEDIATE")
            # A crash after journaling but before committing flags is resumable.
            for sid in value["sessions"]:
                db.execute("UPDATE sessions SET stop_requested=1 WHERE id=?", (sid,))
            observed = rows(db)
            db.commit()
        unsettled = [s for s in observed if any(t == "unknown" for t in s["tools"].values())]
        if unsettled:
            value.update(state="needs_reconciliation", unresolved=[s["id"] for s in unsettled])
            atomic_json(journal, value)
            raise RuntimeError("unknown tool outcome requires original receipt reconciliation")
        active = [s for s in observed if s["status"] in ACTIVE or
                  any(t == "running" for t in s["tools"].values())]
        if not active:
            value.update(state="drained", after=observed)
            atomic_json(journal, value)
            return value
        # Deliberately no HTTP stop here. Even a seemingly inference-only
        # snapshot can race a tool which already passed stopped=false. Let the
        # original actor commit any such tool result and observe the flag itself.
        if time.monotonic() >= deadline:
            value.update(state="waiting_for_boundary", waiting=[s["id"] for s in active])
            atomic_json(journal, value)
            raise TimeoutError("drain waiting for original receipt; no tool was cancelled")
        time.sleep(interval)


def resume(runtime_url, journal, wait_seconds=None, interval=0.2):
    value = json.loads(journal.read_text())
    deadline = None if wait_seconds is None else time.monotonic() + wait_seconds
    value["state"] = "resume_pending"
    atomic_json(journal, value)
    for sid in value["sessions"]:
        if sid in value["resumed"]:
            continue
        while True:
            with sqlite3.connect(value["database"], timeout=30) as db:
                raw, flag = db.execute("SELECT body,stop_requested FROM sessions WHERE id=?", (sid,)).fetchone()
                stored = json.loads(raw)
            status = stored["status"]
            if status in ("needs_resolution", "failed"):
                value["state"] = "needs_reconciliation"
                atomic_json(journal, value)
                raise RuntimeError("session needs owned repair instead of automatic resume: " + sid)
            if not flag:
                # Only the runtime resume transaction (or an explicit concurrent
                # operator action) clears the flag. Observing running is NOT
                # sufficient: a late tool completion can still park that actor.
                value["resumed"].append(sid)
                atomic_json(journal, value)
                break
            if status == "completed":
                with sqlite3.connect(value["database"], timeout=30) as db:
                    db.execute("UPDATE sessions SET stop_requested=0 WHERE id=? AND json_extract(body,'$.status')='completed'", (sid,))
                continue
            if status == "stopped":
                # Stopped can become visible just before its old run guard drops.
                # A 409 is a proven pre-execution refusal; wait and retry it. A
                # lost success is reconciled from the flag rather than reposted.
                try:
                    api(runtime_url, sid, "resume")
                except urllib.error.HTTPError as error:
                    if error.code != 409:
                        raise
                except (OSError, ValueError):
                    with sqlite3.connect(value["database"], timeout=30) as db:
                        observed_flag = db.execute("SELECT stop_requested FROM sessions WHERE id=?", (sid,)).fetchone()[0]
                    if observed_flag:
                        value["state"] = "resume_uncertain"
                        atomic_json(journal, value)
                        raise RuntimeError("resume receipt uncertain; inspect original state") from None
                # Verify the actual committed flag before claiming success.
                continue
            # A timeout while a tool is running must not claim resume and leave
            # stop_requested=1. Keep this detached durable job alive until the
            # safe boundary; then perform/verify the real runtime resume.
            if deadline is not None and time.monotonic() >= deadline:
                atomic_json(journal, value)
                raise TimeoutError("resume still pending original execution boundary")
            time.sleep(interval)
    value["state"] = "resumed"
    atomic_json(journal, value)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("drain", "resume"))
    parser.add_argument("--database", type=Path, default=Path("/home/dev-user/.local/share/borg/state/native.sqlite"))
    parser.add_argument("--runtime-url", default="http://127.0.0.1:8787")
    parser.add_argument("--journal", required=True, type=Path)
    parser.add_argument("--wait-seconds", type=float, default=300)
    args = parser.parse_args()
    value = drain(args.database, args.runtime_url, args.journal, args.wait_seconds) if args.action == "drain" else resume(args.runtime_url, args.journal)
    print(json.dumps(value))


if __name__ == "__main__":
    main()
