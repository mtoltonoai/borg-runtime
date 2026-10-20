#!/usr/bin/env python3
"""Automatic bootstrap-to-native rolling drain, without cancelling tool receipts.

Compatibility bridge for the original runtime, whose HTTP stop cancels tools.
Set its existing durable stop flag WITHOUT sending a cancellation notification.
A running tool then finishes normally; no later tool can start. Only after that
flag is committed and the actor is in inference/read-only discovery do we use
HTTP stop to avoid waiting indefinitely for a model. Execution resumes after
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
SAFE_CANCEL_PROGRESS = {"Waiting for model response", "Loading configured tools"}


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
        for session in active:
            if session["id"] not in value["sessions"]:
                # A newly admitted session or independently stopped actor is not
                # ours to cancel. Fence admission and retry; don't invent ownership.
                continue
            if any(t == "running" for t in session["tools"].values()):
                continue
            if session["id"] in value["notified"]:
                continue
            safe = session["progress"] in SAFE_CANCEL_PROGRESS or session["status"] == "retrying"
            if safe:
                # With the durable flag set, this actor cannot move from inference
                # to a tool. Notify only here, never during a running tool.
                api(runtime_url, session["id"], "stop")
                value["notified"].append(session["id"])
                atomic_json(journal, value)
        if time.monotonic() >= deadline:
            value.update(state="waiting_for_boundary", waiting=[s["id"] for s in active])
            atomic_json(journal, value)
            raise TimeoutError("drain waiting for original receipt; no tool was cancelled")
        time.sleep(interval)


def resume(runtime_url, journal):
    value = json.loads(journal.read_text())
    for sid in value["sessions"]:
        if sid in value["resumed"]:
            continue
        session = api(runtime_url, sid)
        if session["status"] == "stopped":
            try:
                api(runtime_url, sid, "resume")
            except (OSError, ValueError):
                # Reconcile a lost HTTP reply before considering any reattempt.
                session = api(runtime_url, sid)
                if session["status"] == "stopped":
                    raise RuntimeError("resume receipt uncertain; inspect original state") from None
            value["resumed"].append(sid)
        elif session["status"] in ("ready", "running", "retrying"):
            value["resumed"].append(sid)
        elif session["status"] == "completed":
            # Completed during the drain: no reason to rerun the user's finished
            # task. Remove only our compatibility flag, leaving the body intact.
            with sqlite3.connect(value["database"], timeout=30) as db:
                db.execute("UPDATE sessions SET stop_requested=0 WHERE id=? AND json_extract(body,'$.status')='completed'", (sid,))
            value["resumed"].append(sid)
        else:
            raise RuntimeError("session needs owned repair instead of automatic resume: " + sid)
        atomic_json(journal, value)
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
