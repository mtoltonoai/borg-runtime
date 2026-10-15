#!/usr/bin/env python3
"""Prepare an ordinary claimed release candidate; never approve or deploy it."""
from __future__ import annotations

import argparse
import hashlib
import http.client
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
import stat
import sys
import threading
import time
import urllib.parse

MAX_BODY = 1_048_576
WIRE_REVISION = 8
VERIFIER_SHA = "1c0be524cf602bbe4649ade54672ae01f19775ce397643c26366f1690132041e"
PROTOCOL_SOURCE_SHA = "9afd1f1c913e4d011fc2537e3d1894acbf7d3fc42e842283fdd581914d189a06"
QUALIFICATION = {"artifact": "claimed", "repository_submission": "unconfigured",
                 "checks": "unconfigured", "human_rollout": "unconfigured"}
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
COMMIT = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")


def canonical(value) -> bytes:
    data = (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n").encode()
    if len(data) > MAX_BODY:
        raise ValueError("request/receipt exceeds bound")
    return data


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def path_input(path: Path) -> Path:
    path = path.absolute()
    if ".." in path.parts or any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError("symlink/traversal input path refused")
    return path


def write_bytes(path: Path, data: bytes) -> None:
    with path.open("xb") as target:
        target.write(data)
        target.flush()
        os.fsync(target.fileno())
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save_receipt(output: Path, receipt: dict) -> None:
    pending = output / "receipt.pending"
    with pending.open("wb") as target:
        target.write(canonical(receipt))
        target.flush()
        os.fsync(target.fileno())
    pending.replace(output / "receipt.json")
    fd = os.open(output, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def strict_json(data: bytes):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate response field")
            result[key] = value
        return result
    def nonfinite(value):
        raise ValueError("nonfinite JSON refused")
    try:
        return json.loads(data, object_pairs_hook=pairs, parse_constant=nonfinite)
    except (UnicodeError, RecursionError, json.JSONDecodeError) as error:
        raise ValueError("invalid or excessive JSON response") from error


def read_json(path: Path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_BODY:
        raise ValueError("receipt input type/size refused")
    return strict_json(path.read_bytes())


def identifier(value) -> None:
    if (not isinstance(value, str) or not 0 < len(value.encode()) <= 256
            or any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in value)):
        raise ValueError("invalid bounded identity")


def number(value) -> None:
    if type(value) is not int or not 0 < value <= (1 << 63) - 1:
        raise ValueError("invalid bounded revision/size")


def registration(value) -> dict:
    if not isinstance(value, dict) or set(value) != {"repository_id", "registration_revision"}:
        raise ValueError("checkpoint registration shape mismatch")
    identifier(value["repository_id"])
    identifier(value["registration_revision"])
    return value


def checkpoint(value, commit: str) -> dict:
    if not isinstance(value, dict) or set(value) != {"registration", "repository", "commit", "remote_ref", "operation_id"}:
        raise ValueError("checkpoint shape mismatch")
    registration(value["registration"])
    if value["commit"] != commit:
        raise ValueError("accepted checkpoint differs from verified package source")
    identifier(value["operation_id"])
    for key in ("repository", "remote_ref"):
        if not isinstance(value[key], str) or not 0 < len(value[key].encode()) <= 4096 or any(ord(c) < 32 for c in value[key]):
            raise ValueError("checkpoint association bound")
    return value


def verifier():
    path = path_input(Path(__file__).absolute().with_name("package_v2.py"))
    if file_sha(path) != VERIFIER_SHA:
        raise ValueError("installed package verifier differs from selected source")
    spec = importlib.util.spec_from_file_location("borg_release_package_verifier", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def artifact(package: Path, expected_sha: str, commit: str) -> tuple[dict, dict]:
    if not isinstance(expected_sha, str) or not HEX64.fullmatch(expected_sha) or not isinstance(commit, str) or not COMMIT.fullmatch(commit):
        raise ValueError("external package digest/full commit required")
    module = verifier()
    manifest = module.verify(package, expected_sha, commit)
    binary = manifest["files"]["bin/borg-runtime"]
    result = {"format": manifest["format"], "package_sha256": expected_sha, "package_bytes": package.stat().st_size,
              "manifest_sha256": sha(module.canonical(manifest)), "source_commit": manifest["source"]["commit"],
              "source_tree": manifest["source"]["tree"], "object_format": manifest["source"]["object_format"],
              "binary_sha256": binary["sha256"], "binary_bytes": binary["size"], "target": binary["target"]}
    return result, manifest


def endpoint(value: str) -> tuple[str, int]:
    if not isinstance(value, str) or any(ord(c) < 33 for c in value) or "\\" in value or "?" in value or "#" in value:
        raise ValueError("controller must be a literal loopback HTTP origin")
    parsed = urllib.parse.urlsplit(value)
    if (parsed.scheme != "http" or parsed.username is not None or parsed.password is not None
            or parsed.path not in ("", "/") or not parsed.hostname or "%" in parsed.netloc):
        raise ValueError("controller URL path/credentials/scheme refused")
    try:
        ip = ipaddress.ip_address(parsed.hostname)
        port = parsed.port or 80
    except ValueError as error:
        raise ValueError("controller requires a literal loopback IP") from error
    if not ip.is_loopback or not 1 <= port <= 65535:
        raise ValueError("non-loopback controller refused")
    return str(ip), port


def request(operation: str, value: dict, stable_id: str) -> dict:
    return {"protocol_revision": WIRE_REVISION, "request_id": operation + "-" + sha(stable_id.encode()), "operation": operation, "input": value}


class Client:
    def __init__(self, origin: str, output: Path, receipt: dict):
        self.host, self.port = endpoint(origin)
        self.output, self.receipt = output, receipt

    def call(self, value: dict, label: str) -> dict:
        encoded = canonical(value)
        write_bytes(self.output / (label + "-request.json"), encoded)
        entry = {"label": label, "request_sha256": sha(encoded)}
        self.receipt["calls"].append(entry)
        save_receipt(self.output, self.receipt)
        connection = http.client.HTTPConnection(self.host, self.port, timeout=10)
        timer = None
        try:
            connection.connect()  # Literal IP; no DNS, proxy or environment route.
            sock = connection.sock
            def stop():
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            timer = threading.Timer(25, stop)
            timer.daemon = True
            timer.start()
            connection.request("POST", "/api/operations", body=encoded,
                               headers={"Content-Type": "application/json", "Connection": "close"})
            response = connection.getresponse()
            entry["http_status"] = response.status
            if sum(len(k) + len(v) for k, v in response.getheaders()) > 16 * 1024:
                raise ValueError("controller response headers exceed bound")
            if response.status != 200:
                raise ValueError("controller HTTP status refused; redirects are never followed")
            lengths = response.headers.get_all("Content-Length", [])
            if (len(lengths) != 1 or not lengths[0].isdigit() or int(lengths[0]) > MAX_BODY
                    or response.headers.get("Transfer-Encoding") is not None
                    or response.headers.get("Content-Encoding", "identity").lower() != "identity"
                    or response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != "application/json"):
                raise ValueError("controller response framing/type/bound refused")
            body = response.read(MAX_BODY + 1)
            if len(body) != int(lengths[0]) or len(body) > MAX_BODY:
                raise ValueError("controller response incomplete/oversized")
            write_bytes(self.output / (label + "-response.json"), body)
            entry["response_sha256"] = sha(body)
            result = strict_json(body)
            if not isinstance(result, dict) or result.get("request_id") != value["request_id"]:
                raise ValueError("controller reply request identity mismatch")
            validate_context(result, value)
            return result
        except (OSError, http.client.HTTPException) as error:
            raise ValueError("controller transport failed; mutation outcome may be unknown") from error
        finally:
            if timer is not None:
                timer.cancel()
                timer.join()
            connection.close()
            save_receipt(self.output, self.receipt)


REPLY_CONTEXT = {"request_id", "commit", "observed_controller_revision"}


def validate_context(reply: dict, request: dict) -> None:
    if not REPLY_CONTEXT <= set(reply):
        raise ValueError("reply lacks committed/observed context fields")
    observed, commit = reply["observed_controller_revision"], reply["commit"]
    if observed is not None and (type(observed) is not int or not 0 <= observed <= (1 << 63) - 1):
        raise ValueError("invalid observed controller revision")
    operation = request["operation"]
    original_id = request["input"].get("operation_id")
    lookup = (operation == "get_operation" and reply.get("status") == "ok"
              and reply.get("data", {}).get("kind") == "operation"
              and reply.get("data", {}).get("value", {}).get("kind") == "application_mutation")
    if commit is not None:
        if not isinstance(commit, dict) or set(commit) != {"operation_id", "controller_revision"}:
            raise ValueError("invalid committed mutation context")
        identifier(commit["operation_id"])
        number(commit["controller_revision"])
        if (reply.get("status") not in ("ok", "refused") or observed is None
                or commit["controller_revision"] > observed
                or operation != "prepare_release" and not lookup
                or commit["operation_id"] != original_id):
            raise ValueError("committed mutation context differs from request")
    elif lookup or operation == "prepare_release" and reply.get("status") == "ok":
        raise ValueError("successful mutation lacks committed context")
    if reply.get("status") == "ok" and observed is None:
        raise ValueError("successful reply lacks observed revision")


def data(reply: dict, kind: str):
    if set(reply) != REPLY_CONTEXT | {"status", "data"} or reply["status"] != "ok" or not isinstance(reply["data"], dict) or set(reply["data"]) != {"kind", "value"} or reply["data"]["kind"] != kind:
        raise ValueError("controller did not return the requested successful data")
    return reply["data"]["value"]


def task_binding(task: dict, task_id: str, commit: str) -> tuple[int, dict, str]:
    if (not isinstance(task, dict) or task.get("id") != task_id or task.get("status") != "succeeded"
            or task.get("input", {}).get("definition", {}).get("executor") != "coding"):
        raise ValueError("release requires the actual succeeded coding task")
    number(task.get("revision"))
    result, attempt = task.get("result"), task.get("attempt")
    if (not isinstance(result, dict) or not isinstance(attempt, dict)
            or result.get("attempt_id") != attempt.get("id") or attempt.get("task_id") != task_id
            or attempt.get("stopped") is not True or attempt.get("uncertain") is not False or attempt.get("phase") != "stopped"):
        raise ValueError("task lacks an exact accepted/stopped attempt")
    identifier(result["attempt_id"])
    cp = checkpoint(result.get("checkpoint"), commit)
    if task.get("selected_checkpoint") != cp:
        raise ValueError("accepted and selected checkpoints differ")
    repository = task.get("input", {}).get("repository")
    if (not isinstance(repository, dict) or registration(repository.get("registration")) != cp["registration"]
            or repository.get("checkpoint_remote") != cp["repository"]):
        raise ValueError("checkpoint differs from accepted repository registration/destination")
    return task["revision"], cp, result["attempt_id"]


def candidate(value: dict, original: dict, accepted_attempt: str) -> dict:
    fields = {"id", "task_id", "task_revision", "accepted_attempt_id", "checkpoint", "artifact", "revision", "qualification"}
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("release candidate shape mismatch")
    identifier(value["id"])
    number(value["revision"])
    if (value["task_id"] != original["task_id"] or value["task_revision"] != original["expected_task_revision"]
            or value["accepted_attempt_id"] != accepted_attempt or value["checkpoint"] != original["checkpoint"]
            or value["artifact"] != original["artifact"] or value["qualification"] != QUALIFICATION):
        raise ValueError("release binding or protected-status mismatch")
    return value


def outcome(reply: dict, prepared: dict, accepted_attempt: str) -> tuple[str, dict | None]:
    status = reply.get("status")
    if status == "ok":
        return "prepared", candidate(data(reply, "release"), prepared, accepted_attempt)
    if status == "refused" and set(reply) == REPLY_CONTEXT | {"status", "code", "message"}:
        if not isinstance(reply["code"], str) or not isinstance(reply["message"], str):
            raise ValueError("invalid refusal fields")
        return ("refused" if reply["commit"] is not None else "uncertain"), None
    if status in ("pending", "uncertain") and reply.get("operation_id") == prepared["operation_id"]:
        allowed = REPLY_CONTEXT | {"status", "operation_id"} | ({"message"} if status == "uncertain" else set())
        if set(reply) == allowed:
            return status, None
    raise ValueError("unrecognized mutation outcome")


def original_input(path: Path, expected_sha: str, commit: str) -> tuple[dict, dict, dict]:
    old = read_json(path / "receipt.json")
    saved = read_json(path / "prepare-request.json")
    if (old.get("expected_package_sha256") != expected_sha or old.get("expected_commit") != commit
            or old.get("prepare_request_sha256") != sha(canonical(saved))
            or saved.get("operation") != "prepare_release" or saved.get("protocol_revision") != WIRE_REVISION):
        raise ValueError("original request/external identity mismatch")
    supplied = saved.get("input")
    if not isinstance(supplied, dict) or set(supplied) != {"operation_id", "task_id", "expected_task_revision", "checkpoint", "artifact"}:
        raise ValueError("original preparation shape mismatch")
    identifier(supplied["operation_id"])
    identifier(supplied["task_id"])
    number(supplied["expected_task_revision"])
    checkpoint(supplied["checkpoint"], commit)
    if saved != request("prepare_release", supplied, supplied["operation_id"]):
        raise ValueError("original request identity mismatch")
    identifier(old.get("accepted_attempt_id"))
    accepted_task = read_json(path / "accepted-task.json")
    revision, cp, accepted = task_binding(accepted_task, supplied["task_id"], commit)
    if (old.get("accepted_task_sha256") != sha(canonical(accepted_task)) or revision != supplied["expected_task_revision"]
            or cp != supplied["checkpoint"] or accepted != old["accepted_attempt_id"]):
        raise ValueError("original accepted task association mismatch")
    return old, saved, accepted_task


def execute(action: str, controller: str, output: Path, expected_sha: str, commit: str,
            package_path: Path | None = None, task_id: str | None = None,
            operation_id: str | None = None, previous: Path | None = None) -> dict:
    output = path_input(output)
    output.mkdir(mode=0o700)
    receipt = {"format": "borg-release-preparation/1", "action": action, "status": "failed", "controller": controller,
               "expected_package_sha256": expected_sha, "expected_commit": commit, "started_unix_ns": time.time_ns(),
               "adapter_sha256": file_sha(Path(__file__)), "verifier_sha256": VERIFIER_SHA,
               "protocol_source_sha256": PROTOCOL_SOURCE_SHA, "calls": [], "mutation_attempted": False}
    try:
        client = Client(controller, output, receipt)  # Validates origin but opens no socket.
        if action == "prepare":
            identifier(task_id)
            identifier(operation_id)
            package_path = path_input(package_path)
            art, manifest = artifact(package_path, expected_sha, commit)
            receipt["package"] = str(package_path)
            write_bytes(output / "verified-manifest.json", verifier().canonical(manifest))
            current = data(client.call(request("get_task", {"task_id": task_id}, operation_id), "get-task"), "task")
            revision, cp, accepted = task_binding(current, task_id, commit)
            prepared = {"operation_id": operation_id, "task_id": task_id, "expected_task_revision": revision, "checkpoint": cp, "artifact": art}
            saved = request("prepare_release", prepared, operation_id)
        elif action in ("reconcile", "retry"):
            previous = path_input(previous)
            old, saved, current = original_input(previous, expected_sha, commit)
            package_path = path_input(Path(old["package"]))
            art, manifest = artifact(package_path, expected_sha, commit)
            if art != saved["input"]["artifact"]:
                raise ValueError("original artifact no longer matches verified bytes")
            receipt.update(package=str(package_path), original_receipt=str(previous))
            write_bytes(output / "verified-manifest.json", verifier().canonical(manifest))
            prepared, accepted = saved["input"], old["accepted_attempt_id"]
        else:
            raise ValueError("unsupported adapter action")
        receipt.update(accepted_attempt_id=accepted, prepare_request_sha256=sha(canonical(saved)), operation_id=prepared["operation_id"],
                       accepted_task_sha256=sha(canonical(current)))
        write_bytes(output / "accepted-task.json", canonical(current))
        write_bytes(output / "prepare-request.json", canonical(saved))
        save_receipt(output, receipt)  # Durable exact operation before any mutation.
        if action == "reconcile":
            reply = client.call(request("get_operation", {"operation_id": prepared["operation_id"]}, prepared["operation_id"]), "lookup")
            if reply.get("status") == "refused" and reply.get("code") == "not_found":
                if set(reply) != REPLY_CONTEXT | {"status", "code", "message"} or not isinstance(reply["message"], str):
                    raise ValueError("invalid lookup refusal")
                receipt["status"] = "not_observed"  # Not proof that an in-flight original can never commit.
            else:
                record = data(reply, "operation")
                if (not isinstance(record, dict) or record.get("id") != prepared["operation_id"]
                        or set(record) != {"id", "task_id", "attempt_id", "generation", "kind", "intended", "state", "outcome"}
                        or record.get("kind") != "application_mutation" or record.get("state") != "settled"
                        or record.get("intended") != {"operation": "prepare_release", "input": prepared}
                        or not isinstance(record.get("outcome"), dict) or REPLY_CONTEXT & set(record["outcome"])):
                    raise ValueError("operation lookup differs from original request")
                settled = {"request_id": saved["request_id"], "commit": reply["commit"],
                           "observed_controller_revision": reply["observed_controller_revision"], **record["outcome"]}
                validate_context(settled, saved)
                receipt["status"], release = outcome(settled, prepared, accepted)
                if release is not None:
                    receipt["candidate"] = release
        else:
            # Refuse ordinary local drift before sending; no implication that
            # same-UID files are immutable or independently protected evidence.
            if artifact(package_path, expected_sha, commit)[0] != prepared["artifact"]:
                raise ValueError("package changed before preparation")
            receipt.update(mutation_attempted=True, status="uncertain")
            save_receipt(output, receipt)
            reply = client.call(saved, "mutation")  # Exactly once; no automatic resend.
            receipt["status"], release = outcome(reply, prepared, accepted)
            if release is not None:
                receipt["candidate"] = release
        return receipt
    except (OSError, ValueError, KeyError, TypeError, AttributeError, RecursionError) as error:
        receipt["error"] = str(error)
        raise
    finally:
        receipt["finished_unix_ns"] = time.time_ns()
        save_receipt(output, receipt)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    for action in ("prepare", "reconcile", "retry"):
        p = commands.add_parser(action)
        p.add_argument("--controller", required=True)
        p.add_argument("--output", type=Path, required=True)
        p.add_argument("--expect-sha256", required=True)
        p.add_argument("--expect-commit", required=True)
        if action == "prepare":
            p.add_argument("--package", type=Path, required=True)
            p.add_argument("--task-id", required=True)
            p.add_argument("--operation-id", required=True)
        else:
            p.add_argument("--original-receipt", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = execute(args.action, args.controller, args.output, args.expect_sha256, args.expect_commit,
                         getattr(args, "package", None), getattr(args, "task_id", None), getattr(args, "operation_id", None), getattr(args, "original_receipt", None))
        print(json.dumps({"status": result["status"], "operation_id": result["operation_id"], "receipt": str(args.output / "receipt.json")}, sort_keys=True))
        return 0 if result["status"] == "prepared" else 2
    except (OSError, ValueError, KeyError, TypeError, AttributeError, RecursionError) as error:
        print("Release preparation failed: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
