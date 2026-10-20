#!/usr/bin/env python3
"""Apply/verify versioned native role profiles and tool grants on the shared Board.

No service restarts, host JSON writes, credential values or automatic replay of
uncertain writes. Entry updates are skipped when equal; lost acknowledgements
are reconciled against the exact Board entry before reporting success.
"""
from __future__ import annotations
import argparse
import copy
import hashlib
import http.client
import json
import os
import re
from pathlib import Path
import sys
import urllib.error
import urllib.parse
import urllib.request

LIMITS = {"model": "gpt-6-astra", "reasoning_effort": "max", "max_output_tokens": 128000,
          "context_window_tokens": 1050000, "max_turns": None, "max_retries": None}
REQUIRED_TOOLS = {"board", "workspace", "native"}


class ConfigError(Exception):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def validate(plan):
    if plan.get("version") != 1:
        raise ConfigError("unsupported deployment metadata version")
    profile = plan.get("runtime_profile", {})
    if any(key not in profile or profile[key] != value for key, value in LIMITS.items()):
        raise ConfigError("selected full model limits and explicit unlimited retries must be preserved")
    for key in ("retry_initial_ms", "retry_max_ms"):
        if not isinstance(profile.get(key), int) or isinstance(profile[key], bool) or profile[key] <= 0:
            raise ConfigError("retry delays must be positive integers")
    if not isinstance(profile.get("retry_jitter_percent"), int) or not 0 <= profile["retry_jitter_percent"] <= 100:
        raise ConfigError("retry jitter percent must be in 0..100")
    if "api_key" in profile or "bearer_token" in profile:
        raise ConfigError("credential values do not belong in Board profiles")
    provider = urllib.parse.urlsplit(profile.get("provider_url", ""))
    if provider.scheme not in {"http", "https"} or not provider.hostname or provider.username or provider.password or provider.query or provider.fragment:
        raise ConfigError("provider URL must not carry credentials or unsupported schemes")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", profile.get("api_key_env", "")):
        raise ConfigError("provider credentials must be environment variable references")
    roles = plan.get("roles")
    if not isinstance(roles, list) or not roles or any(not isinstance(r, str) or not r for r in roles) or len(set(roles)) != len(roles):
        raise ConfigError("roles must be distinct nonempty names")
    grants = plan.get("tools", [])
    seen = set()
    names = set()
    for grant in grants:
        entry = grant.get("entry_id")
        if not isinstance(entry, str) or not entry or entry in seen:
            raise ConfigError("tool entry IDs must be distinct nonempty names")
        seen.add(entry)
        payload = grant.get("payload", {})
        if payload.get("kind") == "mcp":
            name = payload.get("name")
            names.add(name)
            if "headers" in payload or "authorization" in payload or "api_key" in payload:
                raise ConfigError("credential values and literal headers do not belong in tool grants")
            if any(not isinstance(v, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", v) for v in payload.get("headers_env", {}).values()):
                raise ConfigError("MCP credentials must be environment variable references")
            if "bound_agent_id" in payload:
                raise ConfigError("MCP bound identity is runtime-owned, not a grant value")
            if any(k.lower() == "x-fleet-agent" for k in payload.get("headers_env", {})):
                raise ConfigError("new Board identities must not require per-identity host environment variables")
            endpoint = payload.get("url", "").replace("{runtime_url}", "http://127.0.0.1:8787").replace("{session_id}", "session").replace("{agent_id}", "agent")
            url = urllib.parse.urlsplit(endpoint)
            if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password or url.query or url.fragment:
                raise ConfigError("MCP endpoints must be HTTP(S) without credentials, query or fragment")
        elif payload.get("kind") != "provider" or not isinstance(payload.get("tool"), dict):
            raise ConfigError("tool grants must use core's mcp/provider contract")
    if not REQUIRED_TOOLS.issubset(names):
        raise ConfigError("board/workspace/native grants are required for native engineering roles")
    for role, overrides in plan.get("role_tool_overrides", {}).items():
        if role not in roles or not isinstance(overrides, dict):
            raise ConfigError("tool overrides must name a declared role")
        for entry_id, extra in overrides.items():
            if entry_id not in seen or not isinstance(extra, dict) or set(extra) - {"allowed_agent_ids", "tools_allowlist"}:
                raise ConfigError("role tool overrides may only configure lifecycle targets and method allowlists")
            for values in extra.values():
                if not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values):
                    raise ConfigError("tool permissions must be arrays of nonempty names")
    agents = plan.get("agents")
    if not isinstance(agents, dict) or not agents:
        raise ConfigError("managed identities are required")
    for agent, spec in agents.items():
        if not isinstance(agent, str) or not agent or spec.get("role") not in roles:
            raise ConfigError("every managed identity must name a declared role")
        metadata = spec.get("metadata", {})
        if not isinstance(metadata, dict):
            raise ConfigError("identity metadata must be an object")
        for overrides in (metadata, metadata.get("native", {})):
            if isinstance(overrides, dict) and any(k in overrides and overrides[k] != value for k, value in LIMITS.items()):
                raise ConfigError("identity overrides must not lower selected limits or reintroduce finite retries")
    return plan


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


class Board:
    def __init__(self, url, token=None, timeout=15):
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ConfigError("Board URL must be HTTP(S) without embedded credentials")
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout
        self.opener = urllib.request.build_opener(NoRedirect)

    def request(self, method, path, body=None):
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        data = None
        if body is not None:
            data = canonical(body).encode()
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.url + path, data=data, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                raw = response.read(4 * 1024 * 1024 + 1)
                if len(raw) > 4 * 1024 * 1024 or response.headers.get_content_type() != "application/json":
                    raise ConfigError("unexpected Board response type/size")
                return json.loads(raw)
        except urllib.error.HTTPError as error:
            code = error.code
            error.close()
            raise ConfigError(f"Board HTTP {code}") from None
        except (urllib.error.URLError, OSError, http.client.HTTPException):
            raise ConfigError("Board transport failed; write outcome requires read-back") from None
        except (ValueError, UnicodeError):
            raise ConfigError("invalid Board JSON response") from None


def path_part(value):
    return urllib.parse.quote(value, safe="")


def find_entry(state, entry_id):
    if not isinstance(state, dict) or not isinstance(state.get("entries"), list):
        raise ConfigError("invalid Board config-set shape")
    matches = [e for e in state["entries"] if e.get("id") == entry_id]
    if len(matches) > 1:
        raise ConfigError("duplicate Board config entry ID")
    return matches[0] if matches else None


def equal_entry(current, desired):
    return current is not None and current.get("enabled") is desired["enabled"] and current.get("scope") in (None, []) \
        and current.get("payload") == desired["payload"] and current.get("position") == desired["position"]


def desired_metadata(plan, spec):
    # Flat metadata preserves the selected existing identities during the migration;
    # role/profile is reusable by a NEW identity with only role+harness+charter.
    return {**copy.deepcopy(plan["runtime_profile"]), **copy.deepcopy(spec.get("metadata", {})),
            "role": spec["role"], "harness": "native"}


def metadata_matches(current, desired):
    metadata = current.get("metadata", {})
    return isinstance(metadata, dict) and all(key in metadata and metadata[key] == value for key, value in desired.items())


def tool_entries(plan, role):
    entries = copy.deepcopy(plan["tools"])
    for entry in entries:
        entry["payload"].update(plan.get("role_tool_overrides", {}).get(role, {}).get(entry["entry_id"], {}))
    return entries


def configure(board, plan, mode="verify", actor="pm"):
    validate(plan)
    outcomes = []
    valid = True
    prepared = []
    for role in plan["roles"]:
        # Explicit migration of the alias introduced during parallel preparation.
        # Only the exact recorded predecessor is touched, never arbitrary grants.
        role_path = f"/api/roles/{path_part(role)}/config/tool"
        for retired in plan.get("retire_tool_entries", []):
            current = find_entry(board.request("GET", role_path), retired["entry_id"])
            if current is None or current.get("enabled") is False:
                continue
            if current.get("payload") != retired["expected_payload"]:
                raise ConfigError("retired tool alias changed; refusing to disable an unrelated grant")
            action = "would_disable"
            if mode == "verify":
                action, valid = "mismatch", False
            elif mode == "apply":
                try:
                    board.request("POST", role_path, {"entry_id": retired["entry_id"], "enabled": False, "principal": actor})
                except ConfigError:
                    pass
                receipt = find_entry(board.request("GET", role_path), retired["entry_id"])
                if not receipt or receipt.get("enabled") is not False:
                    raise ConfigError("disabled grant receipt unavailable; reconcile before another write")
                action = "disabled_exact_alias"
            outcomes.append({"target": role_path + "/" + retired["entry_id"], "action": action})
        prepared.append((f"/api/roles/{path_part(role)}/config/runtime", "profile", plan["runtime_profile"], 0))
        prepared.extend((f"/api/roles/{path_part(role)}/config/tool", g["entry_id"], g["payload"], index)
                        for index, g in enumerate(tool_entries(plan, role)))
    for path, entry_id, payload, position in prepared:
        desired = {"entry_id": entry_id, "enabled": True, "scope": [], "payload": payload, "position": position, "principal": actor}
        current = find_entry(board.request("GET", path), entry_id)
        target = path + "/" + entry_id
        if equal_entry(current, desired):
            action = "unchanged"
        elif mode == "plan":
            action = "would_update"
        elif mode == "verify":
            action, valid = "mismatch", False
        else:
            write_error = None
            try:
                board.request("POST", path, desired)
            except ConfigError as error:
                write_error = error
            # A stable ID is not permission to blindly repeat a lost mutation reply.
            # The exact value must be visible through the independent GET receipt.
            try:
                receipt = find_entry(board.request("GET", path), entry_id)
            except ConfigError:
                raise ConfigError("config mutation receipt unavailable; reconcile before another write") from None
            if not equal_entry(receipt, desired):
                raise ConfigError("config update not confirmed by read-back; do not assume deployment success") from None
            action = "reconciled" if write_error else "updated"
        outcomes.append({"target": target, "action": action})
    for agent, spec in plan["agents"].items():
        path = f"/api/agents/{path_part(agent)}"
        desired = desired_metadata(plan, spec)
        current = board.request("GET", path)
        if "remove_runtime_profile_if" in spec:
            own_path = path + "/config/runtime"
            own = find_entry(board.request("GET", own_path), "profile")
            if own is not None:
                if own.get("payload") != spec["remove_runtime_profile_if"]:
                    raise ConfigError("agent profile changed; refusing to remove an unrelated override")
                action = "would_inherit_role"
                if mode == "verify":
                    action, valid = "mismatch", False
                elif mode == "apply":
                    try:
                        board.request("DELETE", own_path + "/profile?principal=" + path_part(actor))
                    except ConfigError:
                        pass
                    if find_entry(board.request("GET", own_path), "profile") is not None:
                        raise ConfigError("profile inheritance receipt unavailable; reconcile before another write")
                    action = "inherited_role_profile"
                outcomes.append({"target": own_path + "/profile", "action": action})
        if current.get("id") != agent:
            raise ConfigError("Board identity mismatch")
        if metadata_matches(current, desired):
            action = "unchanged"
        elif mode == "plan":
            action = "would_update"
        elif mode == "verify":
            action, valid = "mismatch", False
        else:
            write_error = None
            try:
                board.request("PATCH", path, {"metadata": desired, "principal": actor})
            except ConfigError as error:
                write_error = error
            try:
                receipt = board.request("GET", path)
            except ConfigError:
                raise ConfigError("identity mutation receipt unavailable; reconcile before another write") from None
            if not metadata_matches(receipt, desired):
                raise ConfigError("identity update not confirmed by read-back") from None
            action = "reconciled" if write_error else "updated"
        outcomes.append({"target": path + "/metadata", "action": action})
        if mode != "plan":
            for kind, entries in (("runtime", [{"entry_id": "profile", "payload": plan["runtime_profile"]}]), ("tool", tool_entries(plan, spec["role"]))):
                state = board.request("GET", path + f"/config/{kind}?effective=true")
                mismatches = []
                for expected in entries:
                    actual = find_entry(state, expected["entry_id"])
                    if not actual or actual.get("enabled") is not True or actual.get("scope") not in (None, []) or actual.get("payload") != expected["payload"]:
                        mismatches.append(expected["entry_id"])
                # An unexpected enabled grant can widen tools or fail core decoding.
                expected_ids = {e["entry_id"] for e in entries}
                extras = [e.get("id") for e in state["entries"] if e.get("enabled") and e.get("id") not in expected_ids]
                valid = valid and not mismatches and not extras
                outcomes.append({"target": path + f"/config/{kind}?effective=true", "action": "verified" if not mismatches and not extras else "mismatch",
                                 "missing_or_overridden": mismatches, "unexpected_enabled": extras,
                                 "role_version": state.get("role_version"), "agent_version": state.get("agent_version")})
    return {"ok": valid, "mode": mode, "scope": "Board metadata only; runtime/service activation remains integrator-owned",
            "config_sha256": hashlib.sha256(canonical(plan).encode()).hexdigest(), "outcomes": outcomes}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--board", default="http://127.0.0.1:8079")
    parser.add_argument("--config", type=Path, default=Path(__file__).resolve().parents[1] / "config/native-board-profiles.json")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--verify", action="store_true")
    parser.add_argument("--actor", default="pm")
    parser.add_argument("--token-env")
    parser.add_argument("--report-out", type=Path)
    args = parser.parse_args(argv)
    try:
        plan = validate(json.loads(args.config.read_text()))
        token = os.environ.get(args.token_env) if args.token_env else None
        if args.token_env and not token:
            raise ConfigError("requested Board credential environment variable is unset")
        report = configure(Board(args.board, token), plan, "apply" if args.apply else "verify" if args.verify else "plan", args.actor)
        if args.report_out:
            args.report_out.parent.mkdir(parents=True, exist_ok=True)
            args.report_out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["ok"] else 1
    except (ConfigError, ValueError, OSError, TypeError, KeyError) as error:
        print(json.dumps({"ok": False, "error": str(error) if isinstance(error, ConfigError) else "invalid metadata input"}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
