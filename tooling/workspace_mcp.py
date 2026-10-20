#!/usr/bin/env python3
"""Small stateless workspace MCP endpoint, using only the standard library.

Run: python3 tooling/workspace_mcp.py --root /path/to/workspace --port 8766
POST /mcp uses JSON responses; notifications receive 202, GET receives 405.
File tools reject symlinks and traversal. Shell commands are TRUSTED development
commands with cwd=root, not a security sandbox. Do not expose this to untrusted
callers. Effect ownership/recovery belongs to the calling runtime, not this server.
"""
from __future__ import annotations

import argparse
import codecs
import hmac
import json
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

PROTOCOLS = ("2024-11-05", "2025-03-26", "2025-06-18")
MAX_BYTES = 262144
MAX_BODY = 2 * 1024 * 1024
MAX_SECONDS = 300


def schema(properties, required=()):
    return {"type": "object", "properties": properties,
            "required": list(required), "additionalProperties": False}


TOOLS = [
    {"name": "workspace_list", "description": "List immediate workspace entries without following symlinks.",
     "inputSchema": schema({"path": {"type": "string", "default": "."},
                            "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 200}})},
    {"name": "workspace_read", "description": "Read bounded UTF-8 text from a regular workspace file; inspect truncated before editing.",
     "inputSchema": schema({"path": {"type": "string"},
                            "max_bytes": {"type": "integer", "minimum": 1, "maximum": MAX_BYTES, "default": 65536}},
                           ("path",))},
    {"name": "workspace_write", "description": "Write UTF-8 text in the workspace, creating parent directories. No symlinks.",
     "inputSchema": schema({"path": {"type": "string"}, "content": {"type": "string"},
                            "append": {"type": "boolean", "default": False}}, ("path", "content"))},
    {"name": "workspace_run", "description": "Run a trusted /bin/sh command with cwd=workspace, no stdin, bounded combined output and timeout. Remaining process-group children are killed on return; use operator setup for persistent services. This is not a sandbox.",
     "inputSchema": schema({"command": {"type": "string"},
                            "timeout_seconds": {"type": "number", "exclusiveMinimum": 0, "maximum": MAX_SECONDS, "default": 30},
                            "max_output_bytes": {"type": "integer", "minimum": 1, "maximum": MAX_BYTES, "default": 65536}},
                           ("command",))},
]


class ToolFailure(Exception):
    pass


def bounded_int(value, maximum, name):
    if type(value) is not int or not 1 <= value <= maximum:
        raise ToolFailure(f"{name} must be an integer from 1 to {maximum}")
    return value


class Workspace:
    def __init__(self, root):
        self.root = Path(root).resolve(strict=True)
        if not self.root.is_dir():
            raise ValueError("workspace root must be a directory")

    def parts(self, path):
        if not isinstance(path, str) or "\0" in path or Path(path).is_absolute():
            raise ToolFailure("path must be relative to the workspace")
        parts = Path(path).parts
        if ".." in parts:
            raise ToolFailure("parent traversal is not allowed")
        return parts

    def directory(self, parts, create=False):
        """Walk directory descriptors, refusing symlinks even during replacement."""
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        fd = os.open(self.root, flags)
        try:
            for part in parts:
                if create:
                    try:
                        os.mkdir(part, dir_fd=fd)
                    except FileExistsError:
                        pass
                child = os.open(part, flags, dir_fd=fd)
                os.close(fd)
                fd = child
            return fd
        except BaseException:
            os.close(fd)
            raise

    def file(self, path, write=False, append=False):
        parts = self.parts(path)
        if not parts:
            raise ToolFailure("path must name a file")
        parent = self.directory(parts[:-1], create=write)
        try:
            flags = os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
            flags |= (os.O_WRONLY | os.O_CREAT) if write else os.O_RDONLY
            if append:
                flags |= os.O_APPEND
            fd = os.open(parts[-1], flags, 0o666, dir_fd=parent)
        finally:
            os.close(parent)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            os.close(fd)
            raise ToolFailure("path must be a regular file")
        return fd

    def list(self, path=".", limit=200):
        limit = bounded_int(limit, 1000, "limit")
        fd = self.directory(self.parts(path))
        entries = []
        truncated = False
        try:
            with os.scandir(fd) as iterator:
                for entry in iterator:
                    if len(entries) == limit:
                        truncated = True
                        break
                    info = entry.stat(follow_symlinks=False)
                    kind = ("symlink" if stat.S_ISLNK(info.st_mode) else
                            "directory" if stat.S_ISDIR(info.st_mode) else
                            "file" if stat.S_ISREG(info.st_mode) else "other")
                    entries.append({"name": entry.name, "kind": kind, "bytes": info.st_size})
        finally:
            os.close(fd)
        return {"path": path, "entries": sorted(entries, key=lambda e: e["name"]), "truncated": truncated}

    def read(self, path, max_bytes=65536):
        maximum = bounded_int(max_bytes, MAX_BYTES, "max_bytes")
        with os.fdopen(self.file(path), "rb") as stream:
            data = stream.read(maximum + 1)
        truncated = len(data) > maximum
        decoder = codecs.getincrementaldecoder("utf-8")()
        content = decoder.decode(data[:maximum], final=not truncated)
        return {"path": path, "content": content, "bytes_returned": len(content.encode()),
                "truncated": truncated}

    def write(self, path, content, append=False):
        if not isinstance(content, str) or type(append) is not bool:
            raise ToolFailure("content must be text and append must be boolean")
        data = content.encode("utf-8")
        if len(data) > MAX_BYTES:
            raise ToolFailure(f"content exceeds {MAX_BYTES} UTF-8 bytes")
        with os.fdopen(self.file(path, write=True, append=append), "wb") as stream:
            if not append:
                stream.truncate(0)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        return {"path": path, "bytes_written": len(data), "append": append}

    def run(self, command, timeout_seconds=30, max_output_bytes=65536):
        if not isinstance(command, str) or not command or "\0" in command or len(command.encode()) > 65536:
            raise ToolFailure("command must be nonempty text of at most 65536 UTF-8 bytes")
        if (type(timeout_seconds) not in (int, float)
                or not 0 < timeout_seconds <= MAX_SECONDS):
            raise ToolFailure(f"timeout_seconds must be greater than zero and at most {MAX_SECONDS}")
        maximum = bounded_int(max_output_bytes, MAX_BYTES, "max_output_bytes")
        started = time.monotonic()
        deadline = started + timeout_seconds
        output = bytearray()
        total = 0
        timed_out = False
        process = subprocess.Popen(
            ["/bin/sh", "-c", command], cwd=self.root, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
        def exited():
            # Observe without reaping: the group leader's PID stays reserved
            # until cleanup, so killpg cannot target a newly reused group id.
            return os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while selector.get_map() or not exited():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        timed_out = True
                        break
                    for key, _ in selector.select(min(remaining, 0.05)):
                        chunk = os.read(key.fd, 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                        else:
                            total += len(chunk)
                            output.extend(chunk[:max(0, maximum - len(output))])
        finally:
            # Clean up even redirected background children after normal exit.
            # The unreaped leader still reserves this exact owned group id.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            process.stdout.close()
        return {"exit_code": process.returncode, "timed_out": timed_out,
                "output": output.decode("utf-8", errors="replace"),
                "output_bytes_seen": total, "truncated": total > len(output),
                "seconds": round(time.monotonic() - started, 6)}

    def call(self, name, arguments):
        tool = next((t for t in TOOLS if t["name"] == name), None)
        if tool is None:
            raise ToolFailure("unknown workspace tool")
        spec = tool["inputSchema"]
        if (not isinstance(arguments, dict) or set(arguments) - set(spec["properties"])
                or set(spec["required"]) - set(arguments)):
            raise ToolFailure("invalid tool arguments")
        return getattr(self, name.removeprefix("workspace_"))(**arguments)


def rpc_error(request_id, code, message):
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def dispatch(workspace, message):
    if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
        return rpc_error(None, -32600, "Invalid Request")
    method = message.get("method")
    if not isinstance(method, str):
        return rpc_error(message.get("id"), -32600, "Invalid Request")
    if "id" not in message:
        # Notifications never launch tools or other effects.
        return None if method.startswith("notifications/") else rpc_error(None, -32600, "Request id required")
    request_id = message["id"]
    if type(request_id) not in (str, int):
        return rpc_error(None, -32600, "Invalid request id")
    params = message.get("params", {})
    if not isinstance(params, dict):
        return rpc_error(request_id, -32602, "Invalid params")
    if method == "initialize":
        requested = params.get("protocolVersion")
        version = requested if requested in PROTOCOLS else PROTOCOLS[-1]
        result = {"protocolVersion": version, "capabilities": {"tools": {"listChanged": False}},
                  "serverInfo": {"name": "borg-workspace", "version": "0.1.0"},
                  "instructions": "File paths are relative; shell commands are trusted and run in the workspace, not a sandbox."}
    elif method == "ping":
        result = {}
    elif method == "tools/list":
        if params.get("cursor") is not None:
            return rpc_error(request_id, -32602, "No additional tool pages")
        result = {"tools": TOOLS}
    elif method == "tools/call":
        if not isinstance(params.get("name"), str):
            return rpc_error(request_id, -32602, "Tool name required")
        try:
            value = workspace.call(params["name"], params.get("arguments", {}))
            failed = params["name"] == "workspace_run" and (value["timed_out"] or value["exit_code"] != 0)
        except (ToolFailure, OSError, UnicodeError, ValueError) as exc:
            value, failed = {"error": str(exc)}, True
        result = {"content": [{"type": "text", "text": json.dumps(value, ensure_ascii=False)}],
                  "isError": bool(failed)}
    else:
        return rpc_error(request_id, -32601, "Method not found")
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


class WorkspaceServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, workspace, bearer_token=None):
        self.workspace = workspace
        self.bearer_token = bearer_token
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        self.connection.settimeout(15)

    def log_message(self, format, *args):
        pass  # Never log request bodies, commands or authorization headers.

    def reply(self, status, value=None):
        data = b"" if value is None else json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        if value is not None:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        if data:
            self.wfile.write(data)

    def allowed(self):
        if self.path != "/mcp":
            self.reply(404)
            return False
        token = self.server.bearer_token
        if token is not None and not hmac.compare_digest(
                self.headers.get("Authorization", "").encode("utf-8"),
                ("Bearer " + token).encode("utf-8")):
            self.reply(401)
            return False
        host = self.headers.get("Host", "")
        try:
            parsed = urlsplit("//" + host)
            hostname = parsed.hostname
        except ValueError:
            self.reply(400)
            return False
        if self.server.server_address[0].startswith("127.") and hostname not in ("localhost", "127.0.0.1"):
            self.reply(403)
            return False
        origin = self.headers.get("Origin")
        if origin is not None and origin != "http://" + host:
            self.reply(403)
            return False
        if self.headers.get("MCP-Protocol-Version", PROTOCOLS[-1]) not in PROTOCOLS:
            self.reply(400)
            return False
        return True

    def do_GET(self):
        if self.allowed():
            self.reply(405)

    do_DELETE = do_GET

    def do_POST(self):
        if not self.allowed():
            return
        if self.headers.get("Content-Type", "").split(";")[0].strip() != "application/json":
            self.reply(415)
            return
        if self.headers.get("Transfer-Encoding") is not None:
            self.reply(400)
            return
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self.reply(411)
            return
        if not 0 < length <= MAX_BODY:
            self.reply(413)
            return
        try:
            raw = self.rfile.read(length)
            if len(raw) != length:
                self.reply(400)
                return
            message = json.loads(raw, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
        except (ValueError, UnicodeError):
            self.reply(400, rpc_error(None, -32700, "Parse error"))
            return
        if isinstance(message, list):
            if not 1 <= len(message) <= 16:
                self.reply(400, rpc_error(None, -32600, "Batch must contain 1 to 16 messages"))
                return
            result = [r for item in message if (r := dispatch(self.server.workspace, item)) is not None]
            self.reply(200 if result else 202, result or None)
        else:
            result = dispatch(self.server.workspace, message)
            self.reply(200 if result is not None else 202, result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--bearer-token-env", help="Name of environment variable containing an optional bearer token")
    args = parser.parse_args()
    token = os.environ.get(args.bearer_token_env) if args.bearer_token_env else None
    if args.bearer_token_env and not token:
        parser.error("configured bearer token environment variable is empty")
    if args.host not in ("127.0.0.1", "localhost") and not token:
        parser.error("non-loopback binding requires --bearer-token-env")
    with WorkspaceServer((args.host, args.port), Workspace(args.root), token) as server:
        print(json.dumps({"listening": f"http://{args.host}:{server.server_port}/mcp", "root": str(server.workspace.root)}), flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
