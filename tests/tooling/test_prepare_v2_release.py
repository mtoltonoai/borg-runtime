"""Real loopback HTTP and actual package verification, with an ELF-shaped unit
fixture. No Rust build, actual controller acceptance, protected checker or provider.
"""
import copy
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

import package_v2 as package
import prepare_v2_release as prepare


ROOT = Path(__file__).resolve().parents[2]


class Peer:
    def __init__(self, task):
        self.task = task
        self.requests = []
        self.original = None
        self.mode = "normal"
        peer = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                assert self.path == "/api/operations"
                length = int(self.headers["Content-Length"])
                assert length <= prepare.MAX_BODY
                raw = self.rfile.read(length)
                value = json.loads(raw)
                peer.requests.append((raw, value))
                reply = peer.respond(value)
                if reply is None:
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                if peer.mode == "redirect":
                    self.send_response(302)
                    self.send_header("Location", "http://127.0.0.1:1/must-not-contact")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                body = json.dumps(reply).encode()
                if peer.mode == "duplicate":
                    body = body[:-1] + b',"status":"ok"}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(prepare.MAX_BODY + 1 if peer.mode == "oversized" else len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.origin = "http://127.0.0.1:" + str(self.server.server_port)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def release(self, body):
        return {"id":"release-1", "task_id":body["task_id"], "task_revision":body["expected_task_revision"],
                "accepted_attempt_id":"accepted-attempt", "checkpoint":body["checkpoint"], "artifact":body["artifact"],
                "revision":1, "qualification":dict(prepare.QUALIFICATION)}

    def respond(self, req):
        reply = self.response_body(req)
        if reply is None:
            return None
        reply["observed_controller_revision"] = 20
        reply["commit"] = None
        if req["operation"] == "prepare_release" or (req["operation"] == "get_operation" and reply["status"] == "ok"):
            reply["commit"] = {"operation_id":req["input"]["operation_id"], "controller_revision":10}
        if req["operation"] == "prepare_release":
            if self.mode == "missing_commit":
                reply["commit"] = None
            elif self.mode == "wrong_commit":
                reply["commit"]["operation_id"] = "other"
            elif self.mode == "future_commit":
                reply["commit"]["controller_revision"] = 21
            elif self.mode == "uncommitted_refused":
                reply.update(status="refused", code="capacity", message="queue full", commit=None)
                reply.pop("data", None)
        return reply

    def response_body(self, req):
        if req["operation"] == "get_task":
            return {"request_id":req["request_id"], "status":"ok", "data":{"kind":"task","value":self.task}}
        if req["operation"] == "get_operation":
            if self.mode == "not_found":
                return {"request_id":req["request_id"], "status":"refused", "code":"not_found", "message":"no committed outcome"}
            original = self.original
            intended = {"operation":"prepare_release", "input":original["input"]}
            if self.mode == "wrong_original":
                intended = copy.deepcopy(intended)
                intended["input"]["operation_id"] = "different-operation"
            record = {"id":req["input"]["operation_id"], "task_id":"", "attempt_id":"", "generation":0,
                      "kind":"application_mutation", "intended":intended, "state":"settled",
                      "outcome":{"status":"ok","data":{"kind":"release","value":self.release(original["input"])}}}
            if self.mode == "nested_context":
                record["outcome"]["commit"] = {"operation_id":req["input"]["operation_id"],"controller_revision":11}
                record["outcome"]["observed_controller_revision"] = 99
            return {"request_id":req["request_id"], "status":"ok", "data":{"kind":"operation","value":record}}
        assert req["operation"] == "prepare_release"
        self.original = req
        if self.mode == "lost":
            return None  # Simulated committed mutation whose acknowledgment is lost.
        if self.mode == "refused":
            return {"request_id":req["request_id"], "status":"refused", "code":"stale_revision", "message":"changed"}
        value = self.release(req["input"])
        if self.mode == "authority":
            value["qualification"]["checks"] = "passed"
        if self.mode == "wrong_artifact":
            value["artifact"] = dict(value["artifact"], package_sha256="f"*64)
        if self.mode == "wrong_attempt":
            value["accepted_attempt_id"] = "foreign-attempt"
        if self.mode == "wrong_registration":
            value["checkpoint"] = copy.deepcopy(value["checkpoint"])
            value["checkpoint"]["registration"]["registration_revision"] = "r2"
        reply = {"request_id":req["request_id"], "status":"ok", "data":{"kind":"release", "value":value}}
        if self.mode == "wrong_request":
            reply["request_id"] = "foreign-request"
        return reply


class PrepareReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="borg-prepare-release-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.commit = "a" * 40
        binary = self.root / "fake-elf"
        data = bytearray(128)
        data[:7] = b"\x7fELF\x02\x01\x01"
        data[16:18], data[18:20] = (3).to_bytes(2,"little"), (62).to_bytes(2,"little")
        binary.write_bytes(data)
        binary.chmod(0o755)
        files = {}
        for name, value in {"Cargo.toml":(ROOT/"Cargo.toml").read_bytes(), "Cargo.lock":(ROOT/"Cargo.lock").read_bytes(),
                            "src/main.rs":b"fn main() {}\n", "src/lib.rs":b"// controlled fixture\n"}.items():
            files[name] = {"mode":"100644", "git_blob":hashlib.sha1(b"blob "+str(len(value)).encode()+b"\0"+value).hexdigest(),
                           "size":len(value),"sha256":hashlib.sha256(value).hexdigest()}
        self.manifest = {"format":"borg-v2-release/1", "source":{"commit":self.commit,"tree":"b"*40,"object_format":"sha1","files":files},
                         "profile":package.PROFILE, "recipe":{"packager_sha256":package.digest(ROOT/"tooling/package_v2.py"),
                         "build_helper_sha256":package.BUILDER_SHA, **package.READER_SHA, "environment":package.ENV_PROFILE},
                         "build":{"tools":{"sha256":package.TOOL_SHA,"versions":{"gcc":"13.3.0","gcc_target":"x86_64-linux-gnu",
                         "ar":"GNU ar 2.42","pkg-config":"1.8.1","cargo":"cargo 1.99.0 (fixture)","rustc":"rustc 1.99.0 (fixture)\nhost: "+package.TARGET}},
                         "sqlite":{"identity":{"version":"3.54.0","source_id":package.SQLITE_SOURCE,"threadsafe":1,"api_armor":1,"foreign_keys":1},
                         "source_sha256":package.SQLITE_FILES,"archive_sha256":package.SQLITE_ARCHIVE_SHA,"cflags":package.CFLAGS,"static_library_sha256":"c"*64}},
                         "files":{"bin/borg-runtime":package.elf_binary(binary)}}
        self.package = self.root / "artifact.tar"
        package.package_tar(self.package, binary, self.manifest)
        self.digest = package.digest(self.package)
        cp = {"registration":{"repository_id":"controlled-repository", "registration_revision":"r1"}, "repository":"file:///controlled/checkpoints", "commit":self.commit,"remote_ref":"refs/heads/controlled/checkpoint","operation_id":"checkpoint-op"}
        self.task = {"id":"task-1","status":"succeeded","revision":9,"input":{"definition":{"executor":"coding"},
                     "repository":{"registration":copy.deepcopy(cp["registration"]), "url":"file:///controlled/source",
                                   "start_commit":"b"*40, "checkpoint_remote":cp["repository"]}},
                     "attempt":{"id":"accepted-attempt","task_id":"task-1","stopped":True,"uncertain":False,"phase":"stopped"},
                     "result":{"attempt_id":"accepted-attempt","checkpoint":cp},"selected_checkpoint":cp}
        self.peer = Peer(self.task)
        self.addCleanup(self.peer.close)

    def run_prepare(self, output="prepared"):
        return prepare.execute("prepare", self.peer.origin, self.root/output, self.digest, self.commit,
                               self.package, "task-1", "stable-prepare-operation")

    def follow(self, action, previous="prepared", output="follow"):
        return prepare.execute(action, self.peer.origin, self.root/output, self.digest, self.commit,
                               previous=self.root/previous)

    def test_verified_package_binds_actual_task_and_claimed_candidate(self):
        result = self.run_prepare()
        self.assertEqual(result["status"], "prepared")
        self.assertEqual(result["candidate"]["qualification"], prepare.QUALIFICATION)
        requests = [r for _, r in self.peer.requests]
        self.assertEqual([r["operation"] for r in requests], ["get_task", "prepare_release"])
        body = requests[1]["input"]
        self.assertEqual(set(body), {"operation_id","task_id","expected_task_revision","checkpoint","artifact"})
        self.assertEqual(body["expected_task_revision"], 9)
        self.assertEqual(body["checkpoint"], self.task["result"]["checkpoint"])
        self.assertEqual(body["artifact"]["manifest_sha256"], hashlib.sha256(package.canonical(self.manifest)).hexdigest())
        self.assertEqual(body["artifact"]["binary_sha256"], self.manifest["files"]["bin/borg-runtime"]["sha256"])
        self.assertEqual(body["artifact"]["package_bytes"], self.package.stat().st_size)
        self.assertTrue((self.root/"prepared/prepare-request.json").is_file())
        self.assertTrue((self.root/"prepared/accepted-task.json").is_file())

    def test_current_wire_and_protocol_source_pin_match_selected_rust_source(self):
        protocol = (ROOT / "src/protocol.rs").read_bytes()
        self.assertIn(b"pub const WIRE_REVISION: u32 = 8;", protocol)
        self.assertEqual(prepare.WIRE_REVISION, 8)
        self.assertEqual(hashlib.sha256(protocol).hexdigest(), prepare.PROTOCOL_SOURCE_SHA)
        self.run_prepare()
        self.assertTrue(all(req["protocol_revision"] == 8 for _, req in self.peer.requests))

    def test_exact_registration_and_distinct_destination_are_retained(self):
        result = self.run_prepare()
        source = self.task["input"]["repository"]
        checkpoint = result["candidate"]["checkpoint"]
        self.assertNotEqual(source["url"], source["checkpoint_remote"])
        self.assertEqual(checkpoint["repository"], source["checkpoint_remote"])
        self.assertEqual(checkpoint["registration"], source["registration"])
        self.assertEqual(json.loads((self.root / "prepared/prepare-request.json").read_text())
                         ["input"]["checkpoint"], checkpoint)

    def test_missing_malformed_or_extra_registration_fields_refuse_before_mutation(self):
        original = copy.deepcopy(self.task)
        invalid = [None, {}, {"repository_id":"controlled-repository"},
                   {"repository_id":"controlled-repository", "registration_revision":""},
                   {"repository_id":"bad\nidentity", "registration_revision":"r1"},
                   {"repository_id":"controlled-repository", "registration_revision":"r1", "source_store_key":"private"}]
        for i, value in enumerate(invalid):
            with self.subTest(value=value):
                self.peer.task = copy.deepcopy(original)
                self.peer.task["result"]["checkpoint"]["registration"] = value
                with self.assertRaises(ValueError):
                    self.run_prepare("registration-" + str(i))
        self.peer.task = copy.deepcopy(original)
        del self.peer.task["result"]["checkpoint"]["registration"]
        with self.assertRaisesRegex(ValueError, "checkpoint shape"):
            self.run_prepare("legacy-checkpoint")
        self.assertTrue(all(req["operation"] == "get_task" for _, req in self.peer.requests))

    def test_checkpoint_must_match_accepted_registration_and_checkpoint_destination(self):
        original = copy.deepcopy(self.task)
        for i, mutate in enumerate([
            lambda r:r["registration"].update(repository_id="other-repository"),
            lambda r:r["registration"].update(registration_revision="r2"),
            lambda r:r.update(checkpoint_remote="file:///different/destination"),
        ]):
            with self.subTest(index=i):
                self.peer.task = copy.deepcopy(original)
                mutate(self.peer.task["input"]["repository"])
                with self.assertRaisesRegex(ValueError, "accepted repository"):
                    self.run_prepare("accepted-association-" + str(i))
        self.assertTrue(all(req["operation"] == "get_task" for _, req in self.peer.requests))

    def test_failed_package_verification_performs_no_http(self):
        self.package.write_bytes(b"corrupt")
        with self.assertRaises(ValueError):
            self.run_prepare()
        self.assertEqual(self.peer.requests, [])
        receipt = json.loads((self.root/"prepared/receipt.json").read_text())
        self.assertFalse(receipt["mutation_attempted"])

    def test_bad_task_or_checkpoint_cannot_prepare(self):
        original = copy.deepcopy(self.task)
        for i, mutate in enumerate([
            lambda t:t.update(status="failed"),
            lambda t:t["input"]["definition"].update(executor="native"),
            lambda t:t["attempt"].update(uncertain=True),
            lambda t:t["result"].update(attempt_id="other"),
            lambda t:t["result"]["checkpoint"].update(commit="d"*40),
            lambda t:t.update(selected_checkpoint=None),
        ]):
            self.peer.task = copy.deepcopy(original)
            mutate(self.peer.task)
            with self.assertRaises(ValueError):
                self.run_prepare("bad-task-"+str(i))
        self.assertTrue(all(r[1]["operation"] == "get_task" for r in self.peer.requests))

    def test_lost_reply_lookup_recovers_original_without_mutation_retry(self):
        self.peer.mode = "lost"
        with self.assertRaisesRegex(ValueError, "transport"):
            self.run_prepare()
        old = json.loads((self.root/"prepared/receipt.json").read_text())
        self.assertEqual(old["status"], "uncertain")
        self.assertEqual(len(self.peer.requests), 2)
        original = (self.root/"prepared/prepare-request.json").read_bytes()
        self.peer.mode = "normal"
        result = self.follow("reconcile")
        self.assertEqual(result["status"], "prepared")
        self.assertEqual([r[1]["operation"] for r in self.peer.requests].count("prepare_release"), 1)
        self.assertEqual((self.root/"follow/prepare-request.json").read_bytes(), original)
        self.assertFalse(result["mutation_attempted"])

    def test_explicit_retry_reuses_exact_original_bytes_and_revision(self):
        self.peer.mode = "lost"
        with self.assertRaises(ValueError):
            self.run_prepare()
        original = self.peer.requests[-1][0]
        self.peer.mode = "normal"
        self.peer.task["revision"] = 99
        self.peer.task["input"]["repository"]["registration"]["registration_revision"] = "r2"
        result = self.follow("retry")
        self.assertEqual(result["status"], "prepared")
        self.assertEqual(self.peer.requests[-1][0], original)
        self.assertEqual(result["candidate"]["task_revision"], 9)
        self.assertEqual(result["candidate"]["checkpoint"]["registration"],
                         {"repository_id":"controlled-repository", "registration_revision":"r1"})
        self.assertEqual([r[1]["operation"] for r in self.peer.requests].count("get_task"), 1)

    def test_lookup_missing_or_conflicting_original_never_retries(self):
        self.run_prepare()
        self.peer.mode = "not_found"
        result = self.follow("reconcile")
        self.assertEqual(result["status"], "not_observed")
        self.assertFalse(result["mutation_attempted"])
        self.peer.mode = "wrong_original"
        with self.assertRaisesRegex(ValueError, "differs from original"):
            self.follow("reconcile", output="conflict")
        self.assertEqual([r[1]["operation"] for r in self.peer.requests].count("prepare_release"), 1)

    def test_malicious_candidate_authority_and_identity_remain_uncertain(self):
        for mode in ("authority", "wrong_artifact", "wrong_attempt", "wrong_request", "wrong_registration"):
            self.peer.mode = mode
            with self.assertRaises(ValueError):
                self.run_prepare(mode)
            receipt = json.loads((self.root/mode/"receipt.json").read_text())
            self.assertEqual(receipt["status"], "uncertain")
            self.assertNotIn("candidate", receipt)

    def test_refusal_is_not_success_or_automatic_retry(self):
        self.peer.mode = "refused"
        result = self.run_prepare()
        self.assertEqual(result["status"], "refused")
        self.assertNotIn("candidate", result)
        self.assertEqual(len(self.peer.requests), 2)

    def test_redirect_duplicate_oversized_reply_and_nonloopback_refuse(self):
        for mode in ("redirect", "duplicate", "oversized"):
            self.peer.mode = mode
            with self.assertRaises(ValueError):
                self.run_prepare(mode)
        for value in ("https://127.0.0.1:9", "http://example.invalid", "http://127.0.0.1@192.0.2.1", "http://user:password@127.0.0.1", "http://127.0.0.1:9/path", "http://127.0.0.1:9?token=x"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                prepare.endpoint(value)

    def test_stale_output_original_tamper_and_bad_external_identity_refuse(self):
        self.run_prepare()
        original = (self.root/"prepared/receipt.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.run_prepare()
        self.assertEqual((self.root/"prepared/receipt.json").read_bytes(), original)
        saved = self.root/"prepared/prepare-request.json"
        body = json.loads(saved.read_text())
        body["input"]["expected_task_revision"] = 10
        saved.write_text(json.dumps(body))
        before = len(self.peer.requests)
        with self.assertRaisesRegex(ValueError, "original request"):
            self.follow("retry")
        self.assertEqual(len(self.peer.requests), before)

    def test_proxy_environment_is_not_used(self):
        with patch.dict(os.environ, {"http_proxy":"http://192.0.2.1:9", "HTTP_PROXY":"http://192.0.2.1:9", "ALL_PROXY":"http://192.0.2.1:9", "NO_PROXY":""}):
            result = self.run_prepare()
        self.assertEqual(result["status"], "prepared")

    def test_commit_context_is_required_and_original_identity_checked(self):
        for mode in ("missing_commit", "wrong_commit", "future_commit"):
            with self.subTest(mode=mode):
                self.peer.mode = mode
                with self.assertRaisesRegex(ValueError, "commit"):
                    self.run_prepare(mode)
                receipt = json.loads((self.root/mode/"receipt.json").read_text())
                self.assertTrue(receipt["mutation_attempted"])
                self.assertEqual(receipt["status"], "uncertain")

    def test_lookup_outcome_cannot_override_original_receipt_context(self):
        self.run_prepare()
        self.peer.mode = "nested_context"
        with self.assertRaisesRegex(ValueError, "lookup differs"):
            self.follow("reconcile")
        record = json.loads((self.root/"follow/receipt.json").read_text())
        self.assertNotIn("candidate", record)
        self.assertFalse(record["mutation_attempted"])

    def test_uncommitted_refusal_preserves_unknown_outcome(self):
        self.peer.mode = "uncommitted_refused"
        result = self.run_prepare()
        self.assertEqual(result["status"], "uncertain")
        self.assertNotIn("candidate", result)
        self.assertEqual(len(self.peer.requests), 2)
        self.peer.mode = "normal"
        recovered = self.follow("reconcile")
        self.assertEqual(recovered["status"], "prepared")
        self.assertEqual(self.peer.requests[-1][1]["operation"], "get_operation")

    def test_previous_wire_preparation_is_refused_without_network_or_rewrite(self):
        self.run_prepare()
        saved = self.root / "prepared/prepare-request.json"
        receipt = self.root / "prepared/receipt.json"
        body = json.loads(saved.read_text())
        self.assertEqual(body["protocol_revision"], 8)
        body["protocol_revision"] = 7
        saved.write_bytes(prepare.canonical(body))
        record = json.loads(receipt.read_text())
        record["prepare_request_sha256"] = prepare.sha(saved.read_bytes())
        receipt.write_bytes(prepare.canonical(record))
        original = (saved.read_bytes(), receipt.read_bytes())
        calls = len(self.peer.requests)
        with self.assertRaisesRegex(ValueError, "original request"):
            self.follow("retry")
        self.assertEqual(len(self.peer.requests), calls)
        self.assertEqual((saved.read_bytes(), receipt.read_bytes()), original)


if __name__ == "__main__":
    unittest.main()
