"""Read-only evidence collector cannot claim unobserved overlap or fake writes."""
import copy
import importlib.util
import json
import io
from pathlib import Path
import unittest
import urllib.parse

spec = importlib.util.spec_from_file_location("native_lifecycle_proof", Path(__file__).resolve().parents[1] / "tooling/native_lifecycle_proof.py")
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)


def result(value):
    return {"content": [{"type": "text", "text": json.dumps(value)}], "structuredContent": value, "isError": False}


class Fixture:
    def __init__(self):
        self.parent = {"id": "parent", "agent_id": "same", "status": "completed", "driver_running": False,
                       "workspace_cwd": "/fleet/parent", "host_id": "one"}
        self.child = dict(self.parent, id="child", workspace_cwd="/fleet/child", host_id="two")
        self.parent_dispatch = {"id": 1, "session_id": "parent", "runtime_url": "http://host-one/api", "request_id": "proof"}
        self.child_dispatch = {"id": 2, "session_id": "child", "runtime_url": "http://host-two/api", "request_id": "mcp:parent:child-intent"}
        receipt = {"session_id": "child", "agent_id": "same", "parent_session_id": "parent", "request_id": "mcp:parent:child-intent"}
        write = {"name": "workspace__workspace_write", "status": "succeeded", "arguments": {"path": proof.MARKER}, "result": result({})}
        self.parent_tools = [copy.deepcopy(write), {"name": "native__session_create", "status": "succeeded",
            "operation_id": "parent:delegate", "arguments": {"request_id": "child-intent"}, "result": result(receipt)}]
        self.child_tools = [copy.deepcopy(write)]
        self.markers = {session["id"]: {"role": session["id"], "session_id": session["id"], "token": "public-token"}
                        for session in (self.parent, self.child)}
        self.reads = []

    def request(self, url, body=None, agent=None):
        if body is not None:
            raise AssertionError("verifier must not send a native/Board mutation")
        self.reads.append(url)
        parsed = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qs(parsed.query)
        if parsed.path == "/api/fleet/dispatches":
            return [self.parent_dispatch] if query.get("request_id") == ["proof"] else [self.child_dispatch]
        return {"/api/sessions/parent": self.parent, "/api/sessions/child": self.child,
                "/api/sessions/parent/tools": self.parent_tools, "/api/sessions/child/tools": self.child_tools}[parsed.path]

    def marker(self, session):
        self.reads.append("workspace_read:" + session["id"])
        return self.markers[session["id"]]


class ProofTests(unittest.TestCase):
    def inspect(self, fixture=None, overlap=True):
        return proof.inspect(fixture or Fixture(), "http://board/api/", "proof", "public-token", overlap)

    def test_exact_chain_and_actual_markers_with_observed_overlap_pass(self):
        fixture = Fixture()
        sample = self.inspect(fixture)
        self.assertEqual(sample["state"], "passed")
        self.assertTrue(all(sample["checks"].values()))
        self.assertEqual(sample["operation_id"], "parent:delegate")
        self.assertEqual(fixture.reads[-2:], ["workspace_read:parent", "workspace_read:child"])
        self.assertEqual(proof.api("http://host/api/", "/sessions/child"), "http://host/api/sessions/child")

    def test_completion_timestamps_do_not_invent_concurrent_driver_evidence(self):
        sample = self.inspect(overlap=False)
        self.assertEqual(sample["state"], "failed")
        self.assertFalse(sample["checks"]["concurrent_drivers_observed"])
        fixture = Fixture()
        fixture.parent.update(status="running", driver_running=True)
        fixture.child.update(status="running", driver_running=True)
        waiting = self.inspect(fixture, overlap=False)
        self.assertEqual(waiting["state"], "waiting")
        self.assertTrue(waiting["overlap_seen"])
        fixture.parent.update(status="completed", driver_running=False)
        fixture.child.update(status="completed", driver_running=False)
        self.assertEqual(self.inspect(fixture, overlap=waiting["overlap_seen"])["state"], "passed")

    def test_wrong_attribution_join_shared_root_or_false_marker_never_pass(self):
        for mutation in (
            lambda f: f.child.update(agent_id="not-the-parent-identity"),
            lambda f: f.child_dispatch.update(request_id="unrelated-child"),
            lambda f: f.child.update(workspace_cwd=f.parent["workspace_cwd"]),
            lambda f: f.markers["parent"].update(role="child"),
            lambda f: f.child_tools.clear(),
        ):
            fixture = Fixture()
            mutation(fixture)
            self.assertEqual(self.inspect(fixture)["state"], "failed")

    def test_unknown_effect_blocks_without_querying_child_or_attempting_replay(self):
        fixture = Fixture()
        fixture.parent_tools[-1].update(status="unknown", result=None)
        sample = self.inspect(fixture)
        self.assertEqual(sample["state"], "blocked")
        self.assertEqual(len(fixture.reads), 3)
        self.assertNotIn("child", sample)

    def test_cross_host_marker_read_uses_explicit_owner_route_and_only_read_tool(self):
        captured = []
        marker = {"role": "child", "session_id": "child", "token": "public-token"}
        class Opener:
            def open(self, request, timeout):
                captured.append(request)
                return io.BytesIO(json.dumps({"result": result({"content": json.dumps(marker)})}).encode())
        client = proof.Client(workspace_origins={"other-host": "http://127.0.0.1:18793"})
        client.opener = Opener()
        actual = client.marker({"id": "child", "agent_id": "same", "host_id": "other-host",
                                "workspace_mcp_url": "http://127.0.0.1:8791/mcp/{session_id}"})
        self.assertEqual(actual, marker)
        self.assertEqual(captured[0].full_url, "http://127.0.0.1:18793/mcp/child")
        self.assertEqual(captured[0].get_header("X-fleet-agent"), "same")
        self.assertEqual(json.loads(captured[0].data)["params"], {"name": "workspace_read", "arguments": {"path": proof.MARKER}})

    def test_text_only_mcp_result_supported_and_failed_result_rejected(self):
        original = result({"session_id": "child"})
        original.pop("structuredContent")
        self.assertEqual(proof.content(original), {"session_id": "child"})
        original["isError"] = True
        with self.assertRaises(ValueError):
            proof.content(original)


if __name__ == "__main__":
    unittest.main()
