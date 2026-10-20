"""Run the real release recipe across its build-command environment boundary."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


TOOLING = Path(__file__).resolve().parents[1] / "tooling"
REVISION_VARIABLES = ("BORG_BUILD_REV", "FLEET_BUILD_REV", "TASK_BOARD_COMMIT")


class BuildNativeReleaseTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.repo = self.root / "source"
        (self.repo / "tooling").mkdir(parents=True)
        for name in ("build-native-release", "native_ready.py", "native_release.py",
                     "native_drain.py", "native_metadata.py", "native_profile_switch.py"):
            shutil.copy2(TOOLING / name, self.repo / "tooling" / name)
        (self.repo / "components/fleet").mkdir(parents=True)
        (self.repo / "components/task-board/web").mkdir(parents=True)
        (self.repo / ".gitignore").write_text("components/task-board/web/dist/\n")
        self.git("init", "-q")
        self.git("add", ".")
        self.git("-c", "user.name=Release test", "-c", "user.email=test@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "-qm", "Release fixture")
        self.revision = self.git("rev-parse", "HEAD").strip()

        # Substitute only the expensive compilers. The shell recipe, Git checks,
        # artifact staging and Python manifest/checksum verification are real.
        self.bin = self.root / "bin"
        self.bin.mkdir()
        stub = f"#!{sys.executable}\n" + '''import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
tool = Path(sys.argv[0]).name
record = {"tool": tool, "args": args, "cwd": str(Path.cwd()),
          "revision_env": {key: os.environ.get(key) for key in
                           ("BORG_BUILD_REV", "FLEET_BUILD_REV", "TASK_BOARD_COMMIT")}}
with open(os.environ["BUILD_TEST_LOG"], "a") as log:
    log.write(json.dumps(record) + "\\n")
if tool == "cargo":
    profile = args[args.index("--profile") + 1]
    artifact = "debug" if profile == "dev" else "release"
    target = Path(os.environ["CARGO_TARGET_DIR"]) / artifact / args[args.index("--bin") + 1]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(record))
elif args == ["run", "build"]:
    Path("dist").mkdir(exist_ok=True)
    Path("dist/index.html").write_text("<html>release test</html>")
'''
        for name in ("cargo", "npm"):
            executable = self.bin / name
            executable.write_text(stub)
            executable.chmod(0o755)

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.repo), *args], check=True,
                              text=True, capture_output=True, timeout=30).stdout

    def build(self, components, profile, inherited_revision=None):
        log = self.root / "commands.jsonl"
        output = self.root / "release"
        env = os.environ.copy()
        for variable in REVISION_VARIABLES:
            env.pop(variable, None)
            if inherited_revision is not None:
                env[variable] = inherited_revision
        env.update({"PATH": str(self.bin) + os.pathsep + env.get("PATH", ""),
                    "BORG_BUILD_COMPONENTS": components, "BORG_BUILD_PROFILE": profile,
                    "BORG_BUILD_ROOT": str(self.root / "build"), "BUILD_TEST_LOG": str(log),
                    "PYTHONDONTWRITEBYTECODE": "1"})
        result = subprocess.run(["bash", str(self.repo / "tooling/build-native-release"),
                                 str(output)], cwd=self.repo, env=env, text=True,
                                capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        metadata = json.loads((output / "revision.json").read_text())
        self.assertEqual(metadata["revision"], self.revision)
        self.assertEqual(metadata["components"], components.split(","))
        self.assertEqual(self.git("status", "--porcelain"), "")
        calls = [json.loads(line) for line in log.read_text().splitlines()]
        builds = [call for call in calls if call["tool"] == "cargo"]
        names = {"native": "borg-native", "fleet": "fleet", "board": "task-board"}
        self.assertEqual([call["args"][-1] for call in builds],
                         [names[component] for component in components.split(",")])
        for call in builds:
            with self.subTest(binary=call["args"][-1]):
                self.assertEqual(call["args"], ["build", "--locked", "--profile", profile,
                                                "--bin", call["args"][-1]])
                self.assertEqual(call["revision_env"],
                                 dict.fromkeys(REVISION_VARIABLES, self.revision))
                self.assertEqual(json.loads((output / call["args"][-1]).read_text()), call)
        board = next(call for call in builds if call["args"][-1] == "task-board")
        self.assertEqual(board["cwd"], str(self.repo / "components/task-board"))

    def test_board_only_build_stamps_exact_source_revision(self):
        self.build("board", "dev")

    def test_release_build_overrides_stale_revision_for_every_binary(self):
        self.build("native,fleet,board", "release", inherited_revision="stale-parent-revision")


if __name__ == "__main__":
    unittest.main()
