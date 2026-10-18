"""Retained package-profile bytes; never a claim that fixture code was built.

The installed verifier loads a fresh module, so patching an already-imported
module's profile would select different subjects at the two boundaries. These
tests instead use the exact Cargo inputs selected by the production verifier.
"""
import hashlib
import json
from pathlib import Path

import package_v2


def selected_build_files():
    expected = {"Cargo.toml": package_v2.MANIFEST_SHA, "Cargo.lock": package_v2.LOCK_SHA}
    return _read_profile("v2-package-profile-20f1acdf", expected)


def historical_build_files():
    expected = {"Cargo.toml": "221f255d5f2317caf59e4c7ac3df9929dbe248852a33752b78376a97df80ff83",
                "Cargo.lock": "aeac8e626ea3e28dd88700353075859e12d3e5624e5ca23f6451e3a29ee1cf7b"}
    return _read_profile("task125-package-profile", expected)


def _read_profile(directory, expected):
    root = Path(__file__).with_name("fixtures") / directory
    provenance = json.loads((root / "provenance.json").read_text())
    if set(provenance["files"]) != set(expected):
        raise ValueError("selected package fixture membership differs")
    result = {}
    for name, digest in expected.items():
        item = provenance["files"][name]
        if item["file"] != name + ".fixture":
            raise ValueError("selected package fixture path differs")
        raw = (root / item["file"]).read_bytes()
        if (item["sha256"] != digest or hashlib.sha256(raw).hexdigest() != digest
                or len(raw) != item["bytes"]):
            raise ValueError("selected package fixture source/profile differs")
        result[name] = raw
    return result
