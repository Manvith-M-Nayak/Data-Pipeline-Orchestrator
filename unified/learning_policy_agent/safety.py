"""
Phase 5 — Safety & Reversibility.

Every change the Learning Agent makes goes through here first:

  * snapshot() copies the current policies file and/or model files into a
    timestamped version directory BEFORE anything is modified.
  * rollback() restores any previous version.
  * list_versions() lets the dashboard / API show what can be rolled back.

If an update ever performs worse, the RetrainingManager and PolicyEngine
call rollback() automatically — the loop can never spiral without an exit.
"""

from __future__ import annotations

import json
import os
import shutil
import time
from typing import Dict, List, Optional

_AGENT_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_VERSIONS_DIR = os.path.join(_AGENT_DIR, "versions")


class SafetyManager:
    def __init__(self, versions_dir: str = _DEFAULT_VERSIONS_DIR):
        self.versions_dir = versions_dir
        os.makedirs(self.versions_dir, exist_ok=True)

    # ---------------------------------------------------------------- snapshot

    def snapshot(self, paths: List[str], label: str, reason: str = "") -> Optional[str]:
        """
        Copy every existing path (file or directory) into a new version dir.
        Returns the version id, or None if nothing existed to snapshot.
        """
        existing = [p for p in paths if os.path.exists(p)]
        if not existing:
            return None

        version_id = f"{time.strftime('%Y%m%d_%H%M%S')}_{label}"
        vdir = os.path.join(self.versions_dir, version_id)
        os.makedirs(vdir, exist_ok=True)

        manifest = {"version_id": version_id, "label": label, "reason": reason,
                    "created": time.time(), "items": []}

        for p in existing:
            name = os.path.basename(p.rstrip("/"))
            dest = os.path.join(vdir, name)
            if os.path.isdir(p):
                shutil.copytree(p, dest, dirs_exist_ok=True)
            else:
                shutil.copy2(p, dest)
            manifest["items"].append({"original_path": os.path.abspath(p), "name": name})

        with open(os.path.join(vdir, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2)
        return version_id

    # ---------------------------------------------------------------- rollback

    def rollback(self, version_id: str) -> Dict:
        """Restore every item in a snapshot to its original location."""
        vdir = os.path.join(self.versions_dir, version_id)
        manifest_path = os.path.join(vdir, "manifest.json")
        if not os.path.exists(manifest_path):
            raise FileNotFoundError(f"No such version: {version_id}")

        with open(manifest_path) as f:
            manifest = json.load(f)

        restored = []
        for item in manifest["items"]:
            src = os.path.join(vdir, item["name"])
            dst = item["original_path"]
            if os.path.isdir(src):
                if os.path.exists(dst):
                    shutil.rmtree(dst)
                shutil.copytree(src, dst)
            else:
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
            restored.append(dst)

        return {"version_id": version_id, "restored": restored}

    # ------------------------------------------------------------------- list

    def list_versions(self) -> List[Dict]:
        versions = []
        if not os.path.isdir(self.versions_dir):
            return versions
        for name in sorted(os.listdir(self.versions_dir)):
            mp = os.path.join(self.versions_dir, name, "manifest.json")
            if os.path.exists(mp):
                try:
                    with open(mp) as f:
                        m = json.load(f)
                    versions.append({"version_id": m["version_id"],
                                     "label": m.get("label"),
                                     "reason": m.get("reason"),
                                     "created": m.get("created")})
                except (json.JSONDecodeError, KeyError):
                    continue
        return versions