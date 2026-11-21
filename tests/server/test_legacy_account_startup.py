"""Native full-app startup contract, isolated from filesystem-only fixtures."""

import os
import subprocess
import sys
from pathlib import Path


def test_native_app_lifespan_with_persisted_legacy_account():
    probe = Path(__file__).with_name("legacy_startup_probe.py")
    result = subprocess.run(
        [sys.executable, str(probe)],
        env=dict(os.environ),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert (
        "PASS full application lifespan native RAGFS FileStore api_key real reconciliation"
        in result.stdout
    )
