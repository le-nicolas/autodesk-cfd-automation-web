from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any


def run_cfd_script(
    *,
    cfd_executable: str,
    script_path: Path,
    env_overrides: dict[str, str] | None = None,
    timeout_seconds: int = 3600,
    workdir: Path | None = None,
) -> dict[str, Any]:
    cmd = [cfd_executable, "-script", str(script_path), "-nolog"]
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)

    try:
        completed = subprocess.run(
            cmd,
            cwd=str(workdir) if workdir else None,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        timed_out = False
    except subprocess.TimeoutExpired as ex:
        completed = ex
        timed_out = True

    log_path = Path(f"{script_path}.log")
    log_text = ""
    if log_path.exists():
        try:
            log_text = log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            log_text = ""

    if timed_out:
        return {
            "ok": False,
            "timed_out": True,
            "returncode": None,
            "stdout": completed.stdout or "",
            "stderr": completed.stderr or "",
            "log_path": str(log_path),
            "log_text": log_text,
            "command": cmd,
        }

    return {
        "ok": completed.returncode == 0,
        "timed_out": False,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "log_path": str(log_path),
        "log_text": log_text,
        "command": cmd,
    }
