from __future__ import annotations

import os
import threading
import time
from pathlib import Path
import subprocess
from typing import Any, Callable


DriverEventFn = Callable[[dict[str, Any]], None]


def _emit(callback: DriverEventFn | None, **event: Any) -> None:
    if callback:
        callback(event)


def _read_stream_lines(
    stream,
    *,
    source: str,
    sink: list[str],
    callback: DriverEventFn | None,
) -> None:
    try:
        for line in iter(stream.readline, ""):
            if line == "":
                break
            sink.append(line)
            _emit(callback, type="log_line", source=source, line=line.rstrip("\r\n"))
    finally:
        try:
            stream.close()
        except Exception:
            pass


def _discover_log_files(roots: list[Path]) -> list[Path]:
    suffixes = {".log", ".txt", ".meshlog"}
    found: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        if not root.exists():
            continue
        for walk_root, _, files in os.walk(root):
            base = Path(walk_root)
            for file_name in files:
                file_path = base / file_name
                name_lower = file_name.lower()
                suffix = file_path.suffix.lower()
                if suffix not in suffixes and ".meshlog" not in name_lower:
                    continue
                key = str(file_path).lower()
                if key in seen:
                    continue
                seen.add(key)
                found.append(file_path)
    return found


def run_cfd_script(
    *,
    cfd_executable: str,
    script_path: Path,
    env_overrides: dict[str, str] | None = None,
    timeout_seconds: int = 3600,
    workdir: Path | None = None,
    on_event: DriverEventFn | None = None,
    log_watch_roots: list[Path] | None = None,
    poll_interval_seconds: float = 0.75,
) -> dict[str, Any]:
    cmd = [cfd_executable, "-script", str(script_path), "-nolog"]
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)

    log_path = Path(f"{script_path}.log")
    roots = list(log_watch_roots or [])
    roots.append(log_path.parent)

    watched_offsets: dict[str, int] = {}
    watched_buffers: dict[str, str] = {}
    for existing in _discover_log_files(roots):
        try:
            watched_offsets[str(existing)] = existing.stat().st_size
        except OSError:
            watched_offsets[str(existing)] = 0
        watched_buffers[str(existing)] = ""

    _emit(on_event, type="process_state", state="started", command=cmd)

    proc = subprocess.Popen(
        cmd,
        cwd=str(workdir) if workdir else None,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []
    stdout_thread = threading.Thread(
        target=_read_stream_lines,
        args=(proc.stdout,),
        kwargs={"source": "stdout", "sink": stdout_chunks, "callback": on_event},
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_read_stream_lines,
        args=(proc.stderr,),
        kwargs={"source": "stderr", "sink": stderr_chunks, "callback": on_event},
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    start = time.monotonic()
    timed_out = False

    def tail_logs_once() -> None:
        nonlocal watched_offsets, watched_buffers
        for file_path in _discover_log_files(roots):
            key = str(file_path)
            if key not in watched_offsets:
                watched_offsets[key] = 0
                watched_buffers[key] = ""
            try:
                file_size = file_path.stat().st_size
            except OSError:
                continue
            offset = watched_offsets.get(key, 0)
            if file_size <= offset:
                continue
            try:
                with file_path.open("rb") as fh:
                    fh.seek(offset)
                    chunk = fh.read(file_size - offset)
            except OSError:
                continue
            watched_offsets[key] = file_size
            text = watched_buffers.get(key, "") + chunk.decode("utf-8", errors="replace")
            lines = text.splitlines(keepends=True)
            carry = ""
            if lines and not lines[-1].endswith(("\n", "\r")):
                carry = lines[-1]
                lines = lines[:-1]
            watched_buffers[key] = carry
            for line in lines:
                clean = line.rstrip("\r\n")
                if clean:
                    _emit(
                        on_event,
                        type="log_line",
                        source=f"log:{file_path.name}",
                        line=clean,
                    )

    while proc.poll() is None:
        tail_logs_once()
        elapsed = time.monotonic() - start
        if elapsed > timeout_seconds:
            timed_out = True
            try:
                proc.terminate()
                proc.wait(timeout=8)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            _emit(on_event, type="process_state", state="timeout")
            break
        time.sleep(max(0.15, poll_interval_seconds))

    tail_logs_once()

    try:
        stdout_thread.join(timeout=2)
        stderr_thread.join(timeout=2)
    except Exception:
        pass

    returncode = proc.returncode if not timed_out else None
    log_text = ""
    if log_path.exists():
        try:
            log_text = log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            log_text = ""

    _emit(
        on_event,
        type="process_state",
        state="finished",
        returncode=returncode,
        timed_out=timed_out,
    )

    return {
        "ok": (returncode == 0) and not timed_out,
        "timed_out": timed_out,
        "returncode": returncode,
        "stdout": "".join(stdout_chunks),
        "stderr": "".join(stderr_chunks),
        "log_path": str(log_path),
        "log_text": log_text,
        "command": cmd,
    }
