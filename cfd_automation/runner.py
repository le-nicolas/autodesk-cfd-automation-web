from __future__ import annotations

import json
import os
import re
import shutil
from pathlib import Path
from typing import Any, Callable

from .cfd_driver import run_cfd_script
from .config_io import (
    case_fingerprint,
    cases_to_csv,
    load_cases,
    load_config,
    parse_cases_csv,
    save_cases,
    save_config,
)
from .postprocess import run_postprocess
from .utils import ensure_dir, now_utc_stamp, read_json, write_json

ProgressFn = Callable[[dict[str, Any]], None]


class AutomationRunner:
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.config_path = project_root / "config" / "study_config.yaml"
        self.cases_path = project_root / "config" / "cases.csv"
        self.runtime_dir = ensure_dir(project_root / "runtime")
        self.state_dir = ensure_dir(self.runtime_dir / "state")
        self.runs_dir = ensure_dir(self.runtime_dir / "runs")
        self.introspection_dir = ensure_dir(self.runtime_dir / "introspection")
        self.state_path = self.state_dir / "case_state.json"
        self.latest_run_path = self.runtime_dir / "latest_run.json"
        self.case_script = project_root / "scripts" / "cfd_case_runner.py"
        self.introspect_script = project_root / "scripts" / "cfd_introspect.py"

    def get_config(self) -> dict[str, Any]:
        return load_config(self.config_path)

    def save_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        save_config(self.config_path, payload)
        return self.get_config()

    def get_cases(self) -> list[dict[str, Any]]:
        return load_cases(self.cases_path)

    def get_cases_csv(self) -> str:
        rows = self.get_cases()
        return cases_to_csv(rows)

    def save_cases_csv(self, csv_text: str) -> list[dict[str, Any]]:
        rows = parse_cases_csv(csv_text)
        save_cases(self.cases_path, rows)
        return rows

    def introspect(self, study_override: str | None = None) -> dict[str, Any]:
        cfg = self.get_config()
        study_path = study_override or cfg.get("study", {}).get("template_model")
        if not study_path:
            raise ValueError("No study path configured.")
        study_file = Path(str(study_path))
        if study_file.suffix.lower() != ".cfdst":
            raise ValueError(f"Study path must point to a .cfdst file: {study_path}")
        if not study_file.exists():
            raise ValueError(f"Study file does not exist: {study_path}")

        output_path = (self.introspection_dir / "introspection.json").resolve()
        if output_path.exists():
            output_path.unlink()

        cfd_exe = cfg.get("automation", {}).get("cfd_executable")
        if not cfd_exe:
            raise ValueError("No Autodesk CFD executable configured.")

        env = {
            "CFD_AUTOMATION_STUDY": str(study_path),
            "CFD_AUTOMATION_OUTPUT": str(output_path),
            "CFD_AUTOMATION_DESIGN": str(cfg.get("study", {}).get("design_name", "")),
            "CFD_AUTOMATION_SCENARIO": str(cfg.get("study", {}).get("scenario_name", "")),
        }
        timeout = int((cfg.get("automation", {}).get("timeout_minutes", 30) or 30) * 60)
        run_info = run_cfd_script(
            cfd_executable=cfd_exe,
            script_path=self.introspect_script,
            env_overrides=env,
            timeout_seconds=timeout,
            workdir=self.project_root,
        )
        payload = {
            "run_info": run_info,
            "output_path": str(output_path),
            "data": None,
        }
        if output_path.exists():
            payload["data"] = read_json(output_path, default={})
        return payload

    def _load_state(self) -> dict[str, Any]:
        state = read_json(self.state_path, default={})
        if not isinstance(state, dict):
            return {}
        return state

    def _save_state(self, state: dict[str, Any]) -> None:
        write_json(self.state_path, state)

    @staticmethod
    def _safe_case_id(case_id: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "_", case_id)

    @staticmethod
    def _derive_failure_reason(case_result: dict[str, Any], run_info: dict[str, Any]) -> str:
        if case_result.get("success"):
            return ""
        if str(case_result.get("failure_type", "")).strip().lower() == "bad_mesh":
            mesh_quality = case_result.get("mesh_quality", {})
            if isinstance(mesh_quality, dict):
                checks = mesh_quality.get("failed_checks", [])
                if isinstance(checks, list) and checks:
                    return "Mesh quality gate failed: " + "; ".join(str(item) for item in checks[:4])
            if case_result.get("error"):
                return str(case_result.get("error"))
            return "Mesh quality gate failed."
        if case_result.get("failure_reason"):
            return str(case_result.get("failure_reason"))
        if case_result.get("error"):
            return str(case_result.get("error"))
        if run_info.get("timed_out"):
            return "Timed out while waiting for Autodesk CFD script execution."
        stderr = (run_info.get("stderr") or "").strip()
        if stderr:
            return stderr.splitlines()[-1][:500]
        returncode = run_info.get("returncode")
        if returncode not in (None, 0):
            return f"Autodesk CFD script exited with return code {returncode}."
        log_text = run_info.get("log_text", "") or ""
        if "ERROR in Python script" in log_text:
            lines = [line.strip() for line in log_text.splitlines() if line.strip()]
            if lines:
                return lines[-1][:500]
        return "Case failed for an unspecified reason."

    @staticmethod
    def _classify_failure_type(case_result: dict[str, Any], run_info: dict[str, Any]) -> str:
        if case_result.get("success"):
            return ""
        declared = str(case_result.get("failure_type", "")).strip().lower()
        if declared in {"timeout", "non_zero_exit", "python_exception", "no_results", "bad_mesh"}:
            return declared

        if str(case_result.get("error", "")).strip().lower().find("mesh quality") >= 0:
            return "bad_mesh"
        if run_info.get("timed_out"):
            return "timeout"

        log_text = str(run_info.get("log_text", "") or "")
        if "ERROR in Python script" in log_text or case_result.get("traceback"):
            return "python_exception"

        err_text = str(case_result.get("error", "")).strip().lower()
        if "no results" in err_text:
            return "no_results"

        returncode = run_info.get("returncode")
        if returncode not in (None, 0):
            return "non_zero_exit"

        stderr = str(run_info.get("stderr", "")).strip()
        if stderr:
            return "non_zero_exit"

        return "non_zero_exit"

    @staticmethod
    def _classify_failure_mode(case_result: dict[str, Any], failure_type: str) -> str:
        failure_type = str(failure_type or "").strip().lower()
        if failure_type == "bad_mesh":
            return "mesh_failure"
        if failure_type == "python_exception":
            return "script_failure"

        text = " ".join(
            [
                str(case_result.get("error", "")),
                str(case_result.get("failure_reason", "")),
                str(case_result.get("driver", {}).get("stderr", "")),
            ]
        ).lower()
        divergence_tokens = [
            "diverg",
            "residual",
            "not converg",
            "nan",
            "floating point",
            "matrix singular",
        ]
        if any(token in text for token in divergence_tokens):
            return "solver_divergence"
        return "generic_failure"

    @staticmethod
    def _build_mesh_adjustment(config: dict[str, Any], direction: str) -> dict[str, Any]:
        mesh_retry_cfg = (
            config.get("mesh", {}).get("retry", {})
            if isinstance(config.get("mesh", {}), dict)
            else {}
        )
        if direction == "refine":
            size_scale = float(mesh_retry_cfg.get("refine_size_scale", 0.75) or 0.75)
            inflation_delta = int(mesh_retry_cfg.get("refine_inflation_delta", 1) or 1)
        else:
            size_scale = float(mesh_retry_cfg.get("coarsen_size_scale", 1.35) or 1.35)
            inflation_delta = int(mesh_retry_cfg.get("coarsen_inflation_delta", -1) or -1)
        return {
            "direction": direction,
            "size_scale": size_scale,
            "inflation_layer_delta": inflation_delta,
        }

    def _plan_retry(
        self,
        *,
        failure_type: str,
        failure_mode: str,
        config: dict[str, Any],
        attempt: int,
        max_attempts: int,
        mesh_strategy_index: int,
    ) -> tuple[bool, dict[str, Any] | None, int, str]:
        if attempt >= max_attempts:
            return False, None, mesh_strategy_index, ""

        mesh_cfg = config.get("mesh", {}) if isinstance(config.get("mesh", {}), dict) else {}
        mesh_retry_cfg = mesh_cfg.get("retry", {}) if isinstance(mesh_cfg.get("retry", {}), dict) else {}
        mesh_retry_enabled = bool(mesh_retry_cfg.get("enabled", True))
        strategy = mesh_retry_cfg.get("strategy", ["coarsen", "refine"])
        if not isinstance(strategy, list) or not strategy:
            strategy = ["coarsen", "refine"]
        directions = [str(item).strip().lower() for item in strategy if str(item).strip()]
        if not directions:
            directions = ["coarsen", "refine"]

        if mesh_retry_enabled and failure_mode in {"mesh_failure", "solver_divergence"}:
            direction = directions[mesh_strategy_index % len(directions)]
            if direction not in {"coarsen", "refine"}:
                direction = "coarsen"
            adjustment = self._build_mesh_adjustment(config, direction)
            note = (
                "Mesh-aware retry: "
                + (
                    "mesh quality failure"
                    if failure_mode == "mesh_failure"
                    else "solver divergence detected"
                )
                + f", applying {direction} mesh adjustment."
            )
            return True, adjustment, mesh_strategy_index + 1, note

        if failure_mode == "script_failure":
            return True, None, mesh_strategy_index, "Script failure retry: keeping same mesh settings."

        return True, None, mesh_strategy_index, f"Retrying after failure_type={failure_type}."

    @staticmethod
    def _dry_run_case_result(
        *,
        case: dict[str, Any],
        case_id: str,
        run_id: str,
        attempt: int,
    ) -> dict[str, Any]:
        metrics: dict[str, float] = {}
        for key, value in case.items():
            if key == "case_id":
                continue
            try:
                metrics[key] = float(str(value))
            except Exception:
                continue

        force_fail = str(case.get("force_fail", "")).strip().lower() in {"1", "true", "yes", "on"}
        force_fail_type = str(case.get("force_fail_type", "")).strip().lower()
        if force_fail_type and force_fail_type not in {
            "timeout",
            "non_zero_exit",
            "python_exception",
            "no_results",
            "bad_mesh",
            "solver_divergence",
            "script_failure",
        }:
            force_fail_type = ""
        if force_fail_type:
            force_fail = True
        result = {
            "case_id": case_id,
            "run_id": run_id,
            "attempt": attempt,
            "success": not force_fail,
            "messages": ["Dry-run mode: CFD execution was simulated."],
            "warnings": [],
            "metrics": metrics,
            "screenshots": [],
            "cutplanes": [],
        }
        if force_fail:
            if force_fail_type in {"solver_divergence"}:
                result["failure_type"] = "non_zero_exit"
                result["error"] = "Dry-run forced failure: solver divergence detected."
            elif force_fail_type in {"script_failure"}:
                result["failure_type"] = "python_exception"
                result["error"] = "Dry-run forced failure: script error."
            elif force_fail_type in {"timeout", "non_zero_exit", "python_exception", "no_results", "bad_mesh"}:
                result["failure_type"] = force_fail_type
                if force_fail_type == "bad_mesh":
                    result["error"] = "Dry-run forced failure: mesh quality gate failed."
                    result["mesh_quality"] = {
                        "passed": False,
                        "failed_checks": ["skewness=0.99 exceeds threshold=0.95"],
                        "missing_metrics": [],
                    }
                elif force_fail_type == "no_results":
                    result["error"] = "Dry-run forced failure: no results available for post-processing."
                else:
                    result["error"] = f"Dry-run forced failure: {force_fail_type}."
            else:
                result["failure_type"] = "non_zero_exit"
                result["error"] = "Dry-run forced failure via case column force_fail=true."
        return result

    def _emit(self, progress: ProgressFn | None, **event: Any) -> None:
        if progress:
            progress(event)

    def discover_studies(self, *, max_results: int = 200, max_depth: int = 5) -> list[dict[str, Any]]:
        cfg = self.get_config()
        configured_study = str(cfg.get("study", {}).get("template_model", "")).strip()

        roots: list[Path] = []
        if configured_study:
            candidate = Path(configured_study)
            if candidate.exists():
                roots.append(candidate.parent if candidate.is_file() else candidate)

        home = Path.home()
        roots.extend(
            [
                home / "Downloads",
                home / "Documents",
                home / "Desktop",
                self.project_root,
            ]
        )

        seen_roots: list[Path] = []
        dedup = set()
        for root in roots:
            try:
                resolved = root.resolve()
            except Exception:
                continue
            key = str(resolved).lower()
            if key in dedup or not resolved.exists():
                continue
            dedup.add(key)
            seen_roots.append(resolved)

        found: list[dict[str, Any]] = []
        seen_files: set[str] = set()

        for root in seen_roots:
            if len(found) >= max_results:
                break
            for walk_root, _, files in os.walk(root):
                rel_parts = Path(walk_root).relative_to(root).parts
                if len(rel_parts) > max_depth:
                    continue
                for file_name in files:
                    if not file_name.lower().endswith(".cfdst"):
                        continue
                    full_path = Path(walk_root) / file_name
                    key = str(full_path).lower()
                    if key in seen_files:
                        continue
                    seen_files.add(key)
                    try:
                        stat = full_path.stat()
                        size = int(stat.st_size)
                        modified = int(stat.st_mtime)
                    except OSError:
                        size = 0
                        modified = 0
                    found.append(
                        {
                            "path": str(full_path),
                            "size_bytes": size,
                            "modified_epoch": modified,
                        }
                    )
                    if len(found) >= max_results:
                        break
                if len(found) >= max_results:
                    break

        found.sort(key=lambda item: item.get("modified_epoch", 0), reverse=True)
        return found

    def _select_cases(
        self,
        *,
        mode: str,
        cases: list[dict[str, Any]],
        config: dict[str, Any],
        state: dict[str, Any],
    ) -> list[tuple[dict[str, Any], str]]:
        selected: list[tuple[dict[str, Any], str]] = []
        for case in cases:
            case_id = str(case.get("case_id", "")).strip()
            if not case_id:
                continue
            fingerprint = case_fingerprint(case, config)
            prior = state.get(case_id, {})

            if mode == "all":
                selected.append((case, fingerprint))
                continue
            if mode == "failed":
                if prior.get("status") != "success":
                    selected.append((case, fingerprint))
                continue
            if mode == "changed":
                if prior.get("fingerprint") != fingerprint:
                    selected.append((case, fingerprint))
                continue
            selected.append((case, fingerprint))
        return selected

    def run(self, *, mode: str = "all", progress: ProgressFn | None = None) -> dict[str, Any]:
        mode = mode.lower().strip() if mode else "all"
        if mode not in {"all", "failed", "changed"}:
            mode = "all"

        cfg = self.get_config()
        cases = self.get_cases()
        dry_run = os.environ.get("CFD_AUTOMATION_DRY_RUN", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        cfd_exe = cfg.get("automation", {}).get("cfd_executable")
        study_path = str(cfg.get("study", {}).get("template_model", "")).strip()
        if not dry_run:
            if not cfd_exe:
                raise ValueError("No Autodesk CFD executable configured.")
            if not study_path:
                raise ValueError("No study.template_model configured.")
            study_file = Path(study_path)
            if study_file.suffix.lower() != ".cfdst":
                raise ValueError(f"study.template_model must be a .cfdst file: {study_path}")
            if not study_file.exists():
                raise ValueError(f"Configured study file does not exist: {study_path}")
        solve_enabled = bool(cfg.get("solve", {}).get("enabled", False))

        timeout_seconds = int((cfg.get("automation", {}).get("timeout_minutes", 120) or 120) * 60)
        max_retries = int(cfg.get("automation", {}).get("max_retries", 1) or 1)
        max_attempts = max_retries + 1
        state = self._load_state()

        selected_cases = self._select_cases(mode=mode, cases=cases, config=cfg, state=state)
        run_id = now_utc_stamp()
        run_dir = ensure_dir(self.runs_dir / run_id)
        cases_dir = ensure_dir(run_dir / "cases")

        self._emit(
            progress,
            type="run_started",
            run_id=run_id,
            mode=mode,
            total_cases=len(cases),
            selected_cases=len(selected_cases),
            solve_enabled=solve_enabled,
            study_path=str(cfg.get("study", {}).get("template_model", "")),
            dry_run=dry_run,
        )

        new_results: dict[str, dict[str, Any]] = {}
        for index, (case, fingerprint) in enumerate(selected_cases, start=1):
            case_id = str(case.get("case_id"))
            case_dir = ensure_dir(cases_dir / self._safe_case_id(case_id))
            self._emit(
                progress,
                type="case_started",
                case_id=case_id,
                index=index,
                total=len(selected_cases),
            )

            last_result: dict[str, Any] | None = None
            mesh_strategy_index = 0
            pending_mesh_adjustment: dict[str, Any] | None = None
            for attempt in range(1, max_attempts + 1):
                attempt_dir = ensure_dir(case_dir / f"attempt_{attempt}")
                payload_path = attempt_dir / "payload.json"
                payload = {
                    "run_id": run_id,
                    "attempt": attempt,
                    "case": case,
                    "config": cfg,
                    "case_dir": str(attempt_dir),
                    "project_root": str(self.project_root),
                    "mesh_adjustment": pending_mesh_adjustment or {},
                }
                write_json(payload_path, payload)

                env = {
                    "CFD_AUTOMATION_PAYLOAD": str(payload_path),
                }
                case_result_path = attempt_dir / "case_result.json"
                if dry_run:
                    self._emit(
                        progress,
                        type="case_log",
                        case_id=case_id,
                        attempt=attempt,
                        source="dry-run",
                        line="Simulating CFD run in dry-run mode.",
                    )
                    run_info = {
                        "returncode": 0,
                        "timed_out": False,
                        "stdout": "",
                        "stderr": "",
                        "log_path": "",
                        "log_text": "",
                    }
                    case_result = self._dry_run_case_result(
                        case=case,
                        case_id=case_id,
                        run_id=run_id,
                        attempt=attempt,
                    )
                    forced_failure_type = str(case_result.get("failure_type", "")).strip().lower()
                    if forced_failure_type == "timeout":
                        run_info["timed_out"] = True
                        run_info["returncode"] = None
                    elif forced_failure_type in {"non_zero_exit", "python_exception"}:
                        run_info["returncode"] = 1
                    elif forced_failure_type == "bad_mesh":
                        run_info["stderr"] = "mesh quality gate failed"
                    if forced_failure_type == "python_exception":
                        run_info["log_text"] = "ERROR in Python script"
                else:
                    def driver_event(event: dict[str, Any]) -> None:
                        if event.get("type") == "log_line":
                            line = str(event.get("line", "")).strip()
                            if not line:
                                return
                            self._emit(
                                progress,
                                type="case_log",
                                case_id=case_id,
                                attempt=attempt,
                                source=str(event.get("source", "driver")),
                                line=line[:600],
                            )
                        elif event.get("type") == "process_state":
                            self._emit(
                                progress,
                                type="case_log",
                                case_id=case_id,
                                attempt=attempt,
                                source="driver",
                                line=(
                                    f"process_state={event.get('state')} "
                                    f"returncode={event.get('returncode')} "
                                    f"timed_out={event.get('timed_out')}"
                                ),
                            )

                    run_info = run_cfd_script(
                        cfd_executable=cfd_exe,
                        script_path=self.case_script,
                        env_overrides=env,
                        timeout_seconds=timeout_seconds,
                        workdir=self.project_root,
                        on_event=driver_event,
                        log_watch_roots=[attempt_dir],
                    )

                    case_result = read_json(case_result_path, default={}) or {}
                    if not isinstance(case_result, dict):
                        case_result = {}

                case_result.setdefault("case_id", case_id)
                case_result.setdefault("attempt", attempt)
                case_result.setdefault("run_id", run_id)
                case_result.setdefault("success", False)
                case_result.setdefault("messages", [])
                case_result["driver"] = {
                    "returncode": run_info.get("returncode"),
                    "timed_out": run_info.get("timed_out"),
                    "stdout": run_info.get("stdout", ""),
                    "stderr": run_info.get("stderr", ""),
                    "log_path": run_info.get("log_path", ""),
                }
                case_result["payload_path"] = str(payload_path)
                case_result["failure_type"] = self._classify_failure_type(case_result, run_info)
                case_result["failure_reason"] = self._derive_failure_reason(case_result, run_info)
                case_result["failure_mode"] = self._classify_failure_mode(
                    case_result, case_result.get("failure_type", "")
                )
                write_json(case_result_path, case_result)

                if run_info.get("log_text"):
                    log_copy = attempt_dir / "cfd_script.log"
                    log_copy.write_text(run_info["log_text"], encoding="utf-8", errors="replace")

                last_result = case_result
                if case_result.get("success"):
                    self._emit(
                        progress,
                        type="case_success",
                        case_id=case_id,
                        attempt=attempt,
                    )
                    break

                should_retry, next_mesh_adjustment, mesh_strategy_index, retry_note = self._plan_retry(
                    failure_type=str(case_result.get("failure_type", "")).strip().lower(),
                    failure_mode=str(case_result.get("failure_mode", "")).strip().lower(),
                    config=cfg,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    mesh_strategy_index=mesh_strategy_index,
                )
                if not should_retry:
                    break

                pending_mesh_adjustment = next_mesh_adjustment
                retry_reason = case_result.get("failure_reason") or "case failed"
                if retry_note:
                    retry_reason = f"{retry_reason} | {retry_note}"
                self._emit(
                    progress,
                    type="case_retry",
                    case_id=case_id,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    failure_type=case_result.get("failure_type", ""),
                    failure_mode=case_result.get("failure_mode", ""),
                    mesh_adjustment=pending_mesh_adjustment or {},
                    reason=retry_reason,
                )

            if not last_result:
                last_result = {
                    "case_id": case_id,
                    "success": False,
                    "error": "No case result produced.",
                }
            last_result["attempts"] = int(last_result.get("attempt", max_attempts))
            new_results[case_id] = last_result
            if not last_result.get("success"):
                self._emit(
                    progress,
                    type="case_failed",
                    case_id=case_id,
                    attempt=last_result.get("attempt"),
                    failure_type=last_result.get("failure_type", ""),
                    failure_mode=last_result.get("failure_mode", ""),
                    reason=last_result.get("failure_reason", last_result.get("error", "")),
                )

            state[case_id] = {
                "case_id": case_id,
                "status": "success" if last_result.get("success") else "failed",
                "fingerprint": fingerprint,
                "last_run_id": run_id,
                "result_json": str(Path(last_result.get("payload_path", "")).with_name("case_result.json"))
                if last_result.get("payload_path")
                else "",
                "metrics": last_result.get("metrics", {}),
                "failure_type": last_result.get("failure_type", ""),
                "failure_mode": last_result.get("failure_mode", ""),
                "error": last_result.get("failure_reason", last_result.get("error", "")),
            }

        # Rehydrate unchanged cases from prior state so report still contains all cases.
        all_case_results: list[dict[str, Any]] = []
        for case in cases:
            case_id = str(case.get("case_id"))
            if case_id in new_results:
                all_case_results.append(new_results[case_id])
                continue
            prior = state.get(case_id, {})
            result_path = Path(prior.get("result_json", "")) if prior.get("result_json") else None
            if result_path and result_path.exists():
                prior_result = read_json(result_path, default={}) or {}
                if isinstance(prior_result, dict):
                    prior_result.setdefault("case_id", case_id)
                    all_case_results.append(prior_result)
                    continue
            all_case_results.append(
                {
                    "case_id": case_id,
                    "success": prior.get("status") == "success",
                    "metrics": prior.get("metrics", {}),
                    "failure_type": prior.get("failure_type", ""),
                    "failure_mode": prior.get("failure_mode", ""),
                    "error": prior.get("error", ""),
                    "messages": ["carried from prior run"],
                    "screenshots": [],
                }
            )

        post = run_postprocess(run_dir=run_dir, case_results=all_case_results, config=cfg)

        summary = {
            "run_id": run_id,
            "mode": mode,
            "run_dir": str(run_dir),
            "total_cases": len(cases),
            "selected_case_count": len(selected_cases),
            "successful_cases": sum(1 for r in all_case_results if r.get("success")),
            "failed_cases": sum(1 for r in all_case_results if not r.get("success")),
            "results": {
                "master_csv": str(post.master_csv) if post.master_csv else "",
                "ranked_csv": str(post.ranked_csv) if post.ranked_csv else "",
                "charts": [str(path) for path in post.chart_files],
                "report_md": str(post.report_md) if post.report_md else "",
                "report_html": str(post.report_html) if post.report_html else "",
            },
            "case_results": all_case_results,
            "postprocess": post.summary,
        }

        write_json(run_dir / "run_summary.json", summary)
        write_json(self.latest_run_path, summary)
        self._save_state(state)

        self._emit(
            progress,
            type="run_finished",
            run_id=run_id,
            summary=summary,
        )
        return summary

    def latest_run(self) -> dict[str, Any]:
        return read_json(self.latest_run_path, default={}) or {}

    def clean_runtime(self) -> None:
        if self.runtime_dir.exists():
            shutil.rmtree(self.runtime_dir)
        ensure_dir(self.runtime_dir)
        ensure_dir(self.state_dir)
        ensure_dir(self.runs_dir)
        ensure_dir(self.introspection_dir)
