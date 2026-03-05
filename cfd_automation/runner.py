from __future__ import annotations

import json
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

    def _emit(self, progress: ProgressFn | None, **event: Any) -> None:
        if progress:
            progress(event)

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
        cfd_exe = cfg.get("automation", {}).get("cfd_executable")
        if not cfd_exe:
            raise ValueError("No Autodesk CFD executable configured.")

        timeout_seconds = int((cfg.get("automation", {}).get("timeout_minutes", 120) or 120) * 60)
        max_retries = int(cfg.get("automation", {}).get("max_retries", 1) or 1)
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
            for attempt in range(1, max_retries + 2):
                attempt_dir = ensure_dir(case_dir / f"attempt_{attempt}")
                payload_path = attempt_dir / "payload.json"
                payload = {
                    "run_id": run_id,
                    "attempt": attempt,
                    "case": case,
                    "config": cfg,
                    "case_dir": str(attempt_dir),
                    "project_root": str(self.project_root),
                }
                write_json(payload_path, payload)

                env = {
                    "CFD_AUTOMATION_PAYLOAD": str(payload_path),
                }
                run_info = run_cfd_script(
                    cfd_executable=cfd_exe,
                    script_path=self.case_script,
                    env_overrides=env,
                    timeout_seconds=timeout_seconds,
                    workdir=self.project_root,
                )

                case_result_path = attempt_dir / "case_result.json"
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

                self._emit(
                    progress,
                    type="case_retry",
                    case_id=case_id,
                    attempt=attempt,
                    max_attempts=max_retries + 1,
                    reason=(case_result.get("error") or "case failed"),
                )

            if not last_result:
                last_result = {
                    "case_id": case_id,
                    "success": False,
                    "error": "No case result produced.",
                }
            last_result["attempts"] = int(last_result.get("attempt", max_retries + 1))
            new_results[case_id] = last_result

            state[case_id] = {
                "case_id": case_id,
                "status": "success" if last_result.get("success") else "failed",
                "fingerprint": fingerprint,
                "last_run_id": run_id,
                "result_json": str(Path(last_result.get("payload_path", "")).with_name("case_result.json"))
                if last_result.get("payload_path")
                else "",
                "metrics": last_result.get("metrics", {}),
                "error": last_result.get("error", ""),
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
