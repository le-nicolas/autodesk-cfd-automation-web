from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
from threading import Lock, Thread
from typing import Any

from flask import Flask, jsonify, request, send_from_directory

from cfd_automation import AutomationRunner, LLMCaseGenerator, LLMMeshAdvisor


PROJECT_ROOT = Path(__file__).resolve().parent
runner = AutomationRunner(PROJECT_ROOT)
API_KEY = os.environ.get("CFD_AUTOMATION_API_KEY", "").strip()

app = Flask(
    __name__,
    static_folder=str(PROJECT_ROOT / "web"),
    static_url_path="/web",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_runtime_url(path_value: str) -> str | None:
    if not path_value:
        return None
    try:
        abs_path = Path(path_value).resolve()
        rel = abs_path.relative_to(runner.runtime_dir.resolve())
        return "/runtime/" + str(rel).replace("\\", "/")
    except Exception:
        return None


def enrich_summary(summary: dict[str, Any]) -> dict[str, Any]:
    if not summary:
        return {}
    out = dict(summary)
    results = dict(out.get("results", {}))
    if results:
        results["master_csv_url"] = to_runtime_url(results.get("master_csv", ""))
        results["ranked_csv_url"] = to_runtime_url(results.get("ranked_csv", ""))
        results["report_md_url"] = to_runtime_url(results.get("report_md", ""))
        results["report_html_url"] = to_runtime_url(results.get("report_html", ""))
        results["chart_urls"] = [to_runtime_url(path) for path in results.get("charts", [])]
        out["results"] = results

    case_results = []
    for case in out.get("case_results", []):
        item = dict(case)
        item["summary_csv_url"] = to_runtime_url(item.get("summary_csv", ""))
        item["metrics_csv_url"] = to_runtime_url(item.get("metrics_csv", ""))
        item["screenshot_urls"] = [to_runtime_url(path) for path in item.get("screenshots", [])]
        item["failure_reason"] = item.get("failure_reason") or item.get("error", "")
        item["failure_type"] = item.get("failure_type", "")
        item["failure_mode"] = item.get("failure_mode", "")
        case_results.append(item)
    out["case_results"] = case_results
    return out


def merge_mesh_suggestion_into_config(config: dict[str, Any], suggestion: dict[str, Any]) -> dict[str, Any]:
    merged = dict(config)
    mesh_cfg = dict(merged.get("mesh", {}) if isinstance(merged.get("mesh", {}), dict) else {})
    default_params = dict(
        mesh_cfg.get("default_params", {})
        if isinstance(mesh_cfg.get("default_params", {}), dict)
        else {}
    )
    quality_gate = dict(
        mesh_cfg.get("quality_gate", {})
        if isinstance(mesh_cfg.get("quality_gate", {}), dict)
        else {}
    )

    for key, value in suggestion.get("mesh_params", {}).items():
        if key == "refinement_zones":
            default_params[key] = value if isinstance(value, list) else []
            continue
        default_params[key] = "" if value is None else value

    for key, value in suggestion.get("quality_gate", {}).items():
        if value is None:
            continue
        quality_gate[key] = value

    mesh_cfg["default_params"] = default_params
    mesh_cfg["quality_gate"] = quality_gate
    merged["mesh"] = mesh_cfg
    return merged


def require_api_key() -> Any | None:
    if not API_KEY:
        return None
    provided = str(request.headers.get("X-API-Key", "")).strip()
    if provided == API_KEY:
        return None
    return (
        jsonify(
            {
                "ok": False,
                "error": "Unauthorized. Provide X-API-Key header.",
            }
        ),
        401,
    )


class RunManager:
    def __init__(self) -> None:
        self._lock = Lock()
        self._state: dict[str, Any] = {
            "running": False,
            "run_id": "",
            "mode": "",
            "started_at": "",
            "finished_at": "",
            "selected_case_count": 0,
            "completed_case_count": 0,
            "current_case": "",
            "logs": [],
            "last_error": "",
            "last_summary": {},
            "recent_failures": [],
        }

    def _append_log(self, line: str) -> None:
        logs = self._state.setdefault("logs", [])
        logs.append(f"[{utc_now_iso()}] {line}")
        if len(logs) > 1200:
            del logs[:-1200]

    def _handle_progress(self, event: dict[str, Any]) -> None:
        event_type = event.get("type", "event")
        with self._lock:
            if event_type == "run_started":
                self._state["run_id"] = event.get("run_id", "")
                self._state["mode"] = event.get("mode", "")
                self._state["selected_case_count"] = int(event.get("selected_cases", 0))
                self._state["completed_case_count"] = 0
                self._state["current_case"] = ""
                self._append_log(
                    f"Run started. mode={self._state['mode']} selected={self._state['selected_case_count']}"
                )
                if bool(event.get("dry_run")):
                    self._append_log("Info: dry-run mode is enabled (CFD execution simulated).")
                if event.get("study_path"):
                    self._append_log(f"Study: {event.get('study_path')}")
                if not bool(event.get("solve_enabled", False)):
                    self._append_log(
                        "Warning: solve.enabled is false. Outputs will use existing/cached results."
                    )
            elif event_type == "case_started":
                self._state["current_case"] = event.get("case_id", "")
                self._append_log(
                    f"Case started ({event.get('index', '?')}/{event.get('total', '?')}): "
                    f"{self._state['current_case']}"
                )
            elif event_type == "case_success":
                self._state["completed_case_count"] += 1
                self._append_log(
                    f"Case succeeded: {event.get('case_id', '')} (attempt {event.get('attempt', 1)})"
                )
            elif event_type == "case_retry":
                failure_type = str(event.get("failure_type", "")).strip()
                failure_mode = str(event.get("failure_mode", "")).strip()
                mesh_adjustment = event.get("mesh_adjustment", {})
                self._append_log(
                    f"Case retry: {event.get('case_id', '')} "
                    f"(attempt {event.get('attempt', 1)}/{event.get('max_attempts', 1)}) "
                    f"type={failure_type or '-'} mode={failure_mode or '-'} "
                    f"mesh_adjustment={mesh_adjustment if mesh_adjustment else '{}'} "
                    f"reason={event.get('reason', '')}"
                )
            elif event_type == "case_failed":
                fail = {
                    "case_id": event.get("case_id", ""),
                    "attempt": event.get("attempt", 0),
                    "reason": event.get("reason", ""),
                    "failure_type": event.get("failure_type", ""),
                    "failure_mode": event.get("failure_mode", ""),
                }
                failures = self._state.setdefault("recent_failures", [])
                failures.append(fail)
                if len(failures) > 50:
                    del failures[:-50]
                self._append_log(
                    f"Case failed: {fail['case_id']} (attempt {fail['attempt']}), "
                    f"type={fail['failure_type'] or 'unknown'}, reason={fail['reason']}"
                )
            elif event_type == "case_log":
                source = event.get("source", "driver")
                case_id = event.get("case_id", "")
                attempt = event.get("attempt", "")
                line = event.get("line", "")
                self._append_log(f"[{case_id}][attempt {attempt}][{source}] {line}")
            elif event_type == "run_finished":
                self._state["last_summary"] = enrich_summary(event.get("summary", {}))
                self._append_log("Run finished.")
            else:
                self._append_log(str(event))

    def start(self, mode: str) -> tuple[bool, str]:
        with self._lock:
            if self._state.get("running"):
                return False, "A run is already in progress."
            self._state["running"] = True
            self._state["started_at"] = utc_now_iso()
            self._state["finished_at"] = ""
            self._state["last_error"] = ""
            self._state["logs"] = []
            self._state["last_summary"] = {}
            self._state["recent_failures"] = []

        def worker() -> None:
            try:
                summary = runner.run(mode=mode, progress=self._handle_progress)
                with self._lock:
                    self._state["last_summary"] = enrich_summary(summary)
            except Exception as ex:
                with self._lock:
                    self._state["last_error"] = str(ex)
                    self._append_log(f"Run crashed: {ex}")
            finally:
                with self._lock:
                    self._state["running"] = False
                    self._state["finished_at"] = utc_now_iso()
                    self._state["current_case"] = ""

        Thread(target=worker, daemon=True).start()
        return True, "Run started."

    def get(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state)


run_manager = RunManager()


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.get("/api/config")
def api_get_config():
    return jsonify(runner.get_config())


@app.post("/api/config")
def api_save_config():
    auth = require_api_key()
    if auth:
        return auth
    payload = request.get_json(force=True, silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "Request body must be a JSON object."}), 400
    saved = runner.save_config(payload)
    return jsonify({"ok": True, "config": saved})


@app.get("/api/cases")
def api_get_cases():
    return jsonify({"csv": runner.get_cases_csv(), "rows": runner.get_cases()})


@app.post("/api/cases")
def api_save_cases():
    auth = require_api_key()
    if auth:
        return auth
    payload = request.get_json(force=True, silent=True) or {}
    csv_text = payload.get("csv", "")
    if not isinstance(csv_text, str):
        return jsonify({"ok": False, "error": "Field 'csv' must be text."}), 400
    rows = runner.save_cases_csv(csv_text)
    return jsonify({"ok": True, "rows": rows})


@app.post("/api/llm/generate-cases")
def api_llm_generate_cases():
    auth = require_api_key()
    if auth:
        return auth

    payload = request.get_json(force=True, silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "Request body must be a JSON object."}), 400

    prompt = str(payload.get("prompt", "")).strip()
    if not prompt:
        return jsonify({"ok": False, "error": "Field `prompt` is required."}), 400

    apply_changes = bool(payload.get("apply", False))
    max_rows_override = payload.get("max_rows")
    if max_rows_override in ("", None):
        max_rows_override = None
    else:
        try:
            max_rows_override = int(max_rows_override)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "Field `max_rows` must be an integer."}), 400

    cfg = runner.get_config()
    llm_cfg = cfg.get("llm", {}) if isinstance(cfg.get("llm", {}), dict) else {}
    existing_rows = runner.get_cases()

    try:
        generator = LLMCaseGenerator(llm_cfg)
        result = generator.generate(
            prompt=prompt,
            config=cfg,
            existing_rows=existing_rows,
            max_rows_override=max_rows_override,
        )
    except ValueError as ex:
        return jsonify({"ok": False, "error": str(ex)}), 400
    except RuntimeError as ex:
        return jsonify({"ok": False, "error": str(ex)}), 502

    if apply_changes:
        runner.save_cases_csv(result["csv"])

    return jsonify(
        {
            "ok": True,
            "applied": apply_changes,
            "provider": result.get("provider", ""),
            "model": result.get("model", ""),
            "notes": result.get("notes", ""),
            "row_count": result.get("row_count", 0),
            "rows": result.get("rows", []),
            "csv": result.get("csv", ""),
        }
    )


@app.post("/api/llm/suggest-mesh")
def api_llm_suggest_mesh():
    auth = require_api_key()
    if auth:
        return auth

    payload = request.get_json(force=True, silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "Request body must be a JSON object."}), 400

    prompt = str(payload.get("prompt", "")).strip()
    if not prompt:
        prompt = (
            "Suggest robust CFD mesh defaults and mesh quality gates for this project "
            "based on config and case ranges."
        )
    apply_changes = bool(payload.get("apply", False))

    cfg = runner.get_config()
    llm_cfg = cfg.get("llm", {}) if isinstance(cfg.get("llm", {}), dict) else {}
    existing_rows = runner.get_cases()

    try:
        advisor = LLMMeshAdvisor(llm_cfg)
        suggestion = advisor.suggest(prompt=prompt, config=cfg, existing_rows=existing_rows)
    except ValueError as ex:
        return jsonify({"ok": False, "error": str(ex)}), 400
    except RuntimeError as ex:
        return jsonify({"ok": False, "error": str(ex)}), 502

    saved_config = None
    if apply_changes:
        merged = merge_mesh_suggestion_into_config(cfg, suggestion)
        saved_config = runner.save_config(merged)

    return jsonify(
        {
            "ok": True,
            "applied": apply_changes,
            "provider": suggestion.get("provider", ""),
            "model": suggestion.get("model", ""),
            "mesh_params": suggestion.get("mesh_params", {}),
            "quality_gate": suggestion.get("quality_gate", {}),
            "notes": suggestion.get("notes", ""),
            "config": saved_config if saved_config else None,
        }
    )


@app.post("/api/introspect")
def api_introspect():
    auth = require_api_key()
    if auth:
        return auth
    payload = request.get_json(force=True, silent=True) or {}
    study_path = payload.get("study_path")
    result = runner.introspect(study_override=study_path)
    if result.get("data"):
        data_output = dict(result)
        data_output["data_url"] = to_runtime_url(result.get("output_path", ""))
        return jsonify({"ok": True, "result": data_output})
    return jsonify({"ok": False, "result": result}), 500


@app.get("/api/studies")
def api_studies():
    try:
        max_results = int(request.args.get("max_results", "120"))
    except ValueError:
        max_results = 120
    try:
        max_depth = int(request.args.get("max_depth", "5"))
    except ValueError:
        max_depth = 5
    max_results = max(1, min(max_results, 1000))
    max_depth = max(1, min(max_depth, 10))
    studies = runner.discover_studies(max_results=max_results, max_depth=max_depth)
    return jsonify({"ok": True, "count": len(studies), "studies": studies})


@app.post("/api/run")
def api_run():
    auth = require_api_key()
    if auth:
        return auth
    payload = request.get_json(force=True, silent=True) or {}
    mode = str(payload.get("mode", "all")).lower()
    started, message = run_manager.start(mode)
    return jsonify({"ok": started, "message": message})


@app.get("/api/status")
def api_status():
    state = run_manager.get()
    state["auth_required"] = bool(API_KEY)
    return jsonify(state)


@app.get("/api/latest-run")
def api_latest_run():
    summary = enrich_summary(runner.latest_run())
    return jsonify(summary)


@app.get("/runtime/<path:subpath>")
def serve_runtime(subpath: str):
    return send_from_directory(runner.runtime_dir, subpath)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5055, debug=False)
