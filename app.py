from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Callable

from flask import Flask, jsonify, request, send_from_directory

from cfd_automation import (
    AutomationRunner,
    GenerativeDesignLoop,
    LLMCaseGenerator,
    LLMMeshAdvisor,
    SurrogateEngine,
)


PROJECT_ROOT = Path(__file__).resolve().parent
runner = AutomationRunner(PROJECT_ROOT)
design_loop_engine = GenerativeDesignLoop(runner)
surrogate_engine = SurrogateEngine(PROJECT_ROOT, runner)
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
                incoming_mode = str(event.get("mode", "")).strip()
                current_mode = str(self._state.get("mode", "")).strip()
                if current_mode == "validate" and incoming_mode == "all":
                    pass
                else:
                    self._state["mode"] = incoming_mode
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
            elif event_type == "run_warning":
                self._append_log(f"Run warning: {event.get('message', '')}")
            else:
                self._append_log(str(event))

    def start(
        self,
        mode: str,
        task: Callable[[Callable[[dict[str, Any]], None]], dict[str, Any]] | None = None,
    ) -> tuple[bool, str]:
        with self._lock:
            if self._state.get("running"):
                return False, "A run is already in progress."
            self._state["running"] = True
            self._state["mode"] = mode
            self._state["started_at"] = utc_now_iso()
            self._state["finished_at"] = ""
            self._state["last_error"] = ""
            self._state["logs"] = []
            self._state["last_summary"] = {}
            self._state["recent_failures"] = []

        def worker() -> None:
            try:
                if task is not None:
                    summary = task(self._handle_progress)
                else:
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


class DesignLoopManager:
    def __init__(self) -> None:
        self._lock = Lock()
        self._state: dict[str, Any] = {
            "running": False,
            "status": "idle",
            "loop_id": "",
            "started_at": "",
            "finished_at": "",
            "current_batch": 0,
            "max_batches": 0,
            "completed_batches": 0,
            "logs": [],
            "last_error": "",
            "last_summary": {},
            "stop_requested": False,
        }

    def _append_log(self, line: str) -> None:
        logs = self._state.setdefault("logs", [])
        logs.append(f"[{utc_now_iso()}] {line}")
        if len(logs) > 2000:
            del logs[:-2000]

    def _handle_progress(self, event: dict[str, Any]) -> None:
        event_type = str(event.get("type", "event"))
        with self._lock:
            if event_type == "loop_started":
                self._state["loop_id"] = event.get("loop_id", "")
                self._state["status"] = "running"
                self._state["current_batch"] = 0
                self._state["completed_batches"] = 0
                self._state["max_batches"] = int(event.get("max_batches", 0))
                self._append_log(
                    "Design loop started. "
                    f"objective={event.get('objective_alias')} goal={event.get('objective_goal')} "
                    f"batch_size={event.get('batch_size')} max_batches={self._state['max_batches']}"
                )
            elif event_type == "loop_batch_started":
                batch_index = int(event.get("batch_index", 0))
                self._state["current_batch"] = batch_index
                self._append_log(
                    f"Batch {batch_index} started with {event.get('batch_size', 0)} proposed case(s)."
                )
            elif event_type == "loop_batch_finished":
                batch_index = int(event.get("batch_index", 0))
                self._state["completed_batches"] = max(self._state["completed_batches"], batch_index)
                best_case = event.get("best_case", {}) if isinstance(event.get("best_case", {}), dict) else {}
                narration = event.get("narration", {}) if isinstance(event.get("narration", {}), dict) else {}
                self._append_log(
                    f"Batch {batch_index} finished. best_case={best_case.get('case_id', '-')}, "
                    f"score={best_case.get('score', '-')}, objective={best_case.get('objective_value', '-')}"
                )
                if narration.get("text"):
                    self._append_log(f"LLM insight: {str(narration.get('text'))[:900]}")
            elif event_type == "loop_run_event":
                nested = event.get("event", {}) if isinstance(event.get("event", {}), dict) else {}
                nested_type = str(nested.get("type", ""))
                if nested_type == "case_failed":
                    self._append_log(
                        "Case failed in loop batch "
                        f"{event.get('batch_index')}: {nested.get('case_id')} "
                        f"type={nested.get('failure_type', '')} reason={nested.get('reason', '')}"
                    )
                elif nested_type == "case_retry":
                    self._append_log(
                        "Case retry in loop batch "
                        f"{event.get('batch_index')}: {nested.get('case_id')} "
                        f"mode={nested.get('failure_mode', '')} reason={nested.get('reason', '')}"
                    )
            elif event_type == "loop_stopped":
                self._state["status"] = "stopping"
                self._append_log(f"Stop requested. Halting before batch {event.get('batch_index')}")
            elif event_type == "loop_finished":
                summary = event.get("summary", {}) if isinstance(event.get("summary", {}), dict) else {}
                self._state["last_summary"] = summary
                self._state["status"] = summary.get("status", "finished")
                best_case = summary.get("best_case", {}) if isinstance(summary.get("best_case", {}), dict) else {}
                self._append_log(
                    f"Design loop finished with status={self._state['status']}. "
                    f"best_case={best_case.get('case_id', '-')}, score={best_case.get('score', '-')}"
                )
            else:
                self._append_log(str(event))

    def start(self, payload: dict[str, Any]) -> tuple[bool, str]:
        with self._lock:
            if self._state.get("running"):
                return False, "A design loop is already in progress."
            self._state["running"] = True
            self._state["status"] = "starting"
            self._state["started_at"] = utc_now_iso()
            self._state["finished_at"] = ""
            self._state["current_batch"] = 0
            self._state["completed_batches"] = 0
            self._state["logs"] = []
            self._state["last_error"] = ""
            self._state["last_summary"] = {}
            self._state["stop_requested"] = False

        def worker() -> None:
            try:
                summary = design_loop_engine.run(
                    payload=payload,
                    progress=self._handle_progress,
                    should_stop=lambda: bool(self.get().get("stop_requested")),
                )
                with self._lock:
                    self._state["last_summary"] = summary
                    self._state["status"] = summary.get("status", "finished")
            except Exception as ex:
                with self._lock:
                    self._state["last_error"] = str(ex)
                    self._state["status"] = "failed"
                    self._append_log(f"Design loop crashed: {ex}")
            finally:
                with self._lock:
                    self._state["running"] = False
                    self._state["finished_at"] = utc_now_iso()

        Thread(target=worker, daemon=True).start()
        return True, "Design loop started."

    def stop(self) -> tuple[bool, str]:
        with self._lock:
            if not self._state.get("running"):
                return False, "No design loop is currently running."
            self._state["stop_requested"] = True
            self._state["status"] = "stopping"
            self._append_log("Stop requested by user.")
        return True, "Stop request accepted."

    def get(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state)

    def latest(self) -> dict[str, Any]:
        with self._lock:
            if self._state.get("last_summary"):
                return dict(self._state.get("last_summary", {}))

        loops_root = runner.runtime_dir / "design_loops"
        if not loops_root.exists():
            return {}
        summaries = sorted(loops_root.glob("*/loop_summary.json"), reverse=True)
        for summary_path in summaries:
            try:
                return json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                continue
        return {}


design_loop_manager = DesignLoopManager()


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
    if design_loop_manager.get().get("running"):
        return jsonify({"ok": False, "message": "Design loop is running. Stop it before manual run."}), 409
    payload = request.get_json(force=True, silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "Request body must be a JSON object."}), 400
    mode = str(payload.get("mode", "all")).lower()

    if mode == "predict":
        try:
            result = surrogate_engine.predict_mode(payload)
        except ValueError as ex:
            return jsonify({"ok": False, "error": str(ex)}), 400
        except RuntimeError as ex:
            return jsonify({"ok": False, "error": str(ex)}), 502
        return jsonify({"ok": True, "mode": "predict", "result": result})

    if mode == "validate":
        if run_manager.get().get("running"):
            return jsonify({"ok": False, "message": "A run is already in progress."}), 409

        def validate_task(progress_cb: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
            return surrogate_engine.validate_mode(payload, progress=progress_cb)

        started, message = run_manager.start("validate", task=validate_task)
        return jsonify({"ok": started, "message": message})

    started, message = run_manager.start(mode)
    return jsonify({"ok": started, "message": message})


@app.post("/api/surrogate/train")
def api_surrogate_train():
    auth = require_api_key()
    if auth:
        return auth
    payload = request.get_json(force=True, silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "Request body must be a JSON object."}), 400

    try:
        result = surrogate_engine.train(
            objective_alias=str(payload.get("objective_alias", "")).strip() or None,
            include_design_loops=bool(payload.get("include_design_loops", True)),
            min_rows=max(5, int(payload.get("min_rows", 50))),
        )
    except ValueError as ex:
        return jsonify({"ok": False, "error": str(ex)}), 400
    except RuntimeError as ex:
        return jsonify({"ok": False, "error": str(ex)}), 502
    return jsonify({"ok": True, "result": result})


@app.get("/api/surrogate/status")
def api_surrogate_status():
    return jsonify({"ok": True, "result": surrogate_engine.status()})


@app.post("/api/surrogate/predict")
def api_surrogate_predict():
    auth = require_api_key()
    if auth:
        return auth
    payload = request.get_json(force=True, silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "Request body must be a JSON object."}), 400
    try:
        result = surrogate_engine.predict_mode(payload)
    except ValueError as ex:
        return jsonify({"ok": False, "error": str(ex)}), 400
    except RuntimeError as ex:
        return jsonify({"ok": False, "error": str(ex)}), 502
    return jsonify({"ok": True, "result": result})


@app.get("/api/surrogate/coverage")
def api_surrogate_coverage():
    return jsonify({"ok": True, "result": surrogate_engine.coverage()})


@app.get("/api/status")
def api_status():
    state = run_manager.get()
    state["auth_required"] = bool(API_KEY)
    return jsonify(state)


@app.post("/api/design-loop/start")
def api_design_loop_start():
    auth = require_api_key()
    if auth:
        return auth
    if run_manager.get().get("running"):
        return jsonify({"ok": False, "message": "A manual run is in progress. Wait until it finishes."}), 409

    payload = request.get_json(force=True, silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "Request body must be a JSON object."}), 400
    started, message = design_loop_manager.start(payload)
    status_code = 200 if started else 409
    return jsonify({"ok": started, "message": message}), status_code


@app.post("/api/design-loop/stop")
def api_design_loop_stop():
    auth = require_api_key()
    if auth:
        return auth
    ok, message = design_loop_manager.stop()
    status_code = 200 if ok else 409
    return jsonify({"ok": ok, "message": message}), status_code


@app.get("/api/design-loop/status")
def api_design_loop_status():
    state = design_loop_manager.get()
    state["auth_required"] = bool(API_KEY)
    return jsonify(state)


@app.get("/api/design-loop/latest")
def api_design_loop_latest():
    return jsonify(design_loop_manager.latest())


@app.get("/api/latest-run")
def api_latest_run():
    summary = enrich_summary(runner.latest_run())
    return jsonify(summary)


@app.get("/runtime/<path:subpath>")
def serve_runtime(subpath: str):
    return send_from_directory(runner.runtime_dir, subpath)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5055, debug=False)
