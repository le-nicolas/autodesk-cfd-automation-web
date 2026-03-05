from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, Thread
from typing import Any

from flask import Flask, jsonify, request, send_from_directory

from cfd_automation import AutomationRunner


PROJECT_ROOT = Path(__file__).resolve().parent
runner = AutomationRunner(PROJECT_ROOT)

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
        case_results.append(item)
    out["case_results"] = case_results
    return out


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
        }

    def _append_log(self, line: str) -> None:
        logs = self._state.setdefault("logs", [])
        logs.append(f"[{utc_now_iso()}] {line}")
        if len(logs) > 250:
            del logs[:-250]

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
                self._append_log(
                    f"Case retry: {event.get('case_id', '')} "
                    f"(attempt {event.get('attempt', 1)}/{event.get('max_attempts', 1)}) "
                    f"reason={event.get('reason', '')}"
                )
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
    payload = request.get_json(force=True, silent=True) or {}
    csv_text = payload.get("csv", "")
    if not isinstance(csv_text, str):
        return jsonify({"ok": False, "error": "Field 'csv' must be text."}), 400
    rows = runner.save_cases_csv(csv_text)
    return jsonify({"ok": True, "rows": rows})


@app.post("/api/introspect")
def api_introspect():
    payload = request.get_json(force=True, silent=True) or {}
    study_path = payload.get("study_path")
    result = runner.introspect(study_override=study_path)
    if result.get("data"):
        data_output = dict(result)
        data_output["data_url"] = to_runtime_url(result.get("output_path", ""))
        return jsonify({"ok": True, "result": data_output})
    return jsonify({"ok": False, "result": result}), 500


@app.post("/api/run")
def api_run():
    payload = request.get_json(force=True, silent=True) or {}
    mode = str(payload.get("mode", "all")).lower()
    started, message = run_manager.start(mode)
    return jsonify({"ok": started, "message": message})


@app.get("/api/status")
def api_status():
    return jsonify(run_manager.get())


@app.get("/api/latest-run")
def api_latest_run():
    summary = enrich_summary(runner.latest_run())
    return jsonify(summary)


@app.get("/runtime/<path:subpath>")
def serve_runtime(subpath: str):
    return send_from_directory(runner.runtime_dir, subpath)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5055, debug=False)
