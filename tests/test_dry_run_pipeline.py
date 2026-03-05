from __future__ import annotations

import shutil
from pathlib import Path

import yaml

from cfd_automation.runner import AutomationRunner


def _make_project(tmp_path: Path, *, cases_csv: str) -> Path:
    project = tmp_path / "project"
    (project / "config").mkdir(parents=True, exist_ok=True)
    (project / "scripts").mkdir(parents=True, exist_ok=True)

    repo_root = Path(__file__).resolve().parents[1]
    shutil.copy2(repo_root / "scripts" / "cfd_case_runner.py", project / "scripts" / "cfd_case_runner.py")
    shutil.copy2(repo_root / "scripts" / "cfd_introspect.py", project / "scripts" / "cfd_introspect.py")

    config = {
        "study": {
            "template_model": "C:/nonexistent/model.cfdst",
            "design_name": "",
            "scenario_name": "",
        },
        "automation": {
            "cfd_executable": "CFD.exe",
            "timeout_minutes": 2,
            "max_retries": 1,
        },
        "solve": {
            "enabled": False,
            "skip_if_results_exist": True,
        },
        "outputs": {
            "save_all_summary": True,
            "screenshots": {"enabled": False, "views": ["default"]},
            "cutplanes": [],
            "report": {"enabled": True},
        },
        "metrics": [],
        "criteria": [],
        "ranking": [],
        "parameter_mappings": [],
    }
    (project / "config" / "study_config.yaml").write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )
    (project / "config" / "cases.csv").write_text(cases_csv, encoding="utf-8")
    return project


def test_dry_run_pipeline_all_and_changed(tmp_path: Path, monkeypatch) -> None:
    project = _make_project(
        tmp_path,
        cases_csv=(
            "case_id,inlet_velocity_ms,total_heat_w\n"
            "CASE_A,1.5,100\n"
            "CASE_B,2.0,120\n"
        ),
    )
    monkeypatch.setenv("CFD_AUTOMATION_DRY_RUN", "1")

    runner = AutomationRunner(project)
    summary = runner.run(mode="all")

    assert summary["selected_case_count"] == 2
    assert summary["successful_cases"] == 2
    assert summary["failed_cases"] == 0
    assert Path(summary["results"]["master_csv"]).exists()
    assert Path(summary["results"]["report_html"]).exists()

    changed_summary = runner.run(mode="changed")
    assert changed_summary["selected_case_count"] == 0


def test_dry_run_failure_reason_exposed(tmp_path: Path, monkeypatch) -> None:
    project = _make_project(
        tmp_path,
        cases_csv=(
            "case_id,inlet_velocity_ms,force_fail\n"
            "CASE_OK,1.5,false\n"
            "CASE_FAIL,2.0,true\n"
        ),
    )
    monkeypatch.setenv("CFD_AUTOMATION_DRY_RUN", "1")

    runner = AutomationRunner(project)
    summary = runner.run(mode="all")

    failed = [case for case in summary["case_results"] if not case.get("success")]
    assert len(failed) == 1
    assert "Dry-run forced failure" in failed[0].get("failure_reason", "")
